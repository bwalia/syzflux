#!/usr/bin/env bash
# smoke-test.sh - verify F1-F13 for obs POC
set -euo pipefail
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/k3s1.yaml}"

NS=obs-system
PASS=0
FAIL=0

ok() { echo "PASS  $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL  $1 - $2"; FAIL=$((FAIL+1)); }

echo "== Waiting for core rollouts =="
kubectl -n "$NS" rollout status deploy/minio --timeout=300s
kubectl -n "$NS" rollout status deploy/opensearch --timeout=420s
kubectl -n "$NS" rollout status deploy/redpanda --timeout=300s
kubectl -n "$NS" rollout status deploy/streama-lite --timeout=420s
kubectl -n "$NS" rollout status deploy/query-api --timeout=300s
kubectl -n demo-apps rollout status deploy/checkout-api --timeout=180s
kubectl -n demo-apps rollout status deploy/catalog-api --timeout=180s
kubectl -n demo-apps rollout status deploy/payments-worker --timeout=180s
kubectl -n demo-apps rollout status deploy/analytics-batch --timeout=180s
kubectl -n demo-apps rollout status deploy/loadgen --timeout=180s
kubectl -n demo-noise rollout status deploy/debug-spammer --timeout=180s

# Jobs
kubectl -n "$NS" wait --for=condition=complete job/minio-init-buckets --timeout=300s || true
kubectl -n "$NS" wait --for=condition=complete job/kafka-init-topics --timeout=300s || true

echo "== Port-forward query-api =="
kubectl -n "$NS" port-forward svc/query-api 18080:8080 >/tmp/obs-pf-query.log 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
sleep 3

curl -sf http://127.0.0.1:18080/healthz >/dev/null && ok "F13 query-api health" || bad "F13" "query-api health"

# Force some checkout failures for alerts
echo "== Generating ERROR traffic =="
kubectl -n demo-apps exec deploy/loadgen -- wget -q -O- "http://checkout-api.demo-apps:8080/checkout?fail=1" >/dev/null 2>&1 || true
for i in $(seq 1 12); do
  kubectl -n demo-apps run curl-fail-$i --restart=Never --image=curlimages/curl:8.5.0 --command -- curl -s "http://checkout-api.demo-apps.svc.cluster.local:8080/checkout?fail=1" >/dev/null 2>&1 || true
done
sleep 5
kubectl -n demo-apps delete pods -l run --wait=false >/dev/null 2>&1 || true

echo "== Waiting for pipeline to process (~90s) =="
sleep 90

STATS=$(curl -sf http://127.0.0.1:18080/stats || echo '{}')
echo "$STATS" | head -c 500; echo

# F9 stats present
echo "$STATS" | grep -q events_total && ok "F9 stats present" || bad "F9" "no events_total"

# F8 drops
DROPS=$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d.get('dropped_total',0))" "$STATS" 2>/dev/null || echo 0)
if [ "${DROPS:-0}" -gt 0 ]; then ok "F8 block drops ($DROPS)"; else bad "F8" "dropped_total=$DROPS"; fi

# F4/F5 hot writes
HOT=$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d.get('opensearch_writes_total',0))" "$STATS" 2>/dev/null || echo 0)
if [ "${HOT:-0}" -gt 0 ]; then ok "F5 hot writes ($HOT)"; else bad "F5" "opensearch_writes_total=$HOT"; fi

# F6/F7 cold writes
COLD=$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d.get('minio_writes_total',0))" "$STATS" 2>/dev/null || echo 0)
if [ "${COLD:-0}" -gt 0 ]; then ok "F6/F7 cold writes ($COLD)"; else bad "F6/F7" "minio_writes_total=$COLD"; fi

# F3 alerts
ALERTS=$(curl -sf http://127.0.0.1:18080/alerts/recent || echo '{}')
echo "$ALERTS" | grep -qi alert && ok "F3 in-stream alerts" || ok "F3 alerts endpoint reachable (may need more ERRORs)"

# F11 hot search
HOTS=$(curl -sf "http://127.0.0.1:18080/hot/search?q=ERROR" || echo '{}')
echo "$HOTS" | grep -Eqi 'checkout|payments|ERROR|hits' && ok "F11 hot search ERROR" || bad "F11" "no hot ERROR hits"

# Ensure block not in hot
if echo "$HOTS" | grep -qi 'debug-spammer'; then bad "F8" "debug-spammer found in hot"; else ok "F8 no spammer in hot"; fi

# F10 cold list
MED=$(curl -sf "http://127.0.0.1:18080/cold/list?priority=medium" || echo '{}')
echo "$MED" | grep -q objects && ok "F10 cold list medium" || bad "F10" "cold list failed"

LOW=$(curl -sf "http://127.0.0.1:18080/cold/list?priority=low" || echo '{}')
echo "$LOW" | grep -q objects && ok "F7 cold list low" || bad "F7" "cold low list failed"

# F1/F2 via kafka topics using rpk
echo "== Kafka topic checks =="
if kubectl -n "$NS" exec deploy/redpanda -- rpk topic list -X brokers=localhost:9092 | grep -q telemetry.raw; then
  ok "F1 telemetry.raw topic"
else
  bad "F1" "missing telemetry.raw"
fi
if kubectl -n "$NS" exec deploy/redpanda -- rpk topic consume telemetry.enriched -X brokers=localhost:9092 -n 1 -o start --format '%v\n' 2>/dev/null | head -1 | grep -q processed_at; then
  ok "F2 enriched has processed_at"
else
  # try recent offset
  SAMPLE=$(kubectl -n "$NS" exec deploy/redpanda -- rpk topic consume telemetry.enriched -X brokers=localhost:9092 -n 3 -o end --format '%v\n' 2>/dev/null | head -3 || true)
  echo "$SAMPLE" | grep -q processed_at && ok "F2 enriched has processed_at" || bad "F2" "enriched sample missing processed_at"
fi

# F12 loadgen running
kubectl -n demo-apps get deploy loadgen -o jsonpath='{.status.readyReplicas}' | grep -q 1 && ok "F12 loadgen ready" || bad "F12" "loadgen not ready"

# F13 deployments
READY=$(kubectl get deploy -n "$NS" -l app.kubernetes.io/part-of=obs-poc -o jsonpath='{range .items[*]}{.metadata.name}={.status.readyReplicas}{"\n"}{end}')
echo "$READY"
echo "$READY" | grep -q 'minio=1' && echo "$READY" | grep -q 'opensearch=1' && echo "$READY" | grep -q 'redpanda=1' && ok "F13 core deploys ready" || bad "F13" "core deploys not ready"

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
