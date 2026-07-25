# Next-gen observability POC (k3s)

Stream-first observability prototype for SyzFlux:
**Fluent Bit -> Redpanda (Kafka API) -> Streama-lite (enrich / alert / TCO route) -> OpenSearch (hot) + MinIO (cold)**.

See [`diagrams/syzflux-architecture-detailed.svg`](diagrams/syzflux-architecture-detailed.svg).

## Prerequisites

- k3s cluster with `kubectl` access
- StorageClass `longhorn` (or edit PVCs in `k3s1.yaml`)
- ~4+ CPU / 8+ GB free across the cluster
- This repo's kubeconfig example:

```bash
export KUBECONFIG=~/.kube/k3s1.yaml
```

## Apply

```bash
export KUBECONFIG=~/.kube/k3s1.yaml
kubectl apply -f k3s1.yaml
```

Wait for core components (first pull of OpenSearch/Redpanda/Python images can take several minutes):

```bash
kubectl -n obs-system rollout status deploy/minio --timeout=300s
kubectl -n obs-system rollout status deploy/redpanda --timeout=300s
kubectl -n obs-system rollout status deploy/opensearch --timeout=600s
kubectl -n obs-system rollout status deploy/streama-lite --timeout=600s
kubectl -n obs-system rollout status deploy/query-api --timeout=300s
kubectl -n obs-system wait --for=condition=complete job/minio-init-buckets --timeout=300s
kubectl -n obs-system wait --for=condition=complete job/kafka-init-topics --timeout=300s
```

Regenerate `k3s1.yaml` from sources after editing `app-src/`:

```bash
python3 scripts/generate_k3s1.py
```

## Architecture (POC slice)

| Piece | What |
|-------|------|
| Sample apps | `checkout-api` / `payments-worker` (High), `catalog-api` (Medium), `analytics-batch` (Low), `debug-spammer` (Block), `loadgen` |
| Collector | Fluent Bit DaemonSet tails `demo-apps` + `demo-noise` logs -> Kafka topic `telemetry.raw` |
| Stream spine | Redpanda (Kafka-compatible), topics `telemetry.raw`, `telemetry.enriched`, `telemetry.alerts`, `telemetry.metrics.derived` |
| Streama-lite | Parse/enrich, in-stream ERROR-rate alerts, derive metrics, TCO route |
| Hot | OpenSearch index `logs-hot` (High only) |
| Cold | MinIO buckets `obs-archive-medium`, `obs-archive-low` (gzipped JSONL) |
| Query | `query-api` over hot + cold + stats/alerts |

**Why storage stays low:** Kafka/Streama analyze and alert in motion; only High is indexed; Med/Low land in cheap object storage; Block is dropped.

## Port-forward cheatsheet

```bash
export KUBECONFIG=~/.kube/k3s1.yaml

# Query API
kubectl -n obs-system port-forward svc/query-api 18080:8080

# OpenSearch
kubectl -n obs-system port-forward svc/opensearch 9200:9200

# MinIO S3 + console (console on 9001)
kubectl -n obs-system port-forward svc/minio 9000:9000 9001:9001

# Streama metrics/stats
kubectl -n obs-system port-forward svc/streama-lite 18081:8080
```

MinIO console login: `minioadmin` / `minioadmin123`

## Quick queries

```bash
curl -s http://127.0.0.1:18080/stats | jq .
curl -s 'http://127.0.0.1:18080/hot/search?q=ERROR' | jq .
curl -s 'http://127.0.0.1:18080/cold/list?priority=medium' | jq .
curl -s 'http://127.0.0.1:18080/cold/list?priority=low' | jq .
curl -s http://127.0.0.1:18080/alerts/recent | jq .
curl -s http://127.0.0.1:18080/ai/status
```

Force checkout failures (helps F3 alerts):

```bash
kubectl -n demo-apps run failgen --rm -it --restart=Never --image=curlimages/curl:8.5.0 -- \
  sh -c 'for i in $(seq 1 20); do curl -s "http://checkout-api:8080/checkout?fail=1"; done'
```

## Acceptance tests (F1-F13)

```bash
chmod +x scripts/smoke-test.sh
./scripts/smoke-test.sh
```

| ID | Feature |
|----|---------|
| F1 | Unified ingest on `telemetry.raw` |
| F2 | Enriched events with `processed_at` |
| F3 | In-stream alerts (no index wait) |
| F4 | Derived error metrics / counters |
| F5 | High -> OpenSearch `logs-hot` |
| F6 | Medium -> MinIO `obs-archive-medium` |
| F7 | Low -> MinIO `obs-archive-low` |
| F8 | Block dropped (not in hot/cold) |
| F9 | Single platform stats across priorities |
| F10 | Cold read/list without rehydrate |
| F11 | Hot ERROR search |
| F12 | Loadgen sustained traffic |
| F13 | Deployments/Jobs healthy |

## Teardown

```bash
export KUBECONFIG=~/.kube/k3s1.yaml
kubectl delete -f k3s1.yaml
# PVCs may remain depending on reclaim policy:
kubectl -n obs-system delete pvc --all
```

## Known POC limitations

- Single Redpanda broker, single OpenSearch node, single MinIO
- Cold archive is **gzipped JSONL** (Parquet can be phase 2)
- Demo credentials, security plugin disabled on OpenSearch
- Streama/query-api `pip install` on container start (slow first boot)
- SyzFlux educational prototype -- not production-hardened

## Layout

```text
k3s1.yaml                 # applyable manifest
app-src/                  # Python sources embedded into ConfigMaps
scripts/generate_k3s1.py  # regenerates k3s1.yaml
scripts/smoke-test.sh     # F1-F13 checks
diagrams/                 # architecture SVGs/PNG
prompts/                  # build prompt
```
