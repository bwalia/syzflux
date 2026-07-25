# Prompt: Build working first POC prototype (`k3s1.yaml`)

Copy everything below the line into a coding agent (or use this file as the task brief).

---

## Goal

Build a **working first prototype** of **SyzFlux**, a **stream-first observability platform**, for a local **k3s** cluster.

Deliver a single applyable manifest:

```text
k3s1.yaml
```

Plus a short `README.md` with apply/verify/test commands.

This is a **POC**, not production. Prefer clarity, testability, and one `kubectl apply -f k3s1.yaml` over perfect HA.

Use **MinIO** as the S3-compatible cold archive (stand-in for customer S3).

Reference architecture (mental model):

```text
Sample apps
  -> Fluent Bit / OTel collectors
    -> Kafka (topics)
      -> Stream processor (Streama-lite)
        -> TCO router (High / Med / Low / Block)
          -> Hot store (OpenSearch)     [High only]
          -> Cold store (MinIO/Parquet) [Med / Low]
          -> Drop                        [Block]
        -> Alerts (in-stream, before index)
  <- Query API / UI (hot + cold)
```

Diagrams already in repo under `diagrams/` describe the target shape; implement a **minimal runnable slice** of that.

---

## Hard constraints

1. **One file first:** `k3s1.yaml` must contain (or clearly document includes for) everything needed to stand up the POC on k3s.
2. **k3s-friendly:** no cloud LBs required; use ClusterIP + optional NodePort/Ingress. Avoid AWS-specific controllers.
3. **MinIO for cold storage:** create bucket(s), credentials via Secret, lifecycle-ish demo (optional).
4. **Sample workload apps:** multiple namespaces/apps that emit logs/metrics/traces so every TCO path can be exercised.
5. **All features listed below must be demonstrable** with `kubectl` + curl/http + MinIO console/mc + OpenSearch queries.
6. **Resource-light:** design for a laptop/dev k3s node (~8–16 GB RAM). Use single-replica Kafka (KRaft or Bitnami single broker), single OpenSearch node, single MinIO, small JVM heaps.
7. **No external SaaS.** Everything runs in-cluster.
8. **ASCII-safe YAML** (no fancy unicode dashes that break parsers).

---

## Namespaces

Create these namespaces in `k3s1.yaml`:

| Namespace | Purpose |
|-----------|---------|
| `obs-system` | Kafka, Streama-lite, TCO router, OpenSearch, MinIO, query API, collectors |
| `demo-apps` | Sample apps generating telemetry |
| `demo-noise` | High-volume / low-value traffic for Block + Low paths |

Label everything with `app.kubernetes.io/part-of: obs-poc` and `obs.poc/component: ...`.

---

## Components to implement in `k3s1.yaml`

### A. Sample apps / services / pods (must-have)

Deploy at least these workloads in `demo-apps` / `demo-noise`:

1. **`checkout-api`** (Deployment + Service)
   - HTTP API that logs structured JSON (`level`, `order_id`, `latency_ms`, `error`).
   - Endpoint `/checkout` succeeds often; `/checkout?fail=1` emits ERROR.
   - Label for TCO: **High** (critical path).

2. **`payments-worker`** (Deployment)
   - Background loop logging payment events + occasional stack-like errors.
   - TCO: **High**.

3. **`catalog-api`** (Deployment + Service)
   - Read-heavy API; INFO logs; low urgency.
   - TCO: **Medium** (monitoring / dashboards / alerts OK, cold storage).

4. **`analytics-batch`** (CronJob or Deployment loop)
   - Verbose DEBUG/INFO logs, high cardinality fields.
   - TCO: **Low** (compliance / archive only).

5. **`debug-spammer`** (Deployment in `demo-noise`)
   - Intentionally noisy, useless logs.
   - TCO: **Block** (must be dropped; never land in OpenSearch or MinIO).

6. **`loadgen`** (Deployment)
   - Continuously hits checkout/catalog endpoints so the pipeline has live traffic without manual curls.

Each app should:
- Log to stdout (JSON preferred).
- Expose a `/healthz` if it is a Service.
- Carry annotation or label `obs.poc/tco-priority: high|medium|low|block`.

Use tiny public images (`nginx`, `busybox`, `python:3.12-alpine` with inline ConfigMap scripts, or `hashicorp/http-echo`) — keep it self-contained via ConfigMaps where needed.

### B. Collection

- **Fluent Bit** (DaemonSet or Deployment) OR **OpenTelemetry Collector**
  - Tail container logs from `demo-apps` + `demo-noise`.
  - Enrich with k8s metadata (`namespace`, `pod`, `labels`, especially `obs.poc/tco-priority`).
  - Export to Kafka topic `telemetry.raw`.

Optional stretch: OTLP receiver for traces on port 4317/4318 from one sample app.

### C. Kafka (stream spine)

- Single-node Kafka suitable for k3s (KRaft mode preferred; ZooKeeper OK if simpler).
- Topics (create via Job or Kafka startup hooks):
  - `telemetry.raw`
  - `telemetry.enriched`
  - `telemetry.alerts`
  - `telemetry.metrics.derived`
- Document retention small (e.g. 1–6h) for POC disk.

### D. Streama-lite (stream processor)

A small custom service (Deployment) that:
1. Consumes `telemetry.raw`
2. Parses JSON / enriches (add `processed_at`, normalize `level`)
3. Emits to `telemetry.enriched`
4. Derives simple metrics (e.g. error count per app per minute) -> `telemetry.metrics.derived`
5. Fires **in-stream alerts** when ERROR rate exceeds threshold -> `telemetry.alerts` **without waiting for OpenSearch indexing**
6. Applies **TCO routing** based on `obs.poc/tco-priority` (or namespace/app rules):
   - `high` -> write to OpenSearch index `logs-hot`
   - `medium` / `low` -> write Parquet (or JSON lines as POC stand-in) objects to MinIO buckets
   - `block` -> discard (increment drop counter metric)

Implementation suggestion for speed:
- Python (`aiokafka` / `confluent-kafka`) or Go single binary built via ConfigMap+script is fine for POC.
- If writing real Parquet is heavy, **phase 1** may write gzipped JSONL to MinIO with path layout mimicking archive; note Parquet as phase 2 — but prefer real Parquet if dependency weight is acceptable (`pyarrow`).

Expose Prometheus metrics on `:8080/metrics`:
- `streama_events_total{priority,outcome}`
- `streama_alerts_total`
- `streama_dropped_total`
- `streama_opensearch_writes_total`
- `streama_minio_writes_total`

### E. Hot storage (indexed)

- Single-node **OpenSearch** (or OpenSearch + Dashboards if RAM allows).
- Disable security plugin or set easy demo creds (document clearly).
- Index template for `logs-hot*` with basic mappings (`timestamp`, `app`, `level`, `message`, `priority`).
- Only **High** priority data should appear here.

### F. Cold storage (MinIO)

- MinIO Deployment + Service (+ optional Console NodePort).
- Buckets:
  - `obs-archive-medium`
  - `obs-archive-low`
- Path layout example:
  - `s3://obs-archive-medium/logs/dt=YYYY-MM-DD/hour=HH/<app>-<uuid>.jsonl.gz`
  - same for low
- Credentials in Secret `minio-creds`.
- Init Job to create buckets.
- Verify Blocked data never appears in either bucket.

### G. Query / demo UI (minimal)

Provide at least one of:

1. **`query-api`** Deployment:
   - `GET /hot/search?q=ERROR` -> OpenSearch
   - `GET /cold/list?priority=medium` -> list MinIO objects
   - `GET /cold/read?key=...` -> fetch object body
   - `GET /alerts/recent` -> last N alerts from Kafka topic or in-memory store
   - `GET /stats` -> pipeline counters

2. Optional: OpenSearch Dashboards + MinIO Console links in README.

### H. Observability of the platform itself

- ServiceMonitors optional (skip Prometheus Operator if not present).
- At minimum: each component has `/healthz`, and Streama-lite `/metrics` is curl-able.

---

## Features that MUST be testable (acceptance checklist)

Wire these into README as copy-paste commands. Agent must ensure each passes after `kubectl apply -f k3s1.yaml` + wait-ready.

| ID | Feature | How to prove |
|----|---------|--------------|
| F1 | Unified ingest | Logs from multiple apps appear on Kafka `telemetry.raw` |
| F2 | In-stream parse/enrich | `telemetry.enriched` has normalized fields |
| F3 | In-stream alert (no index lag) | Force checkout failures; alert on `telemetry.alerts` / query-api **before** relying on OpenSearch |
| F4 | Derive metrics from logs | `telemetry.metrics.derived` or Streama counter increases on errors |
| F5 | TCO High -> hot index | `checkout-api` / `payments-worker` docs in OpenSearch `logs-hot` |
| F6 | TCO Medium -> MinIO | `catalog-api` objects in `obs-archive-medium` only |
| F7 | TCO Low -> MinIO | `analytics-batch` objects in `obs-archive-low` only |
| F8 | TCO Block -> drop | `debug-spammer` never in OpenSearch or MinIO; drop counter increases |
| F9 | Cross-signal single platform | Same Kafka spine for all apps; query-api `/stats` shows all priorities |
| F10 | Cold query without rehydrate | `query-api` can list/read MinIO archive directly |
| F11 | Hot fast search | OpenSearch search for `ERROR` returns checkout/payments hits quickly |
| F12 | Load / sustained traffic | `loadgen` keeps pipeline moving without manual intervention |
| F13 | Health | All Deployments Available; Jobs Complete (bucket init, topic init) |

---

## `k3s1.yaml` structure (recommended order)

Use `---` separators and comments as section headers:

1. Namespaces
2. Secrets / ConfigMaps (apps, streama, fluent-bit, query-api)
3. MinIO + init buckets Job
4. OpenSearch
5. Kafka + topic init Job
6. Collectors (Fluent Bit / OTel)
7. Streama-lite
8. Query API
9. Demo apps + loadgen + debug-spammer
10. Optional Ingress / NodePorts

At top of file, add a comment block:

```yaml
# k3s1.yaml - Next-gen observability POC (stream-first)
# Apply:  kubectl apply -f k3s1.yaml
# Target: k3s (local). Cold store = MinIO. Hot store = OpenSearch.
```

---

## README.md requirements

Include:

1. Prerequisites (k3s or k3d/kind equivalent, kubectl, 8GB+ RAM recommendation)
2. Apply + wait commands
3. Port-forward cheatsheet (MinIO console, OpenSearch, query-api, Kafka optional)
4. Acceptance test script section (bash) covering F1–F13
5. Architecture note pointing at `diagrams/syzflux-architecture-detailed.svg`
6. Known POC limitations (single broker, no auth hardening, JSONL vs Parquet if deferred)
7. Teardown: `kubectl delete -f k3s1.yaml`

Optional: `scripts/smoke-test.sh` if it keeps `k3s1.yaml` cleaner — but prefer smoke tests documented in README even if inline.

---

## Implementation preferences

- Prefer **vanilla Kubernetes YAML** in one file over Helm for this first prototype (Helm can be phase 2).
- Pin image tags (no `latest` where avoidable).
- Requests/limits on every container (small).
- Use `emptyDir` or small PVCs; for MinIO/OpenSearch/Kafka prefer PVC so restarts keep data during the demo.
- If something cannot fit RAM, degrade gracefully in this order: drop Dashboards -> shrink OpenSearch heap -> replace Kafka with Redpanda single binary -> JSONL instead of Parquet.
- Name SyzFlux POC components clearly: `streama-lite`, `tco-router` (can be same process), `query-api`.

---

## Definition of done

1. `kubectl apply -f k3s1.yaml` succeeds on k3s.
2. Within ~5–10 minutes, all core pods Ready.
3. Smoke tests F1–F13 pass and are documented.
4. README explains how Kafka + selective indexing + MinIO cold storage keep the “single platform” efficient.
5. Repo also retains existing diagrams; do not delete them.

---

## Out of scope (do not build yet)

- Multi-AZ Kafka / OpenSearch clusters
- Real SSO / RBAC hardening
- Full DataPrime language
- AI chat investigator (Olly) — stub endpoint OK (`GET /ai/status` -> `{"status":"not_implemented"}`)
- Production TLS everywhere

---

## First message to the implementing agent (short form)

> Implement the POC described in this prompt. Create `k3s1.yaml` (single applyable manifest) and `README.md` for a k3s stream-first observability prototype: sample apps with High/Med/Low/Block TCO labels, Fluent Bit or OTel -> Kafka -> Streama-lite (parse/enrich/alert/derive metrics/TCO route) -> OpenSearch hot + MinIO cold archive + query-api. Make features F1–F13 verifiable. Keep it laptop-sized. Use MinIO instead of AWS S3. Follow the architecture in `diagrams/syzflux-architecture-detailed.svg`.
