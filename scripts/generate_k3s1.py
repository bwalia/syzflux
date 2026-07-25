#!/usr/bin/env python3
"""Generate k3s1.yaml from app-src scripts."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app-src"
OUT = ROOT / "k3s1.yaml"


def indent(text: str, n: int = 4) -> str:
    pad = " " * n
    return "\n".join(pad + line if line else line for line in text.splitlines())


def cm_file(name: str, filename: str, content: str) -> str:
    # YAML literal block for python scripts
    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: obs-system
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: config
data:
  {filename}: |
{indent(content, 4)}
"""


def cm_demo(name: str, filename: str, content: str, ns: str = "demo-apps") -> str:
    return f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: {ns}
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: config
data:
  {filename}: |
{indent(content, 4)}
"""


parts: list[str] = []

parts.append("""# k3s1.yaml - Next-gen observability POC (stream-first)
# Apply:  kubectl apply -f k3s1.yaml
# Target: k3s. Cold store = MinIO. Hot store = OpenSearch. Stream = Redpanda/Kafka API.
# KUBECONFIG example: export KUBECONFIG=~/.kube/k3s1.yaml
---
apiVersion: v1
kind: Namespace
metadata:
  name: obs-system
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: namespace
---
apiVersion: v1
kind: Namespace
metadata:
  name: demo-apps
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: namespace
---
apiVersion: v1
kind: Namespace
metadata:
  name: demo-noise
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: namespace
---
# ========== Secrets ==========
apiVersion: v1
kind: Secret
metadata:
  name: minio-creds
  namespace: obs-system
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: minio
type: Opaque
stringData:
  accesskey: minioadmin
  secretkey: minioadmin123
---
""")

# ConfigMaps for apps
parts.append(cm_demo("checkout-api-code", "main.py", (APP / "checkout_api.py").read_text()))
parts.append("---\n")
parts.append(cm_demo("catalog-api-code", "main.py", (APP / "catalog_api.py").read_text()))
parts.append("---\n")
parts.append(cm_demo("payments-worker-code", "main.py", (APP / "payments_worker.py").read_text()))
parts.append("---\n")
parts.append(cm_demo("analytics-batch-code", "main.py", (APP / "analytics_batch.py").read_text()))
parts.append("---\n")
parts.append(cm_demo("loadgen-code", "main.py", (APP / "loadgen.py").read_text()))
parts.append("---\n")
parts.append(cm_demo("debug-spammer-code", "main.py", (APP / "debug_spammer.py").read_text(), ns="demo-noise"))
parts.append("---\n")
parts.append(cm_file("streama-lite-code", "main.py", (APP / "streama_lite.py").read_text()))
parts.append("---\n")
parts.append(cm_file("query-api-code", "main.py", (APP / "query_api.py").read_text()))
parts.append("---\n")

# Fluent Bit config
fb_config = r"""[SERVICE]
    Flush         1
    Daemon        Off
    Log_Level     info
    Parsers_File  parsers.conf
    HTTP_Server   On
    HTTP_Listen   0.0.0.0
    HTTP_Port     2020

[INPUT]
    Name              tail
    Tag               kube.*
    Path              /var/log/containers/*_demo-apps_*.log,/var/log/containers/*_demo-noise_*.log
    Parser            cri
    DB                /tmp/flb_kube.db
    Mem_Buf_Limit     32MB
    Skip_Long_Lines   On
    Refresh_Interval  5
    Inotify_Watcher   false

[FILTER]
    Name                kubernetes
    Match               kube.*
    Kube_URL            https://kubernetes.default.svc:443
    Kube_CA_File        /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
    Kube_Token_File     /var/run/secrets/kubernetes.io/serviceaccount/token
    Merge_Log           On
    Merge_Log_Key       log_processed
    Keep_Log            On
    K8S-Logging.Parser  On
    K8S-Logging.Exclude Off
    Labels              On
    Annotations         Off

[FILTER]
    Name           nest
    Match          kube.*
    Operation      lift
    Nested_under   log_processed
    Add_prefix     lp_

[OUTPUT]
    Name           kafka
    Match          kube.*
    Brokers        redpanda.obs-system.svc.cluster.local:9092
    Topics         telemetry.raw
    Timestamp_Key  @timestamp
    Retry_Limit    false
    rdkafka.queue.buffering.max.messages  10000
"""

fb_parsers = r"""[PARSER]
    Name        cri
    Format      regex
    Regex       ^(?<time>[^ ]+) (?<stream>stdout|stderr) (?<logtag>[^ ]*) (?<message>.*)$
    Time_Key    time
    Time_Format %Y-%m-%dT%H:%M:%S.%L%z
    Time_Keep   On
"""

parts.append(f"""apiVersion: v1
kind: ConfigMap
metadata:
  name: fluent-bit-config
  namespace: obs-system
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: fluent-bit
data:
  fluent-bit.conf: |
{indent(fb_config, 4)}
  parsers.conf: |
{indent(fb_parsers, 4)}
---
""")

# MinIO
parts.append("""# ========== MinIO (cold archive) ==========
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: minio-data
  namespace: obs-system
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: minio
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: longhorn
  resources:
    requests:
      storage: 10Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: obs-system
  labels:
    app: minio
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: minio
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
        app.kubernetes.io/part-of: obs-poc
        obs.poc/component: minio
    spec:
      containers:
        - name: minio
          image: quay.io/minio/minio:RELEASE.2024-10-02T17-50-41Z
          args: ["server", "/data", "--console-address", ":9001"]
          env:
            - name: MINIO_ROOT_USER
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: accesskey
            - name: MINIO_ROOT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: secretkey
          ports:
            - containerPort: 9000
              name: s3
            - containerPort: 9001
              name: console
          readinessProbe:
            httpGet:
              path: /minio/health/ready
              port: 9000
            initialDelaySeconds: 5
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /minio/health/live
              port: 9000
            initialDelaySeconds: 10
            periodSeconds: 10
          resources:
            requests:
              cpu: 50m
              memory: 128Mi
            limits:
              cpu: "1"
              memory: 512Mi
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: minio-data
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: obs-system
  labels:
    app: minio
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: minio
spec:
  selector:
    app: minio
  ports:
    - name: s3
      port: 9000
      targetPort: 9000
    - name: console
      port: 9001
      targetPort: 9001
---
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-init-buckets
  namespace: obs-system
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: minio
spec:
  backoffLimit: 20
  template:
    metadata:
      labels:
        app.kubernetes.io/part-of: obs-poc
        obs.poc/component: minio
    spec:
      restartPolicy: OnFailure
      containers:
        - name: mc
          image: quay.io/minio/mc:RELEASE.2024-10-08T09-37-26Z
          env:
            - name: MINIO_ACCESS
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: accesskey
            - name: MINIO_SECRET
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: secretkey
          command:
            - /bin/sh
            - -c
            - |
              set -e
              echo "waiting for minio..."
              for i in $(seq 1 60); do
                mc alias set local http://minio.obs-system:9000 "$MINIO_ACCESS" "$MINIO_SECRET" && break
                sleep 3
              done
              mc mb -p local/obs-archive-medium || true
              mc mb -p local/obs-archive-low || true
              mc ls local
              echo "buckets ready"
          resources:
            requests:
              cpu: 20m
              memory: 64Mi
            limits:
              cpu: 200m
              memory: 128Mi
---
""")

# OpenSearch
parts.append("""# ========== OpenSearch (hot indexed) ==========
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: opensearch-data
  namespace: obs-system
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: opensearch
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: longhorn
  resources:
    requests:
      storage: 10Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: opensearch
  namespace: obs-system
  labels:
    app: opensearch
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: opensearch
spec:
  replicas: 1
  selector:
    matchLabels:
      app: opensearch
  template:
    metadata:
      labels:
        app: opensearch
        app.kubernetes.io/part-of: obs-poc
        obs.poc/component: opensearch
    spec:
      securityContext:
        fsGroup: 1000
      initContainers:
        - name: fix-permissions
          image: busybox:1.36
          command: ["sh", "-c", "chown -R 1000:1000 /usr/share/opensearch/data && sysctl -w vm.max_map_count=262144 || true"]
          securityContext:
            privileged: true
            runAsUser: 0
          volumeMounts:
            - name: data
              mountPath: /usr/share/opensearch/data
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
            limits:
              cpu: 100m
              memory: 64Mi
      containers:
        - name: opensearch
          image: opensearchproject/opensearch:2.11.1
          securityContext:
            runAsUser: 1000
            runAsGroup: 1000
          env:
            - name: discovery.type
              value: single-node
            - name: plugins.security.disabled
              value: "true"
            - name: DISABLE_INSTALL_DEMO_CONFIG
              value: "true"
            - name: OPENSEARCH_JAVA_OPTS
              value: -Xms512m -Xmx512m
            - name: bootstrap.memory_lock
              value: "false"
          ports:
            - containerPort: 9200
              name: http
          readinessProbe:
            httpGet:
              path: /_cluster/health
              port: 9200
            initialDelaySeconds: 30
            periodSeconds: 10
            failureThreshold: 30
          livenessProbe:
            httpGet:
              path: /
              port: 9200
            initialDelaySeconds: 60
            periodSeconds: 20
          resources:
            requests:
              cpu: 200m
              memory: 1Gi
            limits:
              cpu: "2"
              memory: 1536Mi
          volumeMounts:
            - name: data
              mountPath: /usr/share/opensearch/data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: opensearch-data
---
apiVersion: v1
kind: Service
metadata:
  name: opensearch
  namespace: obs-system
  labels:
    app: opensearch
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: opensearch
spec:
  selector:
    app: opensearch
  ports:
    - name: http
      port: 9200
      targetPort: 9200
---
""")

# Redpanda
parts.append("""# ========== Redpanda (Kafka API stream spine) ==========
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: redpanda-data
  namespace: obs-system
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: kafka
spec:
  accessModes: ["ReadWriteOnce"]
  storageClassName: longhorn
  resources:
    requests:
      storage: 10Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redpanda
  namespace: obs-system
  labels:
    app: redpanda
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: kafka
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: redpanda
  template:
    metadata:
      labels:
        app: redpanda
        app.kubernetes.io/part-of: obs-poc
        obs.poc/component: kafka
    spec:
      securityContext:
        fsGroup: 101
      initContainers:
        - name: fix-permissions
          image: busybox:1.36
          command: ["sh", "-c", "chown -R 101:101 /var/lib/redpanda/data"]
          securityContext:
            runAsUser: 0
          volumeMounts:
            - name: data
              mountPath: /var/lib/redpanda/data
          resources:
            requests:
              cpu: 10m
              memory: 16Mi
            limits:
              cpu: 100m
              memory: 64Mi
      containers:
        - name: redpanda
          image: docker.redpanda.com/redpandadata/redpanda:v24.2.4
          securityContext:
            runAsUser: 101
            runAsGroup: 101
          args:
            - redpanda
            - start
            - --overprovisioned
            - --smp
            - "1"
            - --memory
            - 1G
            - --reserve-memory
            - 0M
            - --node-id
            - "0"
            - --check=false
            - --kafka-addr
            - internal://0.0.0.0:9092,external://0.0.0.0:19092
            - --advertise-kafka-addr
            - internal://redpanda.obs-system.svc.cluster.local:9092,external://redpanda.obs-system.svc.cluster.local:19092
            - --pandaproxy-addr
            - internal://0.0.0.0:8082,external://0.0.0.0:18082
            - --advertise-pandaproxy-addr
            - internal://redpanda.obs-system.svc.cluster.local:8082,external://redpanda.obs-system.svc.cluster.local:18082
            - --schema-registry-addr
            - internal://0.0.0.0:8081,external://0.0.0.0:18081
            - --rpc-addr
            - 0.0.0.0:33145
            - --advertise-rpc-addr
            - redpanda.obs-system.svc.cluster.local:33145
            - --mode
            - dev-container
          ports:
            - containerPort: 9092
              name: kafka
            - containerPort: 9644
              name: admin
          readinessProbe:
            exec:
              command: ["rpk", "cluster", "health"]
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 30
          resources:
            requests:
              cpu: 100m
              memory: 512Mi
            limits:
              cpu: "2"
              memory: 1536Mi
          volumeMounts:
            - name: data
              mountPath: /var/lib/redpanda/data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: redpanda-data
---
apiVersion: v1
kind: Service
metadata:
  name: redpanda
  namespace: obs-system
  labels:
    app: redpanda
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: kafka
spec:
  selector:
    app: redpanda
  ports:
    - name: kafka
      port: 9092
      targetPort: 9092
    - name: admin
      port: 9644
      targetPort: 9644
---
apiVersion: batch/v1
kind: Job
metadata:
  name: kafka-init-topics
  namespace: obs-system
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: kafka
spec:
  backoffLimit: 30
  template:
    metadata:
      labels:
        app.kubernetes.io/part-of: obs-poc
        obs.poc/component: kafka
    spec:
      restartPolicy: OnFailure
      containers:
        - name: rpk
          image: docker.redpanda.com/redpandadata/redpanda:v24.2.4
          command:
            - /bin/sh
            - -c
            - |
              set -e
              export RPK_BROKERS=redpanda.obs-system.svc.cluster.local:9092
              echo "waiting for redpanda..."
              for i in $(seq 1 90); do
                rpk cluster health -X brokers="$RPK_BROKERS" && break
                rpk topic list -X brokers="$RPK_BROKERS" >/dev/null 2>&1 && break
                sleep 3
              done
              for t in telemetry.raw telemetry.enriched telemetry.alerts telemetry.metrics.derived; do
                rpk topic create "$t" -p 1 -r 1 -X brokers="$RPK_BROKERS" || true
              done
              for t in telemetry.raw telemetry.enriched telemetry.alerts telemetry.metrics.derived; do
                rpk topic alter-config "$t" --set retention.ms=21600000 -X brokers="$RPK_BROKERS" || true
              done
              rpk topic list -X brokers="$RPK_BROKERS"
              echo "topics ready"
          resources:
            requests:
              cpu: 20m
              memory: 64Mi
            limits:
              cpu: 200m
              memory: 256Mi
---
""")

# Fluent Bit DaemonSet + RBAC
parts.append("""# ========== Fluent Bit collector ==========
apiVersion: v1
kind: ServiceAccount
metadata:
  name: fluent-bit
  namespace: obs-system
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: fluent-bit
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: obs-poc-fluent-bit
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: fluent-bit
rules:
  - apiGroups: [""]
    resources: ["namespaces", "pods"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: obs-poc-fluent-bit
  labels:
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: fluent-bit
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: obs-poc-fluent-bit
subjects:
  - kind: ServiceAccount
    name: fluent-bit
    namespace: obs-system
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: fluent-bit
  namespace: obs-system
  labels:
    app: fluent-bit
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: fluent-bit
spec:
  selector:
    matchLabels:
      app: fluent-bit
  template:
    metadata:
      labels:
        app: fluent-bit
        app.kubernetes.io/part-of: obs-poc
        obs.poc/component: fluent-bit
    spec:
      serviceAccountName: fluent-bit
      containers:
        - name: fluent-bit
          image: cr.fluentbit.io/fluent/fluent-bit:3.1.7
          ports:
            - containerPort: 2020
              name: http
          volumeMounts:
            - name: config
              mountPath: /fluent-bit/etc/
            - name: varlog
              mountPath: /var/log
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              cpu: 500m
              memory: 256Mi
      volumes:
        - name: config
          configMap:
            name: fluent-bit-config
        - name: varlog
          hostPath:
            path: /var/log
---
""")

# Streama-lite
parts.append("""# ========== Streama-lite ==========
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streama-lite
  namespace: obs-system
  labels:
    app: streama-lite
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: streama-lite
spec:
  replicas: 1
  selector:
    matchLabels:
      app: streama-lite
  template:
    metadata:
      labels:
        app: streama-lite
        app.kubernetes.io/part-of: obs-poc
        obs.poc/component: streama-lite
    spec:
      containers:
        - name: streama-lite
          image: python:3.12-slim
          command:
            - /bin/sh
            - -c
            - |
              set -e
              apt-get update -qq && apt-get install -y -qq libsnappy1v5 >/dev/null
              pip install --no-cache-dir kafka-python-ng==2.2.3 python-snappy==0.7.3 minio==7.2.9 requests==2.32.3
              exec python /app/main.py
          env:
            - name: KAFKA_BOOTSTRAP
              value: redpanda.obs-system.svc.cluster.local:9092
            - name: OPENSEARCH_URL
              value: http://opensearch.obs-system.svc.cluster.local:9200
            - name: MINIO_ENDPOINT
              value: minio.obs-system.svc.cluster.local:9000
            - name: MINIO_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: accesskey
            - name: MINIO_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: secretkey
            - name: MINIO_SECURE
              value: "false"
            - name: ALERT_ERROR_THRESHOLD
              value: "5"
            - name: ALERT_WINDOW_SEC
              value: "60"
            - name: COLD_FLUSH_SEC
              value: "10"
            - name: COLD_FLUSH_SIZE
              value: "15"
          ports:
            - containerPort: 8080
              name: http
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 50
            periodSeconds: 5
            failureThreshold: 40
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: "1"
              memory: 512Mi
          volumeMounts:
            - name: code
              mountPath: /app
      volumes:
        - name: code
          configMap:
            name: streama-lite-code
---
apiVersion: v1
kind: Service
metadata:
  name: streama-lite
  namespace: obs-system
  labels:
    app: streama-lite
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: streama-lite
spec:
  selector:
    app: streama-lite
  ports:
    - name: http
      port: 8080
      targetPort: 8080
---
""")

# Query API
parts.append("""# ========== Query API ==========
apiVersion: apps/v1
kind: Deployment
metadata:
  name: query-api
  namespace: obs-system
  labels:
    app: query-api
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: query-api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: query-api
  template:
    metadata:
      labels:
        app: query-api
        app.kubernetes.io/part-of: obs-poc
        obs.poc/component: query-api
    spec:
      containers:
        - name: query-api
          image: python:3.12-slim
          command:
            - /bin/sh
            - -c
            - |
              set -e
              pip install --no-cache-dir minio==7.2.9 requests==2.32.3
              exec python /app/main.py
          env:
            - name: OPENSEARCH_URL
              value: http://opensearch.obs-system.svc.cluster.local:9200
            - name: STREAMA_URL
              value: http://streama-lite.obs-system.svc.cluster.local:8080
            - name: MINIO_ENDPOINT
              value: minio.obs-system.svc.cluster.local:9000
            - name: MINIO_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: accesskey
            - name: MINIO_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: minio-creds
                  key: secretkey
            - name: MINIO_SECURE
              value: "false"
          ports:
            - containerPort: 8080
              name: http
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 15
            periodSeconds: 5
            failureThreshold: 30
          resources:
            requests:
              cpu: 50m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 256Mi
          volumeMounts:
            - name: code
              mountPath: /app
      volumes:
        - name: code
          configMap:
            name: query-api-code
---
apiVersion: v1
kind: Service
metadata:
  name: query-api
  namespace: obs-system
  labels:
    app: query-api
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: query-api
spec:
  selector:
    app: query-api
  ports:
    - name: http
      port: 8080
      targetPort: 8080
---
""")

# Helper for demo app deployments
def app_deploy(
    name: str,
    cm: str,
    priority: str,
    ns: str = "demo-apps",
    service: bool = False,
    replicas: int = 1,
    extra_env: str = "",
) -> str:
    svc = ""
    if service:
        svc = f"""
---
apiVersion: v1
kind: Service
metadata:
  name: {name}
  namespace: {ns}
  labels:
    app: {name}
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: demo-app
    obs.poc/tco-priority: {priority}
spec:
  selector:
    app: {name}
  ports:
    - name: http
      port: 8080
      targetPort: 8080
"""
    probe = ""
    if service:
        probe = """
          readinessProbe:
            httpGet:
              path: /healthz
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 5
          ports:
            - containerPort: 8080
              name: http
"""
    else:
        probe = ""

    return f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {ns}
  labels:
    app: {name}
    app.kubernetes.io/part-of: obs-poc
    obs.poc/component: demo-app
    obs.poc/tco-priority: {priority}
spec:
  replicas: {replicas}
  selector:
    matchLabels:
      app: {name}
  template:
    metadata:
      labels:
        app: {name}
        app.kubernetes.io/part-of: obs-poc
        obs.poc/component: demo-app
        obs.poc/tco-priority: {priority}
    spec:
      containers:
        - name: app
          image: python:3.12-alpine
          command: ["python", "/app/main.py"]
          env:
            - name: TCO_PRIORITY
              value: "{priority}"
            - name: APP_NAME
              value: "{name}"
{extra_env}{probe}
          resources:
            requests:
              cpu: 20m
              memory: 32Mi
            limits:
              cpu: 200m
              memory: 128Mi
          volumeMounts:
            - name: code
              mountPath: /app
      volumes:
        - name: code
          configMap:
            name: {cm}
{svc}
---
"""

parts.append("# ========== Demo apps ==========\n")
parts.append(app_deploy("checkout-api", "checkout-api-code", "high", service=True))
parts.append(app_deploy("catalog-api", "catalog-api-code", "medium", service=True))
parts.append(app_deploy("payments-worker", "payments-worker-code", "high"))
parts.append(app_deploy("analytics-batch", "analytics-batch-code", "low"))
parts.append(app_deploy("debug-spammer", "debug-spammer-code", "block", ns="demo-noise"))

loadgen_env = """            - name: CHECKOUT_URL
              value: http://checkout-api.demo-apps.svc.cluster.local:8080/checkout
            - name: CATALOG_URL
              value: http://catalog-api.demo-apps.svc.cluster.local:8080/catalog
"""
parts.append(app_deploy("loadgen", "loadgen-code", "medium", extra_env=loadgen_env))

OUT.write_text("".join(parts))
print(f"Wrote {OUT} ({OUT.stat().st_size} bytes)")
