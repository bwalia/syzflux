#!/usr/bin/env python3
"""streama-lite: Kafka consume -> enrich/alert/TCO route -> OpenSearch / MinIO / drop."""
import gzip
import io
import json
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
import requests

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "redpanda.obs-system:9092")
OPENSEARCH = os.environ.get("OPENSEARCH_URL", "http://opensearch.obs-system:9200").rstrip("/")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio.obs-system:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
ALERT_WINDOW_SEC = int(os.environ.get("ALERT_WINDOW_SEC", "60"))
ALERT_ERROR_THRESHOLD = int(os.environ.get("ALERT_ERROR_THRESHOLD", "5"))
COLD_FLUSH_SEC = int(os.environ.get("COLD_FLUSH_SEC", "15"))
COLD_FLUSH_SIZE = int(os.environ.get("COLD_FLUSH_SIZE", "20"))

stats = {
    "events_total": defaultdict(lambda: defaultdict(int)),  # priority -> outcome -> count
    "alerts_total": 0,
    "dropped_total": 0,
    "opensearch_writes_total": 0,
    "minio_writes_total": 0,
    "enriched_total": 0,
    "errors_seen": 0,
    "started_at": time.time(),
}
stats_lock = threading.Lock()
recent_alerts = deque(maxlen=50)
error_times = defaultdict(deque)  # app -> timestamps
cold_buffers = defaultdict(list)  # (priority, app) -> [events]
cold_lock = threading.Lock()


def utcnow():
    return datetime.now(timezone.utc)


def bump(priority, outcome):
    with stats_lock:
        stats["events_total"][priority][outcome] += 1
        if outcome == "dropped":
            stats["dropped_total"] += 1
        elif outcome == "hot":
            stats["opensearch_writes_total"] += 1
        elif outcome == "cold":
            stats["minio_writes_total"] += 1


def metrics_text():
    lines = []
    with stats_lock:
        for p, outcomes in stats["events_total"].items():
            for o, c in outcomes.items():
                lines.append(f'streama_events_total{{priority="{p}",outcome="{o}"}} {c}')
        lines.append(f'streama_alerts_total {stats["alerts_total"]}')
        lines.append(f'streama_dropped_total {stats["dropped_total"]}')
        lines.append(f'streama_opensearch_writes_total {stats["opensearch_writes_total"]}')
        lines.append(f'streama_minio_writes_total {stats["minio_writes_total"]}')
        lines.append(f'streama_enriched_total {stats["enriched_total"]}')
        lines.append(f'streama_errors_seen {stats["errors_seen"]}')
    return "\n".join(lines) + "\n"


def stats_json():
    with stats_lock:
        return {
            "events_total": {p: dict(o) for p, o in stats["events_total"].items()},
            "alerts_total": stats["alerts_total"],
            "dropped_total": stats["dropped_total"],
            "opensearch_writes_total": stats["opensearch_writes_total"],
            "minio_writes_total": stats["minio_writes_total"],
            "enriched_total": stats["enriched_total"],
            "errors_seen": stats["errors_seen"],
            "uptime_sec": int(time.time() - stats["started_at"]),
            "recent_alerts": list(recent_alerts),
        }


class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        if self.path.startswith("/healthz"):
            body = b'{"status":"ok"}'
            self.send_response(200)
        elif self.path.startswith("/metrics"):
            body = metrics_text().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
        elif self.path.startswith("/stats"):
            body = json.dumps(stats_json()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
        else:
            body = b'{"error":"not found"}'
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_http():
    HTTPServer(("0.0.0.0", 8080), HealthHandler).serve_forever()


def wait_kafka(producer):
    for i in range(60):
        try:
            producer.bootstrap_connected() or True
            # metadata fetch
            producer.partitions_for("telemetry.enriched")
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("kafka not ready")


def ensure_opensearch():
    for _ in range(60):
        try:
            r = requests.get(OPENSEARCH, timeout=3)
            if r.status_code < 500:
                break
        except Exception:
            pass
        time.sleep(2)
    # index template / create index
    mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "processed_at": {"type": "date"},
                "level": {"type": "keyword"},
                "app": {"type": "keyword"},
                "priority": {"type": "keyword"},
                "message": {"type": "text"},
            }
        },
    }
    try:
        requests.put(f"{OPENSEARCH}/logs-hot", json=mapping, timeout=10)
    except Exception as e:
        print(f"opensearch index ensure: {e}", flush=True)


def extract_event(raw_value, headers=None):
    """Parse fluent-bit / app log into a normalized event dict."""
    try:
        obj = json.loads(raw_value)
    except Exception:
        return {
            "timestamp": utcnow().isoformat().replace("+00:00", "Z"),
            "level": "INFO",
            "app": "unknown",
            "priority": "medium",
            "message": raw_value[:500],
            "raw": True,
        }

    priority = None
    app = None
    level = None
    message = None
    inner = {}

    # Fluent Bit nest lift uses lp_ prefix for merged JSON log fields
    if any(k.startswith("lp_") for k in obj.keys()):
        inner = {k[3:]: v for k, v in obj.items() if k.startswith("lp_")}

    if isinstance(obj.get("log"), str):
        try:
            parsed = json.loads(obj["log"])
            if isinstance(parsed, dict):
                inner = {**parsed, **inner}
        except Exception:
            if not message:
                message = obj["log"].strip()
    elif isinstance(obj.get("log"), dict):
        inner = {**obj["log"], **inner}

    # CRI/fluent-bit often keeps the app JSON string in "message"
    msg_field = obj.get("message")
    if isinstance(msg_field, str) and msg_field.strip().startswith("{"):
        try:
            parsed = json.loads(msg_field)
            if isinstance(parsed, dict):
                inner = {**parsed, **inner}
        except Exception:
            pass

    # Merge_Log may place fields at top-level
    for key in ("app", "priority", "level", "message", "timestamp"):
        if key in obj and key not in inner:
            inner[key] = obj[key]

    if inner:
        priority = inner.get("priority")
        app = inner.get("app")
        level = inner.get("level")
        message = inner.get("message", message)

    k8s = obj.get("kubernetes") or {}
    labels = k8s.get("labels") or {}
    if not priority:
        priority = (
            labels.get("obs.poc/tco-priority")
            or labels.get("obs_poc_tco_priority")
            or labels.get("obs-poc-tco-priority")
        )
    if not app:
        app = labels.get("app") or labels.get("app.kubernetes.io/name") or k8s.get("pod_name") or "unknown"

    priority = (priority or "medium").lower()
    level = (level or "INFO").upper()
    if level in ("WARNING",):
        level = "WARN"

    event = {
        "timestamp": inner.get("timestamp") or obj.get("@timestamp") or utcnow().isoformat().replace("+00:00", "Z"),
        "level": level,
        "app": app,
        "priority": priority,
        "message": message or "",
        "namespace": k8s.get("namespace_name") or inner.get("namespace"),
        "pod": k8s.get("pod_name"),
    }
    for k in ("order_id", "tx_id", "latency_ms", "error", "item", "batch_id", "noise"):
        if k in inner:
            event[k] = inner[k]
    return event


def write_hot(producer_unused, event):
    doc = dict(event)
    try:
        r = requests.post(f"{OPENSEARCH}/logs-hot/_doc", json=doc, timeout=5)
        if r.status_code >= 300:
            print(f"opensearch write fail: {r.status_code} {r.text[:200]}", flush=True)
            return False
        bump(event["priority"], "hot")
        return True
    except Exception as e:
        print(f"opensearch error: {e}", flush=True)
        return False


def flush_cold(minio_client, priority, app, events):
    if not events:
        return
    bucket = "obs-archive-medium" if priority == "medium" else "obs-archive-low"
    now = utcnow()
    key = f"logs/dt={now.strftime('%Y-%m-%d')}/hour={now.strftime('%H')}/{app}-{uuid.uuid4().hex[:10]}.jsonl.gz"
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for ev in events:
            gz.write((json.dumps(ev) + "\n").encode())
    data = buf.getvalue()
    try:
        minio_client.put_object(bucket, key, io.BytesIO(data), length=len(data), content_type="application/gzip")
        for _ in events:
            bump(priority, "cold")
        print(f"minio wrote s3://{bucket}/{key} n={len(events)}", flush=True)
    except Exception as e:
        print(f"minio write error: {e}", flush=True)


def cold_flusher(minio_client):
    while True:
        time.sleep(COLD_FLUSH_SEC)
        with cold_lock:
            items = list(cold_buffers.items())
            cold_buffers.clear()
        for (priority, app), events in items:
            flush_cold(minio_client, priority, app, events)


def maybe_alert(producer, event):
    if event.get("level") != "ERROR":
        return
    with stats_lock:
        stats["errors_seen"] += 1
    app = event.get("app") or "unknown"
    now = time.time()
    q = error_times[app]
    q.append(now)
    while q and now - q[0] > ALERT_WINDOW_SEC:
        q.popleft()
    if len(q) >= ALERT_ERROR_THRESHOLD:
        alert = {
            "timestamp": utcnow().isoformat().replace("+00:00", "Z"),
            "type": "error_rate",
            "app": app,
            "priority": event.get("priority"),
            "count": len(q),
            "window_sec": ALERT_WINDOW_SEC,
            "threshold": ALERT_ERROR_THRESHOLD,
            "message": f"In-stream alert: {app} ERROR count {len(q)} in {ALERT_WINDOW_SEC}s",
            "sample": event.get("message"),
        }
        producer.send("telemetry.alerts", json.dumps(alert).encode())
        with stats_lock:
            stats["alerts_total"] += 1
            recent_alerts.appendleft(alert)
        # reset window to avoid alert flood
        error_times[app].clear()
        print(f"ALERT {alert['message']}", flush=True)


def maybe_derived_metric(producer, event):
    if event.get("level") != "ERROR":
        return
    metric = {
        "timestamp": utcnow().isoformat().replace("+00:00", "Z"),
        "metric": "log_errors",
        "app": event.get("app"),
        "priority": event.get("priority"),
        "value": 1,
    }
    producer.send("telemetry.metrics.derived", json.dumps(metric).encode())


def route(event, producer, minio_client):
    priority = event.get("priority", "medium")
    if priority == "block":
        bump("block", "dropped")
        return
    if priority == "high":
        write_hot(producer, event)
        return
    if priority in ("medium", "low"):
        key = (priority, event.get("app") or "unknown")
        with cold_lock:
            cold_buffers[key].append(event)
            if len(cold_buffers[key]) >= COLD_FLUSH_SIZE:
                events = cold_buffers.pop(key)
                flush_cold(minio_client, key[0], key[1], events)
        return
    # unknown -> treat as medium cold
    event["priority"] = "medium"
    route(event, producer, minio_client)


def main():
    print("streama-lite starting", flush=True)
    threading.Thread(target=start_http, daemon=True).start()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA.split(","),
        acks=1,
        retries=5,
        linger_ms=50,
    )
    for i in range(90):
        try:
            consumer = KafkaConsumer(
                "telemetry.raw",
                bootstrap_servers=KAFKA.split(","),
                group_id="streama-lite",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
            )
            break
        except Exception as e:
            print(f"waiting kafka consumer: {e}", flush=True)
            time.sleep(2)
    else:
        raise RuntimeError("cannot connect kafka consumer")

    ensure_opensearch()
    minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=MINIO_SECURE)
    threading.Thread(target=cold_flusher, args=(minio_client,), daemon=True).start()
    print("streama-lite ready", flush=True)

    while True:
        try:
            polled = consumer.poll(timeout_ms=1000)
            for _tp, records in polled.items():
                for msg in records:
                    try:
                        raw = msg.value.decode("utf-8", errors="replace")
                    except Exception as e:
                        print(f"decode skip: {e}", flush=True)
                        continue
                    event = extract_event(raw)
                    event["processed_at"] = utcnow().isoformat().replace("+00:00", "Z")
                    event["level"] = (event.get("level") or "INFO").upper()
                    producer.send("telemetry.enriched", json.dumps(event).encode())
                    with stats_lock:
                        stats["enriched_total"] += 1
                    maybe_alert(producer, event)
                    maybe_derived_metric(producer, event)
                    route(event, producer, minio_client)
        except Exception as e:
            print(f"loop error: {e}", flush=True)
            time.sleep(1)


if __name__ == "__main__":
    main()
