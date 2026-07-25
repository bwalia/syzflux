#!/usr/bin/env python3
"""query-api: hot OpenSearch + cold MinIO + alerts/stats from streama."""
import gzip
import io
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import requests
from minio import Minio

OPENSEARCH = os.environ.get("OPENSEARCH_URL", "http://opensearch.obs-system:9200").rstrip("/")
STREAMA = os.environ.get("STREAMA_URL", "http://streama-lite.obs-system:8080").rstrip("/")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio.obs-system:9000")
MINIO_ACCESS = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=MINIO_SECURE)
BUCKETS = {"medium": "obs-archive-medium", "low": "obs-archive-low"}


def json_response(handler, code, obj):
    data = json.dumps(obj, default=str).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/healthz":
            return json_response(self, 200, {"status": "ok"})

        if path == "/ai/status":
            return json_response(self, 200, {"status": "not_implemented"})

        if path == "/stats":
            try:
                r = requests.get(f"{STREAMA}/stats", timeout=5)
                return json_response(self, r.status_code, r.json())
            except Exception as e:
                return json_response(self, 502, {"error": str(e)})

        if path == "/alerts/recent":
            try:
                r = requests.get(f"{STREAMA}/stats", timeout=5)
                data = r.json()
                return json_response(self, 200, {"alerts": data.get("recent_alerts", [])})
            except Exception as e:
                return json_response(self, 502, {"error": str(e)})

        if path == "/hot/search":
            q = qs.get("q", ["*"])[0]
            body = {
                "size": 50,
                "sort": [{"timestamp": {"order": "desc", "unmapped_type": "date"}}],
                "query": {
                    "query_string": {
                        "query": q if q != "*" else "*",
                        "default_field": "message",
                        "analyze_wildcard": True,
                    }
                },
            }
            try:
                r = requests.post(f"{OPENSEARCH}/logs-hot/_search", json=body, timeout=10)
                data = r.json()
                hits = [h.get("_source", {}) for h in data.get("hits", {}).get("hits", [])]
                return json_response(self, 200, {"total": data.get("hits", {}).get("total"), "hits": hits})
            except Exception as e:
                return json_response(self, 502, {"error": str(e)})

        if path == "/cold/list":
            priority = qs.get("priority", ["medium"])[0]
            bucket = BUCKETS.get(priority)
            if not bucket:
                return json_response(self, 400, {"error": "priority must be medium|low"})
            prefix = qs.get("prefix", ["logs/"])[0]
            try:
                objects = []
                for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
                    objects.append({"key": obj.object_name, "size": obj.size, "last_modified": obj.last_modified})
                    if len(objects) >= 100:
                        break
                return json_response(self, 200, {"bucket": bucket, "objects": objects})
            except Exception as e:
                return json_response(self, 502, {"error": str(e)})

        if path == "/cold/read":
            priority = qs.get("priority", ["medium"])[0]
            key = qs.get("key", [None])[0]
            bucket = BUCKETS.get(priority)
            if not bucket or not key:
                return json_response(self, 400, {"error": "priority and key required"})
            try:
                resp = minio_client.get_object(bucket, key)
                raw = resp.read()
                resp.close()
                resp.release_conn()
                if key.endswith(".gz"):
                    raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
                text = raw.decode("utf-8", errors="replace")
                lines = [json.loads(line) for line in text.splitlines() if line.strip()][:50]
                return json_response(self, 200, {"bucket": bucket, "key": key, "events": lines})
            except Exception as e:
                return json_response(self, 502, {"error": str(e)})

        return json_response(self, 404, {"error": "not found", "paths": [
            "/healthz", "/stats", "/alerts/recent", "/hot/search?q=ERROR",
            "/cold/list?priority=medium", "/cold/read?priority=medium&key=...", "/ai/status"
        ]})


if __name__ == "__main__":
    print("query-api starting", flush=True)
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
