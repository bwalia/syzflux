#!/usr/bin/env python3
"""catalog-api: Medium TCO sample app."""
import json, os, random, time
from http.server import BaseHTTPRequestHandler, HTTPServer

PRIORITY = os.environ.get("TCO_PRIORITY", "medium")
APP = os.environ.get("APP_NAME", "catalog-api")
ITEMS = ["sku-100", "sku-200", "sku-300", "sku-400"]


def log(level, message, **fields):
    print(json.dumps({
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": level,
        "app": APP,
        "priority": PRIORITY,
        "message": message,
        **fields,
    }), flush=True)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        return

    def _send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/healthz"):
            self._send(200, {"status": "ok"})
            return
        if self.path.startswith("/catalog"):
            item = random.choice(ITEMS)
            log("INFO", "catalog lookup", item=item, latency_ms=random.randint(5, 80))
            self._send(200, {"item": item, "in_stock": True})
            return
        self._send(404, {"error": "not found"})


if __name__ == "__main__":
    log("INFO", "starting catalog-api")
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
