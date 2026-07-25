#!/usr/bin/env python3
"""checkout-api: High TCO sample app."""
import json, os, random, time, uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

PRIORITY = os.environ.get("TCO_PRIORITY", "high")
APP = os.environ.get("APP_NAME", "checkout-api")


def log(level, message, **fields):
    rec = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": level,
        "app": APP,
        "priority": PRIORITY,
        "message": message,
        **fields,
    }
    print(json.dumps(rec), flush=True)


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
        if self.path.startswith("/checkout"):
            order_id = str(uuid.uuid4())[:8]
            latency = random.randint(20, 400)
            fail = "fail=1" in self.path or random.random() < 0.08
            if fail:
                log("ERROR", "checkout failed", order_id=order_id, latency_ms=latency, error="payment_declined")
                self._send(500, {"ok": False, "order_id": order_id, "error": "payment_declined"})
            else:
                log("INFO", "checkout ok", order_id=order_id, latency_ms=latency)
                self._send(200, {"ok": True, "order_id": order_id})
            return
        self._send(404, {"error": "not found"})


if __name__ == "__main__":
    log("INFO", "starting checkout-api")
    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
