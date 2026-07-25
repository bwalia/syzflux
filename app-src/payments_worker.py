#!/usr/bin/env python3
"""payments-worker: High TCO background worker."""
import json, os, random, time, uuid

PRIORITY = os.environ.get("TCO_PRIORITY", "high")
APP = os.environ.get("APP_NAME", "payments-worker")


def log(level, message, **fields):
    print(json.dumps({
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": level,
        "app": APP,
        "priority": PRIORITY,
        "message": message,
        **fields,
    }), flush=True)


def main():
    log("INFO", "payments-worker started")
    while True:
        tx = str(uuid.uuid4())[:8]
        if random.random() < 0.12:
            log("ERROR", "payment settlement failed", tx_id=tx, error="timeout", stack="Traceback: settle()\n  File worker.py")
        else:
            log("INFO", "payment settled", tx_id=tx, amount_cents=random.randint(100, 9000))
        time.sleep(random.uniform(1.0, 3.0))


if __name__ == "__main__":
    main()
