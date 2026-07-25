#!/usr/bin/env python3
"""analytics-batch: Low TCO verbose logger."""
import json, os, random, time, uuid

PRIORITY = os.environ.get("TCO_PRIORITY", "low")
APP = os.environ.get("APP_NAME", "analytics-batch")


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
    log("INFO", "analytics-batch started")
    while True:
        batch = str(uuid.uuid4())[:8]
        for i in range(random.randint(3, 8)):
            log("DEBUG", "aggregate row", batch_id=batch, row=i, dim_a=random.randint(1, 5000), dim_b=f"u-{random.randint(1,99999)}")
        log("INFO", "batch complete", batch_id=batch)
        time.sleep(random.uniform(2.0, 5.0))


if __name__ == "__main__":
    main()
