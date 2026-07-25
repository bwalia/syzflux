#!/usr/bin/env python3
"""debug-spammer: Block TCO noisy logger (must be dropped)."""
import json, os, time

PRIORITY = os.environ.get("TCO_PRIORITY", "block")
APP = os.environ.get("APP_NAME", "debug-spammer")


def main():
    n = 0
    while True:
        n += 1
        print(json.dumps({
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "level": "DEBUG",
            "app": APP,
            "priority": PRIORITY,
            "message": f"noise spam line {n}",
            "noise": True,
        }), flush=True)
        time.sleep(0.5)


if __name__ == "__main__":
    main()
