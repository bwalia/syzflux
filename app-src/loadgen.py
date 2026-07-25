#!/usr/bin/env python3
"""loadgen: continuous traffic to checkout + catalog."""
import json, os, random, time, urllib.request

CHECKOUT = os.environ.get("CHECKOUT_URL", "http://checkout-api.demo-apps:8080/checkout")
CATALOG = os.environ.get("CATALOG_URL", "http://catalog-api.demo-apps:8080/catalog")


def hit(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            r.read()
    except Exception as e:
        print(json.dumps({"level": "WARN", "app": "loadgen", "priority": "medium", "message": str(e)}), flush=True)


def main():
    print(json.dumps({"level": "INFO", "app": "loadgen", "priority": "medium", "message": "loadgen started"}), flush=True)
    while True:
        url = CHECKOUT + ("?fail=1" if random.random() < 0.15 else "")
        hit(url)
        hit(CATALOG)
        time.sleep(random.uniform(0.4, 1.2))


if __name__ == "__main__":
    main()
