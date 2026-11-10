"""Retail event simulator -> Eventstream custom endpoint (Event Hub-compatible).

    python simulator/producer.py --dry-run --count 10
    python simulator/producer.py --rate 20 --freezer-fault-store 7

Emits three event types (routed in Eventhouse by $.event_type):
  pos_transaction  - register line items (stock_item_id matches WideWorldImporters 1..227)
  iot_reading      - freezer/fridge temps, door, footfall counters
  click            - online view / add_to_cart / checkout

--freezer-fault-store makes one store's freezers drift warm to trigger the
freezer_too_warm alert.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import UTC, datetime

STORES = list(range(1, 11))  # matches dim_store (partner footfall feed, scripts/generate_partner_files.py)
STOCK_ITEMS = list(range(1, 228))
PAYMENT_METHODS = ["card", "card", "card", "cash", "mobile"]
PAGES = ["home", "search", "category", "product", "cart", "checkout"]


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def pos_event(rng: random.Random) -> dict:
    qty = rng.choices([1, 2, 3, 4, 6, 12], weights=[50, 20, 10, 8, 7, 5])[0]
    price = round(rng.uniform(1.5, 120.0), 2)
    return {
        "event_type": "pos_transaction",
        "transaction_id": str(uuid.uuid4()),
        "store_id": rng.choice(STORES),
        "register_id": rng.randint(1, 8),
        "stock_item_id": rng.choice(STOCK_ITEMS),
        "quantity": qty,
        "unit_price": price,
        "amount": round(qty * price, 2),
        "payment_method": rng.choice(PAYMENT_METHODS),
        "event_time": _now(),
    }


def iot_event(rng: random.Random, fault_store: int | None, tick: int) -> dict:
    store = rng.choice(STORES)
    sensor = rng.choices(["freezer", "fridge", "door", "footfall"], weights=[40, 30, 15, 15])[0]
    if sensor == "freezer":
        value = rng.gauss(-20, 1.0)
        if store == fault_store:
            value += min(tick / 50, 12)  # drifts warm over time
        unit = "C"
    elif sensor == "fridge":
        value, unit = rng.gauss(4, 0.7), "C"
    elif sensor == "door":
        value, unit = float(rng.random() < 0.2), "open"
    else:
        value, unit = float(rng.randint(0, 15)), "visitors"
    return {
        "event_type": "iot_reading",
        "device_id": f"s{store:02d}-{sensor}-{rng.randint(1, 3)}",
        "store_id": store,
        "sensor_type": sensor,
        "value": round(value, 2),
        "unit": unit,
        "event_time": _now(),
    }


def click_event(rng: random.Random, sessions: list[dict]) -> dict:
    if not sessions or rng.random() < 0.3:
        sessions.append({"session_id": str(uuid.uuid4()), "customer_id": rng.randint(1, 1061)})
        del sessions[:-200]
    s = rng.choice(sessions)
    action = rng.choices(["view", "add_to_cart", "checkout"], weights=[80, 15, 5])[0]
    return {
        "event_type": "click",
        "session_id": s["session_id"],
        "customer_id": s["customer_id"],
        "page": "checkout" if action == "checkout" else rng.choice(PAGES),
        "action": action,
        "stock_item_id": rng.choice(STOCK_ITEMS),
        "event_time": _now(),
    }


def generate(rng: random.Random, fault_store: int | None):
    sessions: list[dict] = []
    tick = 0
    while True:
        tick += 1
        kind = rng.choices(["pos", "iot", "click"], weights=[45, 35, 20])[0]
        if kind == "pos":
            yield pos_event(rng)
        elif kind == "iot":
            yield iot_event(rng, fault_store, tick)
        else:
            yield click_event(rng, sessions)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rate", type=float, default=10, help="events per second")
    parser.add_argument("--count", type=int, default=0, help="stop after N events (0 = forever)")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--freezer-fault-store", type=int, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="print events instead of sending")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    events = generate(rng, args.freezer_fault_store)

    producer = None
    if not args.dry_run:
        conn = os.getenv("EVENTSTREAM_CONNECTION_STRING")
        if not conn:
            parser.error("set EVENTSTREAM_CONNECTION_STRING (Eventstream custom endpoint) or use --dry-run")
        from azure.eventhub import EventData, EventHubProducerClient

        producer = EventHubProducerClient.from_connection_string(conn)

    sent = 0
    interval = 1.0 / args.rate if args.rate > 0 else 0
    try:
        while args.count == 0 or sent < args.count:
            n = args.batch_size if args.count == 0 else min(args.batch_size, args.count - sent)
            chunk = [next(events) for _ in range(n)]
            if producer is None:
                for e in chunk:
                    print(json.dumps(e))
            else:
                batch = producer.create_batch()
                for e in chunk:
                    batch.add(EventData(json.dumps(e)))
                producer.send_batch(batch)
                print(f"sent {sent + n} events", end="\r", flush=True)
            sent += n
            if interval:
                time.sleep(interval * n)
    except KeyboardInterrupt:
        pass
    finally:
        if producer is not None:
            producer.close()
    print(f"\ndone - {sent} events", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
