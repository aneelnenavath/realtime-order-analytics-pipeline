import json

from kafka import KafkaProducer

TOPIC = "orders-events"
BOOTSTRAP_SERVERS = "kafka:9092"

# v1 schema: original producer contract, no "region" field
V1_EVENTS = [
    {"order_id": 9101, "customer_id": 1600, "product_id": "PROD-10", "amount": 55.00,
     "status": "completed", "event_timestamp": "2026-11-06T09:00:00Z"},
    {"order_id": 9102, "customer_id": 1601, "product_id": "PROD-11", "amount": 120.50,
     "status": "pending", "event_timestamp": "2026-11-06T09:00:01Z"},
    {"order_id": 9103, "customer_id": 1602, "product_id": "PROD-12", "amount": 89.99,
     "status": "completed", "event_timestamp": "2026-11-06T09:00:02Z"},
]

# v2 schema: additive change, new optional "region" field
V2_EVENTS = [
    {"order_id": 9104, "customer_id": 1603, "product_id": "PROD-13", "amount": 210.00,
     "status": "completed", "event_timestamp": "2026-11-06T09:00:03Z", "region": "UK"},
    {"order_id": 9105, "customer_id": 1604, "product_id": "PROD-14", "amount": 34.20,
     "status": "cancelled", "event_timestamp": "2026-11-06T09:00:04Z", "region": "EU"},
    {"order_id": 9106, "customer_id": 1605, "product_id": "PROD-15", "amount": 150.75,
     "status": "completed", "event_timestamp": "2026-11-06T09:00:05Z", "region": "US"},
]

# breaking schema change: upstream renamed customer_id -> client_id without coordinating
BREAKING_EVENTS = [
    {"order_id": 9107, "client_id": 1606, "product_id": "PROD-16", "amount": 60.00,
     "status": "completed", "event_timestamp": "2026-11-06T09:00:06Z"},
    {"order_id": 9108, "client_id": 1607, "product_id": "PROD-17", "amount": 95.00,
     "status": "completed", "event_timestamp": "2026-11-06T09:00:07Z"},
]


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        acks="all",
    )

    all_events = (
        [("v1", e) for e in V1_EVENTS]
        + [("v2", e) for e in V2_EVENTS]
        + [("breaking", e) for e in BREAKING_EVENTS]
    )

    for label, event in all_events:
        key = event.get("customer_id", event.get("client_id"))
        producer.send(TOPIC, key=key, value=event)
        print(f"sent [{label}] order_id={event['order_id']}")

    producer.flush()
    producer.close()
    print(f"\nDone. Sent {len(V1_EVENTS)} v1, {len(V2_EVENTS)} v2, {len(BREAKING_EVENTS)} breaking-schema events to '{TOPIC}'.")


if __name__ == "__main__":
    main()