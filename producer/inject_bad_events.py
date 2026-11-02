import json

from kafka import KafkaProducer

TOPIC = "orders-events"
BOOTSTRAP_SERVERS = "kafka:9092"

BAD_EVENTS = [
    # missing required field (no 'status')
    {"order_id": 9001, "customer_id": 1500, "product_id": "PROD-5", "amount": 42.50,
     "event_timestamp": "2026-11-02T10:00:00Z"},
    # negative amount
    {"order_id": 9002, "customer_id": 1501, "product_id": "PROD-6", "amount": -15.00,
     "status": "completed", "event_timestamp": "2026-11-02T10:00:01Z"},
    # invalid status value
    {"order_id": 9003, "customer_id": 1502, "product_id": "PROD-7", "amount": 75.00,
     "status": "shipped", "event_timestamp": "2026-11-02T10:00:02Z"},
    # customer_id out of the expected range
    {"order_id": 9004, "customer_id": 42, "product_id": "PROD-8", "amount": 120.00,
     "status": "pending", "event_timestamp": "2026-11-02T10:00:03Z"},
    # amount is a string instead of a number
    {"order_id": 9005, "customer_id": 1503, "product_id": "PROD-9", "amount": "fifty",
     "status": "completed", "event_timestamp": "2026-11-02T10:00:04Z"},
]


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        acks="all",
    )

    for event in BAD_EVENTS:
        producer.send(TOPIC, key=event.get("customer_id"), value=event)
        print(f"sent bad event order_id={event['order_id']}")

    producer.flush()
    producer.close()
    print(f"\nDone. Sent {len(BAD_EVENTS)} deliberately invalid events to '{TOPIC}'.")


if __name__ == "__main__":
    main()