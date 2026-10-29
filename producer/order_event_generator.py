import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "orders-events"
BOOTSTRAP_SERVERS = "kafka:9092"

STATUSES = ["completed", "pending", "cancelled"]
PRODUCTS = [f"PROD-{i}" for i in range(1, 51)]


def make_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
    )


def generate_event(order_id):
    customer_id = random.randint(1000, 1999)
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": random.choice(PRODUCTS),
        "amount": round(random.uniform(5, 500), 2),
        "status": random.choices(STATUSES, weights=[0.7, 0.2, 0.1])[0],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main(num_events=200, min_delay=0.05, max_delay=0.4):
    producer = make_producer()
    order_id = 1
    sent = 0
    try:
        while sent < num_events:
            event = generate_event(order_id)
            producer.send(TOPIC, key=event["customer_id"], value=event)
            print(f"sent order_id={event['order_id']} customer_id={event['customer_id']} "
                  f"status={event['status']} amount={event['amount']}")
            order_id += 1
            sent += 1
            time.sleep(random.uniform(min_delay, max_delay))
    finally:
        producer.flush()
        producer.close()
        print(f"\nDone. Sent {sent} events to topic '{TOPIC}'.")


if __name__ == "__main__":
    main()
