import json
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer

TOPIC = "orders-events-validated"
BOOTSTRAP_SERVERS = "kafka:9092"


def minute_bucket(timestamp_str):
    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
    return dt.isoformat()


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    totals = defaultdict(float)
    counts = defaultdict(int)
    total_events = 0

    for message in consumer:
        event = message.value
        bucket = minute_bucket(event["event_timestamp"])
        totals[bucket] += event["amount"]
        counts[bucket] += 1
        total_events += 1

    consumer.close()

    print(f"\nIndependent verification - {total_events} events read from '{TOPIC}'\n")
    for bucket in sorted(totals):
        print(f"window_start={bucket}  total_revenue={round(totals[bucket], 2)}  order_count={counts[bucket]}")


if __name__ == "__main__":
    main()