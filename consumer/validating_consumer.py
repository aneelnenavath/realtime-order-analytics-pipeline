import json
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

SOURCE_TOPIC = "orders-events"
VALID_TOPIC = "orders-events-validated"
DLQ_TOPIC = "orders-events-dlq"
BOOTSTRAP_SERVERS = "kafka:9092"

VALID_STATUSES = {"completed", "pending", "cancelled"}
REQUIRED_FIELDS = ["order_id", "customer_id", "product_id", "amount", "status", "event_timestamp"]

# Optional fields: may or may not be present depending on which producer version sent the
# event. Missing one of these is NOT a validation failure - it just gets a default value
# during enrichment, so older events and newer events can both flow through unchanged.
OPTIONAL_FIELDS_WITH_DEFAULTS = {
    "region": "UNKNOWN",
}


def validate(event):
    """Return None if the event is valid, otherwise a string reason it failed.

    Only REQUIRED_FIELDS are enforced here. A field disappearing (e.g. an upstream rename
    like customer_id -> client_id without coordination) is still correctly caught as a
    missing required field and routed to the DLQ - the contract is defined by what we
    require, not by rejecting anything we don't recognise.
    """
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            return f"missing required field: {field}"

    if not isinstance(event["customer_id"], int) or not (1000 <= event["customer_id"] <= 1999):
        return f"customer_id out of expected range: {event.get('customer_id')}"

    if not isinstance(event["amount"], (int, float)) or event["amount"] <= 0:
        return f"amount must be a positive number: {event.get('amount')}"

    if event["status"] not in VALID_STATUSES:
        return f"invalid status: {event.get('status')}"

    return None


def amount_tier(amount):
    if amount < 50:
        return "low"
    elif amount < 200:
        return "medium"
    return "high"


def enrich(event):
    enriched = dict(event)
    enriched["processed_at"] = datetime.now(timezone.utc).isoformat()
    enriched["amount_tier"] = amount_tier(event["amount"])
    for field, default in OPTIONAL_FIELDS_WITH_DEFAULTS.items():
        enriched.setdefault(field, default)
    return enriched


def main():
    consumer = KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="validating-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k is not None else None,
        acks="all",
    )

    valid_count = 0
    dlq_count = 0

    try:
        for message in consumer:
            event = message.value
            reason = validate(event)

            if reason is None:
                enriched = enrich(event)
                producer.send(VALID_TOPIC, key=enriched.get("customer_id"), value=enriched)
                valid_count += 1
                print(f"VALID  order_id={event.get('order_id')} tier={enriched['amount_tier']} region={enriched['region']}")
            else:
                dlq_record = {
                    "original_event": event,
                    "error_reason": reason,
                    "failed_at": datetime.now(timezone.utc).isoformat(),
                    "source_partition": message.partition,
                    "source_offset": message.offset,
                }
                producer.send(DLQ_TOPIC, key=event.get("customer_id"), value=dlq_record)
                dlq_count += 1
                print(f"DLQ    order_id={event.get('order_id')} reason={reason}")
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        print(f"\nDone. valid={valid_count} dlq={dlq_count}")


if __name__ == "__main__":
    main()