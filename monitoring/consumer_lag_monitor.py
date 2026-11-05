import sys

from kafka import KafkaConsumer, TopicPartition

BOOTSTRAP_SERVERS = "kafka:9092"
TOPIC = "orders-events"
GROUP_ID = "validating-consumer-group"
LAG_THRESHOLD = 50


def main():
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        enable_auto_commit=False,
    )

    partitions = consumer.partitions_for_topic(TOPIC)
    if not partitions:
        print(f"No partitions found for topic '{TOPIC}'. Is the topic name correct?")
        sys.exit(2)

    topic_partitions = [TopicPartition(TOPIC, p) for p in sorted(partitions)]
    consumer.assign(topic_partitions)

    end_offsets = consumer.end_offsets(topic_partitions)
    committed_offsets = {tp: (consumer.committed(tp) or 0) for tp in topic_partitions}

    total_lag = 0
    print(f"Consumer group '{GROUP_ID}' on topic '{TOPIC}':\n")
    print(f"{'partition':<10}{'committed':<12}{'end_offset':<12}{'lag':<8}")
    for tp in topic_partitions:
        committed = committed_offsets[tp]
        end = end_offsets[tp]
        lag = end - committed
        total_lag += lag
        print(f"{tp.partition:<10}{committed:<12}{end:<12}{lag:<8}")

    consumer.close()

    print(f"\nTotal lag: {total_lag} (threshold: {LAG_THRESHOLD})")

    if total_lag > LAG_THRESHOLD:
        print(f"ALERT: consumer group '{GROUP_ID}' lag ({total_lag}) exceeds threshold ({LAG_THRESHOLD})")
        sys.exit(1)
    else:
        print(f"OK: consumer group '{GROUP_ID}' lag is within threshold")
        sys.exit(0)


if __name__ == "__main__":
    main()