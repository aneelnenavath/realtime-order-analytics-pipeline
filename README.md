# Real-Time Order Analytics Pipeline

A Kafka-based streaming pipeline that ingests simulated e-commerce
order events in real time, validates and enriches them, computes
rolling revenue aggregations, and sinks the results into Postgres -
built incrementally, one capability at a time, with every stage proven
against a live Kafka broker rather than just described.

## Architecture (growing day by day)

    Order producer --> Kafka (orders-events, 3 partitions)
        --> validating/enriching consumer --> clean / dead-letter topics
        --> windowed stream aggregation (revenue per minute)
        --> idempotent Postgres sink
        (+ consumer lag monitoring, schema evolution handling)

## Stack

Kafka (KRaft mode, no Zookeeper), Python (kafka-python), Spark
Structured Streaming (from Day 63), Postgres, all running in Docker.
No dbt anywhere in this project.

## Running it

Everything here runs against a real Kafka broker in a Docker container
named `kafka`, attached to a Docker network (`daylog-net`). Python
components run inside disposable containers on that same network
(`docker run --network daylog-net ...`) rather than requiring a local
Python environment with kafka-python installed - this keeps the project
portable for anyone cloning the repo.

## Day 61 - Project kickoff: architecture + order-event producer

Set up the Kafka topic this whole project streams through:
`orders-events`, 3 partitions, replication factor 1.

producer/order_event_generator.py simulates a live order feed - not a
batch dump. It generates one order event at a time (order_id,
customer_id, product_id, amount, status, event_timestamp as JSON) and
sends it to Kafka with a randomized 50-400ms delay before the next one,
so the stream behaves like genuinely arriving traffic rather than a
burst. Each event is keyed by customer_id: Kafka only guarantees
ordering within a partition, so keying by customer ensures one
customer's events stay ordered relative to each other, even though
different customers' events can land on different partitions and
interleave. The producer uses acks="all", meaning it waits for the
broker to fully confirm durability before considering a send
successful.

### Proof

Ran the producer for 200 simulated order events against the live
`orders-events` topic:

    Done. Sent 200 events to topic 'orders-events'.

Consumed a sample back from the topic to confirm the events landed
correctly and match the producer's schema exactly - including seeing
non-sequential order_ids in a single partition's read (3, 13, 14),
which is expected: with 3 partitions and a customer_id key, global
ordering across the whole topic was never promised, only ordering
within each partition.

Checked the authoritative per-partition offsets to confirm the full
count with no loss and no duplication:

    orders-events:0:72
    orders-events:1:50
    orders-events:2:78

72 + 50 + 78 = 200 - exactly matching the number of events sent. The
uneven split across partitions reflects the randomly generated
customer_id values being hashed to partitions by Kafka's default
partitioner, not a perfectly even round-robin distribution.

### Note on local setup

Running this project's Python components from the Windows host
directly fails, because the Kafka broker's `KAFKA_ADVERTISED_LISTENERS`
points clients to the broker at hostname `kafka`, which only resolves
inside the Docker network - and no port was published from the
container to the host either. Rather than reconfigure the existing,
already-running broker (which holds state from earlier Kafka work and
risks being disrupted), every Python component in this project runs
inside a disposable container on the same Docker network as the
broker instead, which is also the more portable, more professional way
to build something meant to be cloned and run by someone else later.
