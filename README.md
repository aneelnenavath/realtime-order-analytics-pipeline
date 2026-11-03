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

## Day 62 — Validating/Enriching Consumer + Dead-Letter Queue

Built the first processing stage downstream of the raw event stream: a consumer that validates and enriches every order event, splitting the output across two new topics instead of writing bad data forward silently.

**New topics**

- `orders-events-validated` — clean records, enriched, 3 partitions
- `orders-events-dlq` — rejected records, wrapped with the reason they failed, 3 partitions

**Validation rules**

All required fields present (`order_id`, `customer_id`, `product_id`, `amount`, `status`, `event_timestamp`); `amount` must be a positive number; `status` must be one of `completed`/`pending`/`cancelled`; `customer_id` must fall in the expected range (1000–1999).

**Enrichment on valid records**

Adds `processed_at` (UTC timestamp of when the consumer handled it) and `amount_tier` (`low`/`medium`/`high`, bucketed off `amount`).

**On failure**

The original record is preserved untouched, wrapped with `error_reason`, `failed_at`, and the source partition/offset it came from, then sent to the DLQ — nothing is silently dropped.

**Testing the DLQ path**

Added `producer/inject_bad_events.py`, which sends five deliberately invalid events — one per validation rule — directly onto `orders-events`, so the DLQ path has something real to prove against rather than relying on all-valid traffic.

**Proof**

Ran the consumer against all 205 events sitting in `orders-events` (the 200 good ones from Day 61 plus today's 5 bad ones). Console output: `valid=200 dlq=5`. Verified independently via each output topic's authoritative partition offsets:

- `orders-events-validated`: 72 + 50 + 78 = 200
- `orders-events-dlq`: 1 + 1 + 3 = 5

200 + 5 = 205 — matches exactly, zero loss anywhere. Inspected all 5 DLQ messages directly and confirmed each `error_reason` matches the rule it was designed to break.


## Day 63 — Windowed Stream Aggregation (Revenue per Minute)

Added the first real stream-processing stage: a Spark Structured Streaming job that reads the validated event stream and computes revenue in 1-minute tumbling windows, with an independent script verifying its output.

**The job**

`aggregation/windowed_revenue.py` reads from `orders-events-validated`, parses the JSON, casts `event_timestamp` to a proper timestamp, and groups into 1-minute tumbling windows with a 2-minute watermark, aggregating total revenue and order count per window. Output mode `update`, printed to console every 10 seconds, stopping itself after 60.

**Independent verification, not just trust**

Rather than take Spark's console output at face value, wrote `aggregation/verify_revenue.py` — a separate script using plain `kafka-python`, no Spark at all — that reads the same topic and hand-computes the same per-minute totals by bucketing `event_timestamp` to the minute.

**Proof**

Both approaches landed on identical numbers for both windows:

- `22:56–22:57`: total_revenue=1039.85, order_count=5
- `22:57–22:58`: total_revenue=52076.09, order_count=195

200 orders and $53,115.94 total revenue either way — two independent code paths agreeing exactly is the same kind of proof as the partition-offset checks on earlier days, just applied to an aggregation instead of a count.

**Infrastructure note**

Spark needs a real JVM, so swapped the lightweight `python:3.11-slim` pattern for the official `apache/spark:3.5.0` image, pulling the Kafka connector via `--packages` at runtime (Maven, cached locally in a gitignored `.ivy-cache/` folder for faster reruns). Also hit the Kafka broker container having exited again since yesterday (same as Day 62) — same fix, `docker start kafka`.