# Real-Time Order Analytics Pipeline

A Kafka-based streaming pipeline that ingests simulated e-commerce
order events in real time, validates and enriches them, computes
rolling revenue aggregations, and sinks the results into Postgres -
built incrementally, one capability at a time, with every stage proven
against a live Kafka broker rather than just described.

## Architecture

    Order producer --> Kafka (orders-events, 3 partitions)
        --> validating/enriching consumer --> clean / dead-letter topics
        --> windowed stream aggregation (revenue per minute)
        --> idempotent Postgres sink
        (+ consumer lag monitoring, schema evolution handling)

Every stage above is built, wired together, and proven end-to-end (see Day 67).

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

Full pipeline, start to finish:

1. `producer/order_event_generator.py` - produce orders
2. `consumer/validating_consumer.py` - validate, enrich, split clean/DLQ
3. `aggregation/windowed_revenue.py` - per-minute revenue aggregation
4. `sink/postgres_sink.py` - idempotent upsert into Postgres
5. `monitoring/consumer_lag_monitor.py` - exit-code health check for the consumer group

See Day 67 below for a full run of all five, back to back, against one fresh batch.

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


## Day 64 — Idempotent Postgres Sink

Added the final stage of the pipeline shown in the original architecture plan: a Spark Structured Streaming job that writes the windowed revenue aggregation into Postgres, idempotently.

**Infrastructure**

A new long-lived `orders-postgres` container (Postgres 16, named volume for durability, on `daylog-net`) — long-lived like `kafka`, unlike the disposable Python containers used for the stream components.

**The sink**

`sink/postgres_sink.py` re-runs the same 1-minute windowed revenue aggregation from Day 63, but instead of a console sink, uses `foreachBatch` to run `INSERT ... ON CONFLICT (window_start) DO UPDATE` against a `revenue_by_minute` table per micro-batch — `window_start` is the primary key, so re-processing the same window updates that row in place instead of creating a duplicate.

**Proving idempotency, not just claiming it**

Ran the sink job twice against the same backlog of events. Both runs logged `upserted 2 window row(s)` — not 4 after the second run. Queried Postgres directly after each run: both times, exactly 2 rows, with identical totals matching Day 63's independently-verified numbers (`22:56–22:57`: $1,039.85 / 5 orders; `22:57–22:58`: $52,076.09 / 195 orders). The `updated_at` column changed between runs (`14:45:48` → `14:51:57`), confirming the second run genuinely re-processed and upserted in place rather than silently no-op'ing.

**Proof**

Two full pipeline runs, same 2 rows both times, same revenue totals both times, `updated_at` advancing each run — the strongest kind of idempotency proof available: actually running it twice and checking the end state, not just reading the upsert SQL and assuming it's correct.


## Day 65 — Consumer Lag Monitoring and Alerting

Added a monitoring check for the Day 62 validating consumer's actual Kafka lag — not a guess, a real comparison between committed offsets and the topic's live end offsets.

**The monitor**

`monitoring/consumer_lag_monitor.py` checks `validating-consumer-group`'s lag on `orders-events`: for each partition, it compares the group's committed offset against the topic's current end offset, sums the difference into a total, and compares that against a threshold. It exits `0` (OK) when healthy or `1` (ALERT) when not — the same contract a real scheduler or alerting system (cron, a monitoring platform, PagerDuty's check runner) expects from a health check, rather than something that has to be read by eye.

**Proving it catches a real problem, not a synthetic one**

Three-stage test against the live broker:

1. Baseline: 0 lag, `OK`, exit 0 (73+51+81 committed = 205 end offset, fully caught up from Day 62).
2. Reran the Day 61 producer to push 200 more events onto `orders-events` without consuming them. Reran the monitor: correctly detected 200 lag (61+65+74 across partitions), printed `ALERT: ... lag (200) exceeds threshold (50)`, exit 1.
3. Ran the Day 62 validating consumer to drain the backlog (valid=200, dlq=0). Reran the monitor: lag back to 0 (134+116+155 committed = 405 end offset), `OK`, exit 0.

**Proof**

Three full states — healthy, deliberately broken, repaired — each confirmed against the live consumer group's real offsets, with the monitor correctly classifying all three via its exit code.

## Day 66 — Schema Evolution Handling Mid-Stream

Tested the pipeline against three real schema-drift scenarios in a single mixed batch: an additive field, a breaking rename, and the original schema unchanged — proving the system handles all three without crashing.

**The scenario**

`producer/schema_evolution_demo.py` sends 8 events onto `orders-events`: 3 **v1** events using the original schema unchanged, 3 **v2** events with a new additive `region` field (`UK`/`EU`/`US`), and 2 **breaking** events where `customer_id` was renamed to `client_id` without coordination — a genuinely incompatible change, not a synthetic one.

**Evolving the consumer, not forking it**

Updated the Day 62 `consumer/validating_consumer.py` in place, the way a real system actually evolves. Required fields stayed exactly as strict as before — unchanged on purpose, since that's what still catches the breaking rename automatically with zero new code. Added `OPTIONAL_FIELDS_WITH_DEFAULTS` so a new field like `region` can appear or not appear without anything breaking: present, it's preserved; absent, it defaults to `"UNKNOWN"` during enrichment.

**Proof**

Ran the mixed batch through: `valid=6 dlq=2`. v1 events landed as valid with `region=UNKNOWN` applied. v2 events landed as valid with their real region values intact. Both breaking-rename events landed correctly in the DLQ with `reason=missing required field: customer_id` - caught by the exact same validation logic that's been running since Day 62, with no changes needed to handle that case. Three different schema outcomes from one batch, nothing crashed, nothing silently corrupted.
## Day 67 - End-to-end verification + README polish

With every stage of the pipeline built across the week - producer, validating/DLQ consumer, windowed aggregation, idempotent Postgres sink, consumer lag monitoring, schema evolution handling - the only thing not yet proven was that they all actually work together, back to back, against one fresh batch of data rather than each being tested in isolation on its own day.

### Proof

Sent 200 fresh events via the Day 61 producer, then ran the full chain against them in order:

    1. producer/order_event_generator.py  -> 200 events sent to orders-events
    2. consumer/validating_consumer.py    -> valid=200 dlq=0
    3. aggregation/windowed_revenue.py    -> fresh windows aggregated correctly (183 + 17 = 200 orders)
    4. sink/postgres_sink.py              -> upserted 6 window rows (idempotent upsert, no duplicates)
    5. monitoring/consumer_lag_monitor.py -> total lag: 0, OK, exit 0

All five stages passed clean on the same run, with no stage-specific workarounds needed. Also polished the Architecture and Running It sections above, which had been written day-by-day in a forward-looking, work-in-progress tense - they now describe the pipeline as what it is: a complete, working system.
