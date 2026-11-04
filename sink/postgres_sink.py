from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

KAFKA_BOOTSTRAP = "kafka:9092"
SOURCE_TOPIC = "orders-events-validated"

PG_HOST = "orders-postgres"
PG_DB = "orders_analytics"
PG_USER = "pipeline"
PG_PASSWORD = "pipeline_pw"

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("processed_at", StringType()),
    StructField("amount_tier", StringType()),
])


def ensure_table():
    import psycopg2
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS revenue_by_minute (
            window_start TIMESTAMP PRIMARY KEY,
            window_end TIMESTAMP NOT NULL,
            total_revenue NUMERIC(12, 2) NOT NULL,
            order_count INTEGER NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT now()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()


def upsert_batch(batch_df, batch_id):
    import psycopg2
    rows = batch_df.collect()
    if not rows:
        print(f"Batch {batch_id}: no rows")
        return

    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    cur = conn.cursor()
    for row in rows:
        cur.execute("""
            INSERT INTO revenue_by_minute (window_start, window_end, total_revenue, order_count, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (window_start)
            DO UPDATE SET
                window_end = EXCLUDED.window_end,
                total_revenue = EXCLUDED.total_revenue,
                order_count = EXCLUDED.order_count,
                updated_at = now()
        """, (row.window_start, row.window_end, row.total_revenue, row.order_count))
    conn.commit()
    cur.close()
    conn.close()
    print(f"Batch {batch_id}: upserted {len(rows)} window row(s)")


def main():
    ensure_table()

    spark = SparkSession.builder.appName("PostgresRevenueSink").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", SOURCE_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("event_timestamp").cast(TimestampType()))
    )

    revenue_per_minute = (
        parsed
        .withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"))
        .agg(
            _sum("amount").alias("total_revenue"),
            _count("order_id").alias("order_count"),
        )
        .selectExpr(
            "window.start as window_start",
            "window.end as window_end",
            "round(total_revenue, 2) as total_revenue",
            "order_count",
        )
    )

    query = (
        revenue_per_minute.writeStream
        .outputMode("update")
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", "/tmp/spark-checkpoints/postgres-sink")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination(60)
    query.stop()
    print("Stream stopped after 60 seconds.")


if __name__ == "__main__":
    main()