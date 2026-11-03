from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

KAFKA_BOOTSTRAP = "kafka:9092"
SOURCE_TOPIC = "orders-events-validated"

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


def main():
    spark = SparkSession.builder.appName("WindowedRevenueAggregation").getOrCreate()
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
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "/tmp/spark-checkpoints/windowed-revenue")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination(60)
    query.stop()
    print("Stream stopped after 60 seconds.")


if __name__ == "__main__":
    main()