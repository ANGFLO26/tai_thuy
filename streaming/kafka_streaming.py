"""
Structured Streaming: read a static CSV from HDFS, then emit 1 record every N seconds to Kafka.

Why:
- Your previous version wrote all rows in one batch (too fast).
- This version keeps the app running and throttles output (default: 1 record / 5 seconds).

IMPORTANT:
- Run with spark-submit that includes Kafka connector package, e.g.
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Row


HDFS_PATH = "hdfs://192.168.80.52:9000/data/stream.csv"
KAFKA_BOOTSTRAP_SERVERS = "192.168.80.122:9092"
KAFKA_TOPIC = "input"

# Emit 1 record every INTERVAL_SECONDS (must be an integer >= 1)
INTERVAL_SECONDS = 5

# Checkpoint for Kafka sink (Structured Streaming requires it)
CHECKPOINT_LOCATION = "hdfs://192.168.80.52:9000/checkpoints/hdfs_to_kafka_input_stream"


def main():
    print("[STREAM] Creating Spark session...", file=sys.stdout)
    # Resource target for this job (Standalone):
    # - Total cores: 8
    # - 1 executor x 8 cores
    spark = (
        SparkSession.builder.appName("HDFS_to_Kafka_Stream")
        .config("spark.cores.max", "8")
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("[STREAM] Requested resources: cores.max=8, executor.instances=1, executor.cores=8", file=sys.stdout)

    print(f"[STREAM] Reading CSV from HDFS: {HDFS_PATH}", file=sys.stdout)
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(HDFS_PATH)
    )

    # Bỏ cột label (Class) nếu có (user requirement)
    if "Class" in df.columns:
        df = df.drop("Class")
    if "class" in df.columns:
        df = df.drop("class")

    # Gắn index liên tục cho từng row để join với stream "rate"
    # (không collect về driver)
    indexed_rdd = df.rdd.zipWithIndex().map(lambda x: Row(idx=x[1], **x[0].asDict()))
    df_indexed = spark.createDataFrame(indexed_rdd).cache()

    total = df_indexed.count()
    print(f"[STREAM] Loaded {total} rows. Will emit 1 row every {INTERVAL_SECONDS}s.", file=sys.stdout)

    # Stream ticks: mỗi giây tăng value 1. Chọn mỗi INTERVAL_SECONDS giây để emit 1 record.
    rate_stream = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 1)
        .load()
    )

    ticks = (
        rate_stream
        .where((F.col("value") % F.lit(INTERVAL_SECONDS)) == 0)
        .select((F.col("value") / F.lit(INTERVAL_SECONDS)).cast("long").alias("idx"))
        .where(F.col("idx") < F.lit(total))
    )

    # Join stream(static index) => mỗi tick lấy đúng 1 row
    out_df = (
        ticks.join(df_indexed, on="idx", how="inner")
        .drop("idx")
    )

    json_df = out_df.selectExpr("to_json(struct(*)) as value")

    print(f"[STREAM] Starting Kafka writeStream to topic '{KAFKA_TOPIC}'...", file=sys.stdout)
    query = (
        json_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_TOPIC)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .outputMode("append")
        .start()
    )

    print("[STREAM] Streaming started. Press Ctrl+C to stop.", file=sys.stdout)
    query.awaitTermination()


if __name__ == "__main__":
    main()
