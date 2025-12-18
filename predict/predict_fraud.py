"""
Streaming fraud prediction:
- Read events from Kafka topic 'input' at 192.168.80.122:9092
- Load trained Spark RF model from HDFS
  (hdfs://192.168.80.52:9000/model)
- Run predictions
- Write results to Kafka topic 'output' at 192.168.80.122:9092
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.functions import vector_to_array


INPUT_BOOTSTRAP_SERVERS = "192.168.80.122:9092"
INPUT_TOPIC = "input"

OUTPUT_BOOTSTRAP_SERVERS = "192.168.80.122:9092"
OUTPUT_TOPIC = "output"

MODEL_PATH = "hdfs://192.168.80.52:9000/model"
CHECKPOINT_LOCATION = "hdfs://192.168.80.52:9000/checkpoints/fraud_prediction"


def create_spark_session() -> SparkSession:
    print("[PREDICT] Creating SparkSession...", file=sys.stdout)
    # Resource target for this job (Standalone):
    # - Total cores: 12
    # - 1 executor x 12 cores (forces allocation on the 12-core worker)
    spark = (
        SparkSession.builder.appName("FraudPredictionStreaming")
        .config("spark.cores.max", "12")
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "12")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("[PREDICT] SparkSession created.", file=sys.stdout)
    print("[PREDICT] Requested resources: cores.max=12, executor.instances=1, executor.cores=12", file=sys.stdout)
    return spark


def load_model():
    print(f"[PREDICT] Loading model from {MODEL_PATH} ...", file=sys.stdout)
    model = RandomForestClassificationModel.load(MODEL_PATH)
    print("[PREDICT] Model loaded.", file=sys.stdout)
    return model


def build_input_stream(spark: SparkSession):
    print(
        f"[PREDICT] Building input Kafka stream from {INPUT_BOOTSTRAP_SERVERS}, "
        f"topic='{INPUT_TOPIC}'...",
        file=sys.stdout,
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", INPUT_BOOTSTRAP_SERVERS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Schema phải khớp với JSON được gửi từ kafka_streaming.py
    schema = T.StructType(
        [
            T.StructField("Time", T.DoubleType(), True),
            T.StructField("V1", T.DoubleType(), True),
            T.StructField("V2", T.DoubleType(), True),
            T.StructField("V3", T.DoubleType(), True),
            T.StructField("V4", T.DoubleType(), True),
            T.StructField("V5", T.DoubleType(), True),
            T.StructField("V6", T.DoubleType(), True),
            T.StructField("V7", T.DoubleType(), True),
            T.StructField("V8", T.DoubleType(), True),
            T.StructField("V9", T.DoubleType(), True),
            T.StructField("V10", T.DoubleType(), True),
            T.StructField("V11", T.DoubleType(), True),
            T.StructField("V12", T.DoubleType(), True),
            T.StructField("V13", T.DoubleType(), True),
            T.StructField("V14", T.DoubleType(), True),
            T.StructField("V15", T.DoubleType(), True),
            T.StructField("V16", T.DoubleType(), True),
            T.StructField("V17", T.DoubleType(), True),
            T.StructField("V18", T.DoubleType(), True),
            T.StructField("V19", T.DoubleType(), True),
            T.StructField("V20", T.DoubleType(), True),
            T.StructField("V21", T.DoubleType(), True),
            T.StructField("V22", T.DoubleType(), True),
            T.StructField("V23", T.DoubleType(), True),
            T.StructField("V24", T.DoubleType(), True),
            T.StructField("V25", T.DoubleType(), True),
            T.StructField("V26", T.DoubleType(), True),
            T.StructField("V27", T.DoubleType(), True),
            T.StructField("V28", T.DoubleType(), True),
            T.StructField("Amount", T.DoubleType(), True),
            T.StructField("transaction_id", T.StringType(), True),
            T.StructField("timestamp", T.StringType(), True),
        ]
    )

    value_str = F.col("value").cast("string")
    parsed = raw_stream.select(F.from_json(value_str, schema).alias("data"))

    stream_df = parsed.select("data.*")

    print("[PREDICT] Input Kafka stream built and JSON parsed.", file=sys.stdout)
    return stream_df


def build_prediction_stream(stream_df, model):
    # Chuẩn bị features giống train_model.py
    feature_cols = [
        "Time",
        "V1",
        "V2",
        "V3",
        "V4",
        "V5",
        "V6",
        "V7",
        "V8",
        "V9",
        "V10",
        "V11",
        "V12",
        "V13",
        "V14",
        "V15",
        "V16",
        "V17",
        "V18",
        "V19",
        "V20",
        "V21",
        "V22",
        "V23",
        "V24",
        "V25",
        "V26",
        "V27",
        "V28",
        "Amount",
    ]

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
    )

    assembled = assembler.transform(stream_df)

    # Chạy model để lấy prediction và probability
    predictions = model.transform(assembled)

    # probability: vector [P(class=0), P(class=1)] → lấy phần tử 1 làm fraud_score
    prob_array = vector_to_array("probability")
    predictions = predictions.withColumn("fraud_score", prob_array[1])

    # Chỉ giữ lại các cột cần gửi ra ngoài
    result = predictions.select(
        "transaction_id",
        "timestamp",
        F.col("prediction").cast("int").alias("fraud_prediction"),
        F.col("fraud_score"),
    )

    return result


def build_kafka_output_stream(prediction_stream):
    print(
        f"[PREDICT] Building Kafka sink to {OUTPUT_BOOTSTRAP_SERVERS}, "
        f"topic='{OUTPUT_TOPIC}'...",
        file=sys.stdout,
    )

    # Đóng gói lại thành JSON string trong cột "value"
    output_json = prediction_stream.select(
        F.to_json(
            F.struct(
                "transaction_id",
                "timestamp",
                "fraud_prediction",
                "fraud_score",
            )
        ).alias("value")
    )

    query = (
        output_json.writeStream.outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", OUTPUT_BOOTSTRAP_SERVERS)
        .option("topic", OUTPUT_TOPIC)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    print("[PREDICT] Kafka sink started. Awaiting termination...", file=sys.stdout)
    return query


def main():
    spark = create_spark_session()
    model = load_model()

    stream_df = build_input_stream(spark)
    prediction_stream = build_prediction_stream(stream_df, model)
    query = build_kafka_output_stream(prediction_stream)

    query.awaitTermination()


if __name__ == "__main__":
    main()


