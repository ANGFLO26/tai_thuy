import json
import time
import sys

import pandas as pd
from kafka import KafkaProducer


def main():
    print("[STREAM] Loading local CSV file...", file=sys.stdout)
    csv_path = "/home/labsit-13/tai_thuy/streaming/stream.csv"
    df = pd.read_csv(csv_path)

    # Chỉ giữ lại các cột feature + thông tin giao dịch, bỏ cột Class (label)
    columns_to_send = [
        "Time",
        "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9",
        "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19",
        "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28",
        "Amount",
        "transaction_id",
        "timestamp",
    ]
    df = df[columns_to_send]

    total = len(df)
    print(f"[STREAM] Local CSV loaded and filtered (without 'Class'). Total records: {total}", file=sys.stdout)

    # Tạo Kafka producer
    print("[STREAM] Creating Kafka producer...", file=sys.stdout)
    producer = KafkaProducer(
        bootstrap_servers="192.168.80.122:9092",
        # Dùng default=str để convert datetime (và kiểu không JSON được) sang chuỗi
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    print("[STREAM] Kafka producer created.", file=sys.stdout)

    # Gửi từng bản ghi vào topic "input" cách nhau 5 giây
    print("[STREAM] Starting to send records to Kafka topic 'input'...", file=sys.stdout)
    for idx, row in enumerate(df.itertuples(index=False), start=1):
        record = row._asdict()
        producer.send("input", record)
        print(f"[STREAM] Sent record {idx}/{total}", file=sys.stdout)
        time.sleep(5)

    print("[STREAM] Flushing and closing Kafka producer...", file=sys.stdout)
    producer.flush()
    producer.close()

    print("[STREAM] Streaming job finished.", file=sys.stdout)


if __name__ == "__main__":
    main()