# Big Data Fraud Detection Pipeline

Hệ thống pipeline Big Data hoàn chỉnh sử dụng Apache Airflow để điều phối, Apache Spark ML để huấn luyện và dự đoán mô hình phát hiện gian lận, Apache Kafka để streaming dữ liệu, và Apache Hadoop HDFS để lưu trữ dữ liệu phân tán.

## 📋 Mục lục

- [Tổng quan](#tổng-quan)
- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Cài đặt](#cài-đặt)
- [Cấu hình](#cấu-hình)
- [Sử dụng](#sử-dụng)
- [DAGs](#dags)
- [Lưu ý](#lưu-ý)
- [Troubleshooting](#troubleshooting)
- [Tác giả](#tác-giả)

---

## 🎯 Tổng quan

Hệ thống pipeline Big Data để phát hiện gian lận giao dịch sử dụng:

- **Apache Airflow**: Điều phối workflow
- **Apache Spark ML**: Huấn luyện và dự đoán mô hình Random Forest
- **Apache Kafka**: Streaming dữ liệu real-time
- **Apache Hadoop HDFS**: Lưu trữ dữ liệu phân tán
- **Celery**: Phân phối tasks trên nhiều máy

### Chức năng chính

1. Huấn luyện mô hình Random Forest từ dữ liệu trên HDFS
2. Dự đoán real-time các giao dịch từ Kafka stream
3. Streaming dữ liệu từ HDFS vào Kafka
4. Điều phối toàn bộ pipeline bằng Airflow

### Công nghệ sử dụng

| Component | Version | Mục đích |
|-----------|---------|----------|
| Apache Airflow | 3.1.1 | Workflow orchestration và scheduling |
| Apache Spark | 4.0.1 | Distributed data processing và ML |
| Apache Kafka | Latest | Real-time data streaming |
| Apache Hadoop | Latest | Distributed storage (HDFS) |
| Celery | Latest | Distributed task execution |
| Redis | 7.2 | Message broker cho Celery |
| PostgreSQL | 16 | Metadata database cho Airflow |
| Python | 3.10+ | Programming language |

---

## 🏗️ Kiến trúc hệ thống

### Sơ đồ tổng quan

```
┌─────────────────────────────────────────────────────────────┐
│                    Airflow Orchestrator                      │
│                  (192.168.80.98:9090)                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Scheduler   │  │ DAG Processor│  │  Triggerer   │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         └─────────────────┴──────────────────┘              │
│                          │                                    │
│         ┌────────────────┴────────────────┐                 │
│         │      Celery Executor            │                 │
│         │   (Redis Broker + Workers)      │                 │
│         └────────────────┬────────────────┘                 │
└──────────────────────────┼──────────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌───────▼───────┐  ┌───────▼───────┐  ┌───────▼───────┐
│ Hadoop & Spark│  │     Kafka      │  │ Spark Worker  │
│    Master     │  │                │  │                │
│ 192.168.80.52 │  │ 192.168.80.122 │  │ 192.168.80.130 │
│               │  │                │  │                │
│ • HDFS        │  │ • Input Topic  │  │ • Prediction   │
│ • Spark       │  │ • Output Topic │  │   Processing   │
│   Master      │  │                │  │                │
└───────────────┘  └────────────────┘  └────────────────┘
```

### Phân bố máy và vai trò

| IP Address | Vai trò | Services | Celery Queue |
|------------|---------|----------|--------------|
| 192.168.80.98 | Airflow Server | Airflow, PostgreSQL, Redis | default |
| 192.168.80.52 | Hadoop & Spark Master | Hadoop HDFS, Spark Master | node_52 |
| 192.168.80.122 | Kafka & Spark Worker | Kafka Broker, Spark Worker | node_122 |
| 192.168.80.130 | Spark Worker | Spark Worker | node_130 |

### Luồng dữ liệu

```
1. Training Phase:
   HDFS (train.csv) → Spark ML → Model → HDFS (model/)

2. Prediction Phase:
   HDFS (stream.csv) → Kafka (input) → Spark Prediction → Kafka (output)

3. Real-time Processing:
   Kafka (input) → Spark Streaming → Load Model → Predict → Kafka (output)
```

### Dataset và Model

- **Dataset**: Credit Card Fraud Detection (30 features: Time, V1-V28, Amount)
- **Model**: Random Forest (300 trees, maxDepth=15)
- **Data Location**: `hdfs://192.168.80.52:9000/data/train.csv`
- **Model Location**: `hdfs://192.168.80.52:9000/model`

---

## 💻 Yêu cầu hệ thống

### Yêu cầu phần cứng

- **Tối thiểu**: 4GB RAM, 2 CPU cores, 20GB disk space
- **Khuyến nghị**: 8GB+ RAM, 4+ CPU cores, 50GB+ disk space

### Yêu cầu phần mềm

- **Docker & Docker Compose** (trên máy Airflow)
- **Python 3.10+** với `uv` hoặc `pip` (trên tất cả các máy)
- **SSH access** giữa các máy (passwordless SSH khuyến nghị)
- **Java 17+** (cho Spark và Hadoop)
- **Network connectivity** giữa tất cả các máy

### Dependencies

```bash
# Python packages cần thiết
celery>=5.3.0
redis>=5.0.0
psycopg2-binary>=2.9.0
pyspark>=3.5.0
```

---

## 🚀 Cài đặt

### Bước 1: Clone repository

```bash
# Thay thế <repository-url> bằng URL thực tế của repository
git clone <repository-url>
cd tai_thuy
```

### Bước 2: Cài đặt trên máy Airflow (192.168.80.98)

```bash
cd airflow-docker

# Tạo file .env
echo "AIRFLOW_UID=$(id -u)" > .env

# Khởi tạo Airflow database
docker compose up airflow-init

# Khởi động tất cả services
docker compose up -d

# Kiểm tra trạng thái
docker compose ps
```

### Bước 3: Cài đặt Celery Workers trên các máy khác

#### Trên máy Hadoop & Spark Master (192.168.80.52):

```bash
cd ~/tai_thuy/airflow-docker

# Cài đặt dependencies
uv pip install celery redis psycopg2-binary

# Khởi động Celery worker
nohup uv run celery -A mycelery.system_worker.app worker \
    --loglevel=INFO -E -Q node_52 \
    > celery_node_52.log 2>&1 &

# Kiểm tra
pgrep -fl "celery.*worker.*node_52"
```

#### Trên máy Kafka (192.168.80.122):

```bash
cd ~/tai_thuy/airflow-docker

# Cài đặt dependencies
uv pip install celery redis psycopg2-binary

# Khởi động Celery worker
nohup uv run celery -A mycelery.system_worker.app worker \
    --loglevel=INFO -E -Q node_122 \
    > celery_node_122.log 2>&1 &

# Kiểm tra
pgrep -fl "celery.*worker.*node_122"
```

#### Trên máy Spark Worker (192.168.80.130):

```bash
cd ~/tai_thuy/airflow-docker

# Cài đặt dependencies
uv pip install celery redis psycopg2-binary

# Khởi động Celery worker
nohup uv run celery -A mycelery.system_worker.app worker \
    --loglevel=INFO -E -Q node_130 \
    > celery_node_130.log 2>&1 &

# Kiểm tra
pgrep -fl "celery.*worker.*node_130"
```

### Bước 4: Chuẩn bị dữ liệu trên HDFS

```bash
# SSH vào máy Hadoop (192.168.80.52)
ssh labsit@192.168.80.52

# Tạo thư mục trên HDFS
hdfs dfs -mkdir -p /data
hdfs dfs -mkdir -p /model
hdfs dfs -mkdir -p /checkpoints

# Upload dữ liệu training và streaming
hdfs dfs -put ~/tai_thuy/train_model/train.csv /data/train.csv
hdfs dfs -put ~/tai_thuy/streaming/stream.csv /data/stream.csv

# Kiểm tra
hdfs dfs -ls /data
```

### Bước 5: Kiểm tra kết nối

```bash
# Từ máy Airflow, kiểm tra Celery workers
cd ~/tai_thuy/airflow-docker
docker compose exec airflow-scheduler airflow celery list-workers

# Kết quả mong đợi:
# worker_name          | queues  
# =====================+=========
# celery@<hostname>    | default 
# celery@<hostname>    | node_52 
# celery@<hostname>    | node_122
# celery@<hostname>    | node_130
```

---

## ⚙️ Cấu hình

### Cấu hình Airflow

File `airflow-docker/docker-compose.yaml` đã được cấu hình với:

- **Executor**: CeleryExecutor
- **Broker**: Redis tại `192.168.80.98:6379`
- **Database**: PostgreSQL tại `192.168.80.98:5432`
- **Web UI**: Port `9090`

### Cấu hình Celery Workers

File `airflow-docker/mycelery/system_worker.py` chứa:

- **Broker URL**: `redis://192.168.80.98:6379/0`
- **Backend**: `db+postgresql://airflow:airflow@192.168.80.98/airflow`
- **Queues**: `node_52`, `node_122`, `node_130`

### Cấu hình Pipeline

File `airflow-docker/dags/bigdata_full_pipeline_dag.py` chứa cấu hình:

```python
FULL_PIPELINE_CONFIG = {
    'hadoop_host': '192.168.80.52',
    'spark_master_url': 'spark://192.168.80.52:7077',
    'kafka_bootstrap': '192.168.80.122:9092',
    'train_input': 'hdfs://192.168.80.52:9000/data/train.csv',
    'model_path': 'hdfs://192.168.80.52:9000/model',
    # ... các cấu hình khác
}
```

---

## 📖 Sử dụng

### Truy cập Airflow Web UI

1. Mở trình duyệt và truy cập: `http://192.168.80.98:9090`
2. Đăng nhập với:
   - Username: `airflow`
   - Password: `airflow`

### Chạy Full Pipeline

1. Trong Airflow UI, tìm DAG `bigdata_full_pipeline`
2. Click vào DAG và chọn **"Trigger DAG"**
3. Pipeline sẽ tự động:
   - Khởi động infrastructure (Hadoop, Spark, Kafka)
   - Kiểm tra services sẵn sàng
   - Huấn luyện mô hình từ HDFS
   - Tạo Kafka topics (input, output)
   - Khởi động Spark prediction job
   - Chờ 60 giây
   - Khởi động streaming job từ HDFS vào Kafka

### Parameters

DAG hỗ trợ các parameters:

- `start_infrastructure` (boolean): Khởi động infrastructure hay không
- `train_model` (boolean): Huấn luyện mô hình hay không
- `start_predict` (boolean): Khởi động prediction job hay không
- `start_streaming` (boolean): Khởi động streaming job hay không
- `delay_before_streaming` (integer): Delay trước khi streaming (mặc định: 60 giây)

### Xem logs

```bash
# Logs Airflow Scheduler
cd ~/tai_thuy/airflow-docker
docker compose logs -f airflow-scheduler

# Logs Celery Workers trên các máy khác
tail -f ~/tai_thuy/airflow-docker/celery_node_52.log
tail -f ~/tai_thuy/airflow-docker/celery_node_122.log
tail -f ~/tai_thuy/airflow-docker/celery_node_130.log

# Logs Spark jobs
tail -f /tmp/spark_predict.log
tail -f /tmp/spark_kafka_streaming.log
```

---

## 🏛️ Kiến trúc

### Celery Queues

Dự án sử dụng **IP-based Celery queues** để phân phối tasks đến đúng máy:

- `node_52`: Hadoop & Spark Master (192.168.80.52)
- `node_122`: Kafka & Spark Worker (192.168.80.122)
- `node_130`: Spark Worker (192.168.80.130)

Mỗi máy cần chạy Celery worker với queue tương ứng để nhận tasks từ Airflow.

---

## 📁 DAGs

### DAG chính

#### `bigdata_full_pipeline`

DAG chính thực hiện toàn bộ pipeline từ đầu đến cuối:

**Phases:**
1. **Infrastructure Setup**: Khởi động Hadoop, Spark Master, Spark Workers, Kafka
2. **Service Verification**: Kiểm tra tất cả services đã sẵn sàng
3. **Model Training**: Huấn luyện Random Forest model từ HDFS
4. **Model Verification**: Xác minh model đã được lưu
5. **Kafka Topics Setup**: Tạo topics input và output nếu chưa có
6. **Prediction Job**: Khởi động Spark streaming prediction job
7. **Wait Period**: Chờ 60 giây để prediction job sẵn sàng
8. **Streaming Job**: Khởi động streaming từ HDFS vào Kafka

**Dependencies:**
```
start_hadoop
  ↓
start_spark_master → [start_spark_worker_1, start_spark_worker_2]
  ↓
start_kafka
  ↓
[check_hadoop_ready, check_spark_ready, check_kafka_ready]
  ↓
train_model → verify_model_saved
  ↓
check_kafka_topics
  ↓
start_predict
  ↓
wait_before_streaming
  ↓
start_streaming
```

### DAGs test

Các DAG test để kiểm tra từng component riêng lẻ:

- `test_hadoop_dag`: Test Hadoop start/stop
- `test_spark_dag`: Test Spark cluster
- `test_kafka_dag`: Test Kafka start/stop
- `test_train_model_dag`: Test model training
- `test_spark_predict_dag`: Test prediction job
- `test_kafka_streaming_dag`: Test streaming từ HDFS vào Kafka
- `test_create_kafka_topics_dag`: Test tạo Kafka topics

---

## ⚠️ Lưu ý

- Cần khởi động Celery workers trên tất cả các máy trước khi chạy DAG
- Đảm bảo network connectivity giữa các máy
- Dữ liệu phải được upload lên HDFS trước khi training
- Kafka topics sẽ được tạo tự động khi start Kafka

---

## 🔧 Troubleshooting

### Vấn đề: Airflow không nhận được tasks từ Celery workers

**Nguyên nhân**: Celery workers trên các máy khác chưa được khởi động hoặc không kết nối được Redis.

**Giải pháp**:
```bash
# Kiểm tra Celery workers
docker compose exec airflow-scheduler airflow celery list-workers

# Nếu thiếu worker, khởi động lại trên máy tương ứng
# Ví dụ trên máy 192.168.80.122:
ssh labsit@192.168.80.122
cd ~/tai_thuy/airflow-docker
nohup uv run celery -A mycelery.system_worker.app worker \
    --loglevel=INFO -E -Q node_122 \
    > celery_node_122.log 2>&1 &
```

### Vấn đề: DAG không xuất hiện trong Airflow UI

**Nguyên nhân**: Lỗi syntax hoặc import trong DAG file.

**Giải pháp**:
```bash
# Kiểm tra lỗi import
docker compose exec airflow-scheduler airflow dags list-import-errors

# Kiểm tra syntax Python
docker compose exec airflow-scheduler python3 -m py_compile dags/bigdata_full_pipeline_dag.py
```

### Vấn đề: Spark job không chạy

**Nguyên nhân**: Spark Master không sẵn sàng hoặc không đủ resources.

**Giải pháp**:
```bash
# Kiểm tra Spark Master UI: http://192.168.80.52:8080
# Kiểm tra workers đã kết nối chưa
# Kiểm tra logs Spark job
tail -f /tmp/spark_predict.log
```

### Vấn đề: Kafka topics không được tạo

**Nguyên nhân**: Kafka chưa sẵn sàng hoặc lỗi kết nối.

**Giải pháp**:
```bash
# Kiểm tra Kafka đang chạy
ssh labsit@192.168.80.122
docker ps | grep kafka

# Tạo topics thủ công nếu cần
docker exec -i kafka kafka-topics \
    --create \
    --topic input \
    --bootstrap-server 192.168.80.122:9092 \
    --partitions 1 \
    --replication-factor 1
```

### Vấn đề: HDFS không accessible

**Nguyên nhân**: Hadoop chưa khởi động hoặc lỗi cấu hình.

**Giải pháp**:
```bash
# Kiểm tra Hadoop services
ssh labsit@192.168.80.52
jps | grep -E "NameNode|DataNode"

# Kiểm tra HDFS
hdfs dfsadmin -report
hdfs dfs -ls /
```

### Kiểm tra kết nối Redis

```bash
# Từ máy Airflow
docker compose exec redis redis-cli PING

# Từ các máy khác
redis-cli -h 192.168.80.98 -p 6379 PING
```

### Xem logs chi tiết

```bash
# Airflow logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker

# Celery worker logs trên các máy
tail -f ~/tai_thuy/airflow-docker/celery_node_*.log

# Spark logs
tail -f /tmp/spark_*.log
```

---

## 📊 Monitoring

### Airflow Web UI

- **URL**: http://192.168.80.98:9090
- **Features**: Xem DAGs, task status, logs, graphs

### Spark Master UI

- **URL**: http://192.168.80.52:8080
- **Features**: Xem Spark cluster status, applications, workers

### Kafka Topics

```bash
# List topics
docker exec -i kafka kafka-topics \
    --list \
    --bootstrap-server 192.168.80.122:9092

# Xem messages trong topic
docker exec -i kafka kafka-console-consumer \
    --bootstrap-server 192.168.80.122:9092 \
    --topic output \
    --from-beginning
```

### HDFS Status

```bash
# Xem cluster status
hdfs dfsadmin -report

# Xem disk usage
hdfs dfs -du -h /
```

---

## 📝 Cấu trúc thư mục

```
tai_thuy/
├── airflow-docker/              # Airflow và Celery configuration
│   ├── dags/                    # DAG definitions
│   │   ├── bigdata_full_pipeline_dag.py
│   │   ├── test_*.py
│   ├── mycelery/                # Celery tasks
│   │   └── system_worker.py
│   ├── config/                  # Airflow config
│   ├── logs/                    # Airflow logs
│   ├── docker-compose.yaml      # Docker services
│   └── README.md
├── train_model/                 # Model training scripts
│   ├── train_model.py
│   └── requirements.txt
├── predict/                     # Prediction scripts
│   └── predict_fraud.py
├── streaming/                   # Streaming scripts
│   ├── kafka_streaming.py
│   └── stream.csv
└── README.md                    # This file
```

---

## 🛠️ Development

### Thêm task mới vào Celery

1. Thêm function vào `airflow-docker/mycelery/system_worker.py`:

```python
@app.task(bind=True)
def my_new_task(self, param1, param2):
    """Mô tả task"""
    # Implementation
    return {'status': 'success', 'result': ...}
```

2. Import trong DAG:

```python
from mycelery.system_worker import my_new_task
```

3. Sử dụng trong DAG:

```python
result = my_new_task.apply_async(
    args=[param1, param2],
    queue='node_52'  # Chọn queue phù hợp
)
```

### Tạo DAG mới

1. Tạo file mới trong `airflow-docker/dags/`:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(
    dag_id='my_new_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
) as dag:
    # Define tasks
    pass
```

2. Airflow sẽ tự động phát hiện và load DAG mới.

---

## 📚 Tài liệu tham khảo

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/)
- [Celery Documentation](https://docs.celeryq.dev/)

---

## 👥 Tác giả

**Nhóm 2 - Khoa Công nghệ Thông tin**

- **Phan Văn Tài** (2202081)
- **Phan Minh Thuy** (2202079)

**Giảng viên hướng dẫn**: Dr. Cao Tien Dung

**Trường**: Đại học Tân Tạo  
**Khoa**: Công nghệ Thông tin

---

## 📄 License

Apache License 2.0

---

## Acknowledgments

- Apache Software Foundation cho các công cụ mã nguồn mở
- Cộng đồng open source
- Dr. Cao Tien Dung cho sự hướng dẫn và hỗ trợ

---

**Lưu ý**: Đây là dự án học tập. Để sử dụng trong production, cần thêm các biện pháp bảo mật, monitoring, và backup phù hợp.

