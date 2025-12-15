# Big Data Pipeline - Final Project

Dự án xây dựng hệ thống xử lý dữ liệu lớn (Big Data Pipeline) sử dụng Apache Airflow để orchestration workflow và tích hợp với các công nghệ phân tán như Celery, Redis, Spark và Hadoop.

## Tổng quan kiến trúc

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BIG DATA PIPELINE                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────────┐         ┌─────────────────────┐           │
│  │   airflow-docker/   │         │     services/       │           │
│  │                     │         │                     │           │
│  │  - Airflow UI       │◄───────►│  - Task Producer    │           │
│  │  - Scheduler        │         │  - Redis Queue      │           │
│  │  - Celery Workers   │         │  - RQ Workers       │           │
│  │  - DAG Processor    │         │                     │           │
│  └─────────────────────┘         └─────────────────────┘           │
│           │                               │                        │
│           ▼                               ▼                        │
│  ┌─────────────────────────────────────────────────────────┐       │
│  │                    INFRASTRUCTURE                        │       │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────┐ │       │
│  │  │ Redis   │  │PostgreSQL│  │  Spark  │  │   Hadoop    │ │       │
│  │  │ :6379   │  │  :5432  │  │ Master  │  │    HDFS     │ │       │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────────┘ │       │
│  └─────────────────────────────────────────────────────────┘       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Cấu trúc dự án

```
airflow/
├── airflow-docker/          # Apache Airflow orchestration platform
│   ├── dags/               # DAG definitions (workflow pipelines)
│   ├── logs/               # Airflow execution logs
│   ├── plugins/            # Custom Airflow plugins
│   ├── config/             # Airflow configuration files
│   ├── celery/             # Celery worker configuration
│   ├── docker-compose.yaml # Docker services definition
│   └── requirements.txt    # Python dependencies
│
├── services/               # External services & task producers
│   ├── main.py            # Main entry point
│   ├── master.py          # Redis Queue task producer
│   └── pyproject.toml     # Project configuration
│
└── README.md              # This file
```

## Tech Stack

| Component | Version | Mục đích |
|-----------|---------|----------|
| Apache Airflow | 3.1.1 | Workflow orchestration & scheduling |
| Celery | - | Distributed task execution |
| Redis | 7.2 | Message broker & task queue |
| PostgreSQL | 16 | Metadata database |
| Apache Spark | - | Distributed data processing |
| Hadoop HDFS | - | Distributed storage system |
| Docker | - | Containerization |
| Python | >=3.10 | Programming language |

## Yêu cầu hệ thống

- **Docker & Docker Compose** (bắt buộc)
- **RAM**: Tối thiểu 4GB
- **CPU**: Tối thiểu 2 cores
- **Disk**: Tối thiểu 10GB trống
- **Python**: 3.10+ (cho services/)

## Hướng dẫn cài đặt

### 1. Clone repository

```bash
git clone <repository-url>
cd airflow
```

### 2. Khởi động Airflow Platform

```bash
cd airflow-docker

# Thiết lập AIRFLOW_UID (Linux users)
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Khởi tạo Airflow database
docker-compose up airflow-init

# Khởi động tất cả services
docker-compose up -d

# (Tùy chọn) Khởi động với Flower monitoring
docker-compose --profile flower up -d
```

### 3. Cài đặt Services module

```bash
cd ../services

# Sử dụng pip
pip install -r requirements.txt

# Hoặc sử dụng uv/poetry
uv sync
```

## Truy cập giao diện

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow Web UI | http://localhost:9090 | airflow / airflow |
| Flower (Celery Monitor) | http://localhost:5555 | - |
| PostgreSQL | localhost:5432 | airflow / airflow |
| Redis | localhost:6379 | - |

## Mô tả các thành phần

### 1. airflow-docker/

Platform chính để orchestration các data pipeline:

- **Airflow Scheduler**: Lên lịch và trigger các DAG runs
- **Airflow Workers**: Celery workers thực thi các tasks
- **DAG Processor**: Xử lý và parse các DAG files
- **API Server**: REST API và Web UI
- **Triggerer**: Xử lý các deferred tasks

**DAGs mẫu:**
- `hello_world.py`: DAG demo cơ bản với PythonOperator và BashOperator
- `test.py`: DAG demo tích hợp Celery tasks

### 2. services/

Module xử lý các external services và task producers:

- **master.py**: Task producer sử dụng Redis Queue (RQ)
  - Kết nối tới Redis server
  - Đẩy jobs vào queue để workers xử lý

- **main.py**: Entry point chính của services module

## Các lệnh thường dùng

### Airflow Docker Commands

```bash
# Khởi động services
docker-compose up -d

# Dừng services
docker-compose down

# Xem logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker

# Scale workers
docker-compose up -d --scale airflow-worker=3

# Truy cập Airflow CLI
docker-compose run airflow-cli airflow dags list

# Xóa hoàn toàn (kể cả volumes)
docker-compose down -v
```

### Services Commands

```bash
cd services

# Chạy master (task producer)
python master.py

# Chạy main service
python main.py
```

## Tạo DAG mới

Tạo file Python mới trong `airflow-docker/dags/`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def my_task():
    print("Processing data...")
    # Xử lý dữ liệu tại đây
    return "Done"

with DAG(
    dag_id='my_data_pipeline',
    description='My custom data pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['bigdata'],
) as dag:
    task = PythonOperator(
        task_id='process_data',
        python_callable=my_task,
    )
```

## Tích hợp Spark

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_task = SparkSubmitOperator(
    task_id='spark_etl_job',
    application='/opt/airflow/dags/spark_jobs/etl_job.py',
    conn_id='spark_default',
    verbose=True,
)
```

## Tích hợp Hadoop/HDFS

```python
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

hdfs_hook = WebHDFSHook(webhdfs_conn_id='webhdfs_default')
# Đọc/ghi dữ liệu từ HDFS
```

## Monitoring & Health Checks

```bash
# Kiểm tra Scheduler health
curl http://localhost:8974/health

# Kiểm tra API Server
curl http://localhost:9090/api/v2/version

# Kiểm tra Redis
docker-compose exec redis redis-cli ping
```

## Troubleshooting

### Lỗi thiếu memory
```bash
# Tăng Docker memory allocation lên ít nhất 4GB trong Docker Desktop settings
```

### Lỗi permission (Linux)
```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker-compose down && docker-compose up -d
```

### DAG không hiển thị
```bash
# Kiểm tra logs của DAG processor
docker-compose logs airflow-dag-processor

# Kiểm tra syntax DAG
docker-compose run airflow-cli airflow dags list
```

### Worker không nhận tasks
```bash
# Kiểm tra logs worker
docker-compose logs airflow-worker

# Kiểm tra kết nối Redis
docker-compose exec redis redis-cli ping
```

## Production Considerations

- Sử dụng secrets management cho credentials
- Cấu hình external PostgreSQL và Redis
- Thiết lập logging và monitoring đầy đủ
- Build custom Docker image với dependencies pre-installed
- Cấu hình resource limits cho containers
- Enable SSL/TLS cho tất cả connections
- Thiết lập backup strategies

## Thành viên nhóm

| STT | Họ tên | MSSV | Vai trò |
|-----|--------|------|---------|
| 1 | | | |
| 2 | | | |
| 3 | | | |

## License

Apache License 2.0
# tai_thuy
# tai_thuy
