# Airflow Data Pipeline Platform

A production-ready Apache Airflow setup with Celery executor, Redis, PostgreSQL, Spark, and Hadoop integration for building scalable data pipelines.

## Architecture

```
                    +------------------+
                    |   Airflow UI     |
                    |   (Port 9090)    |
                    +--------+---------+
                             |
              +--------------+--------------+
              |              |              |
     +--------v----+  +------v------+  +----v--------+
     |  Scheduler  |  | DAG Processor|  |  Triggerer  |
     +-------------+  +-------------+  +-------------+
              |              |              |
              +--------------+--------------+
                             |
                    +--------v---------+
                    |  Celery Workers  |
                    +--------+---------+
                             |
         +-------------------+-------------------+
         |                   |                   |
+--------v-------+  +--------v-------+  +--------v-------+
|     Redis      |  |   PostgreSQL   |  |  Spark/Hadoop  |
|  (Message Queue)|  |   (Metadata)   |  | (Processing)   |
|   Port 6379    |  |   Port 5432    |  |                |
+----------------+  +----------------+  +----------------+
```

## Tech Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Apache Airflow | 3.1.1 | Workflow orchestration |
| Celery | - | Distributed task execution |
| Redis | 7.2 | Message broker for Celery |
| PostgreSQL | 16 | Metadata database |
| Apache Spark | - | Distributed data processing |
| Hadoop | - | Distributed storage (HDFS) |
| Flower | - | Celery monitoring (optional) |

## Prerequisites

- Docker & Docker Compose
- Minimum 4GB RAM
- Minimum 2 CPUs
- 10GB+ disk space

## Quick Start

### 1. Clone and Setup

```bash
cd airflow-docker

# Set Airflow UID (Linux users)
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### 2. Initialize Airflow

```bash
docker-compose up airflow-init
```

### 3. Start Services

```bash
# Start all services
docker-compose up -d

# Start with Flower monitoring
docker-compose --profile flower up -d
```

### 4. Access Web Interfaces

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:9090 | airflow / airflow |
| Flower | http://localhost:5555 | - |

## Project Structure

```
airflow-docker/
├── dags/                  # DAG definitions
├── logs/                  # Airflow logs
├── plugins/               # Custom plugins
├── config/                # Airflow configuration
│   └── airflow.cfg
├── celery/                # Celery configuration
├── docker-compose.yaml    # Docker services
├── .env                   # Environment variables
└── README.md
```

## Services

| Service | Description |
|---------|-------------|
| `postgres` | PostgreSQL database for Airflow metadata |
| `redis` | Redis message broker for Celery |
| `airflow-apiserver` | Airflow REST API and Web UI |
| `airflow-scheduler` | Schedules and triggers DAG runs |
| `airflow-dag-processor` | Processes DAG files |
| `airflow-worker` | Celery worker for task execution |
| `airflow-triggerer` | Handles deferred tasks |
| `flower` | Celery monitoring dashboard (optional) |

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```bash
# Airflow
AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.1
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Additional pip packages (for quick testing only)
_PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-apache-spark apache-airflow-providers-apache-hdfs

# Spark
SPARK_MASTER_URL=spark://spark-master:7077

# Hadoop
HADOOP_CONF_DIR=/etc/hadoop/conf
```

### Spark Integration

Add Spark provider to your DAGs:

```python
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_task = SparkSubmitOperator(
    task_id='spark_job',
    application='/opt/airflow/dags/spark_jobs/my_job.py',
    conn_id='spark_default',
    verbose=True,
)
```

### Hadoop/HDFS Integration

```python
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

hdfs_hook = WebHDFSHook(webhdfs_conn_id='webhdfs_default')
```

## Common Commands

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f airflow-scheduler

# Scale workers
docker-compose up -d --scale airflow-worker=3

# Access Airflow CLI
docker-compose run airflow-cli airflow dags list

# Restart a service
docker-compose restart airflow-worker

docker compose restart airflow-dag-processor

# Clean up (remove volumes)
docker-compose down -v
```

### Run Celery Worker (Manual)

Nếu muốn chạy Celery worker thủ công (ngoài Docker):

```bash
# Từ thư mục airflow-docker/
celery -A mycelery.worker.app worker --loglevel=INFO

# Hoặc từ thư mục mycelery/
cd mycelery
celery -A worker.app worker --loglevel=INFO
```

## Creating DAGs

Place your DAG files in the `dags/` directory:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def my_task():
    print("Hello from Airflow!")

with DAG(
    dag_id='my_first_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='my_task',
        python_callable=my_task,
    )
```

## Monitoring

### Flower Dashboard

Enable Flower for Celery monitoring:

```bash
docker-compose --profile flower up -d
```

Access at http://localhost:5555

### Health Checks

```bash
# Check scheduler health
curl http://localhost:8974/health

# Check API server
curl http://localhost:9090/api/v2/version
```

## Troubleshooting

### Common Issues

**Memory issues:**
```bash
# Increase Docker memory allocation to at least 4GB
```

**Permission issues (Linux):**
```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker-compose down && docker-compose up -d
```

**DAGs not appearing:**
```bash
# Check DAG processor logs
docker-compose logs airflow-dag-processor

# Verify DAG syntax
docker-compose run airflow-cli airflow dags list
```

**Worker not picking up tasks:**
```bash
# Check worker logs
docker-compose logs airflow-worker

# Verify Redis connection
docker-compose exec redis redis-cli ping
```

## Production Considerations

- Use secrets management for credentials
- Configure external PostgreSQL and Redis
- Set up proper logging and monitoring
- Use custom Docker image with pre-installed dependencies
- Configure resource limits for containers
- Enable SSL/TLS for all connections
- Set up proper backup strategies

## License

Apache License 2.0

Hướng dẫn setup trên mỗi node:

1. Trên máy 192.168.80.192 (Spark Master)

nohup uv run celery -A mycelery.system_worker.app worker --loglevel=INFO -E -Q node_55 > output.log 2>&1 &

2. Trên máy 192.168.80.53 (Spark Worker)

nohup uv run celery -A mycelery.system_worker.app worker --loglevel=INFO -E -Q node_53 > output.log 2>&1 &

3. Trên máy 192.168.80.57 (Hadoop Namenode + Kafka)

nohup uv run celery -A mycelery.system_worker.app worker --loglevel=INFO -E -Q node_57 > output.log 2>&1 &

4. Trên máy 192.168.80.87 (Hadoop Datanode)

nohup uv run celery -A mycelery.system_worker.app worker --loglevel=INFO -E -Q node_87 > output.log 2>&1 &

DAGs đã tạo:

| DAG                    | Mô tả                                                                                    |
|------------------------|------------------------------------------------------------------------------------------|
| bigdata_pipeline_start | Khởi động cluster theo thứ tự: Namenode → Datanode + Spark Master → Spark Worker + Kafka |
| bigdata_pipeline_stop  | Dừng cluster theo thứ tự ngược: Kafka → Spark → Hadoop                                   |

Pipeline Flow:

Start:
Hadoop Namenode (55)
    ├── Hadoop Datanode (87) → Kafka (57)
    └── Spark Master (55) → Spark Worker (53)

Stop:
Kafka (57) → Spark Worker (53) → Spark Master (55) → Hadoop Datanode (87) → Hadoop Namenode (57)