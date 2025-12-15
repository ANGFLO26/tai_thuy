from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    docker_compose_up,
    run_command
)


def wait_for_celery_result(result, timeout=60, poll_interval=2):
    """Chờ kết quả từ Celery task"""
    elapsed = 0
    while elapsed < timeout:
        if result.ready():
            if result.successful():
                return result.result
            else:
                raise Exception(f"Celery task failed: {result.result}")
        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"Celery task {result.id} timed out after {timeout} seconds")


# Cấu hình Kafka
KAFKA_CONFIG = {
    'host': '192.168.80.122',
    'queue': 'node_122',
    'compose_path': '~/kafka-docker/docker-compose.yml',
    'bootstrap_server': '192.168.80.122:9092',
}


# ============== DAG: Test Kafka Start ==============
with DAG(
    dag_id='test_kafka_start',
    description='DAG test khởi động Kafka cluster (Kafka + Zookeeper)',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'kafka', 'start'],
) as dag_test_kafka_start:

    def start_kafka(**context):
        """Khởi động Kafka và Zookeeper"""
        print(f"🚀 Starting Kafka on {KAFKA_CONFIG['host']}...")
        
        result = docker_compose_up.apply_async(
            args=[KAFKA_CONFIG['compose_path']],
            kwargs={
                'services': ['kafka', 'zookeeper'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=KAFKA_CONFIG['queue']
        )

        # Chờ kết quả từ Celery worker
        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Kafka started successfully on {KAFKA_CONFIG['host']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'host': KAFKA_CONFIG['host'],
                'queue': KAFKA_CONFIG['queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Kafka on {KAFKA_CONFIG['host']}: {str(e)}")
            raise Exception(f"Failed to start Kafka on {KAFKA_CONFIG['host']}: {str(e)}")

    def create_kafka_topic(**context):
        """Tạo Kafka topic"""
        topic_name = context['params'].get('topic_name', '')
        if not topic_name:
            raise Exception("Topic name is required")
        
        print(f"📝 Creating Kafka topic '{topic_name}' on {KAFKA_CONFIG['host']}...")
        
        command = (
            f"docker exec -i kafka kafka-topics "
            f"--create "
            f"--topic {topic_name} "
            f"--bootstrap-server {KAFKA_CONFIG['bootstrap_server']} "
            f"--partitions 1 "
            f"--replication-factor 1"
        )
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=KAFKA_CONFIG['queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ Topic '{topic_name}' created successfully")
            print(f"Stdout: {output.get('stdout', '')}")
            if output.get('stderr'):
                print(f"Stderr: {output.get('stderr', '')}")
            
            # Kiểm tra nếu topic đã tồn tại (return code có thể là 0 hoặc khác 0)
            if output.get('return_code') != 0 and 'already exists' not in output.get('stderr', '').lower():
                raise Exception(f"Failed to create topic: {output.get('stderr', '')}")
            
            return {
                'task_id': result.id,
                'topic': topic_name,
                'host': KAFKA_CONFIG['host'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to create topic '{topic_name}': {str(e)}")
            raise Exception(f"Failed to create topic '{topic_name}': {str(e)}")

    def create_topic_input(**context):
        """Tạo topic input"""
        context['params'] = {'topic_name': 'input'}
        return create_kafka_topic(**context)

    def create_topic_output(**context):
        """Tạo topic output"""
        context['params'] = {'topic_name': 'output'}
        return create_kafka_topic(**context)

    def check_kafka_status(**context):
        """Kiểm tra trạng thái Kafka containers"""
        print("🔍 Checking Kafka cluster status...")
        
        command = "docker ps --filter 'name=kafka' --filter 'name=zookeeper' --format 'table {{.Names}}\t{{.Status}}'"
        
        try:
            result = run_command.apply_async(
                args=[command],
                kwargs={},
                queue=KAFKA_CONFIG['queue']
            )
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ Kafka status checked on {KAFKA_CONFIG['host']}")
            print(f"Containers: {output.get('stdout', '')}")
            return {
                'host': KAFKA_CONFIG['host'],
                'status': 'success',
                'containers': output.get('stdout', ''),
                'stderr': output.get('stderr', '')
            }
        except Exception as e:
            print(f"❌ Failed to check Kafka status on {KAFKA_CONFIG['host']}: {str(e)}")
            raise Exception(f"Failed to check Kafka status: {str(e)}")

    # Tạo tasks
    task_start_kafka = PythonOperator(
        task_id='start_kafka',
        python_callable=start_kafka,
    )

    task_create_topic_input = PythonOperator(
        task_id='create_topic_input',
        python_callable=create_topic_input,
    )

    task_create_topic_output = PythonOperator(
        task_id='create_topic_output',
        python_callable=create_topic_output,
    )

    task_check_status = PythonOperator(
        task_id='check_kafka_status',
        python_callable=check_kafka_status,
    )

    # Dependencies: Start Kafka -> Create Topics (parallel) -> Check Status
    task_start_kafka >> [task_create_topic_input, task_create_topic_output] >> task_check_status


# ============== DAG: Test Kafka Stop ==============
with DAG(
    dag_id='test_kafka_stop',
    description='DAG test dừng Kafka cluster',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'kafka', 'stop'],
) as dag_test_kafka_stop:

    def stop_kafka(**context):
        """Dừng Kafka và Zookeeper"""
        print(f"🛑 Stopping Kafka on {KAFKA_CONFIG['host']}...")
        
        command = f"cd ~/kafka-docker && docker compose stop kafka zookeeper"
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=KAFKA_CONFIG['queue']
        )

        # Chờ kết quả từ Celery worker
        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Kafka stopped successfully on {KAFKA_CONFIG['host']}")
            print(f"Stdout: {output.get('stdout', '')}")
            if output.get('stderr'):
                print(f"Stderr: {output.get('stderr', '')}")
            
            if output.get('return_code') != 0:
                raise Exception(f"Kafka stop command returned non-zero code: {output.get('return_code')}")
            
            return {
                'task_id': result.id,
                'host': KAFKA_CONFIG['host'],
                'command': command,
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to stop Kafka on {KAFKA_CONFIG['host']}: {str(e)}")
            raise Exception(f"Failed to stop Kafka on {KAFKA_CONFIG['host']}: {str(e)}")

    # Tạo task
    task_stop_kafka = PythonOperator(
        task_id='stop_kafka',
        python_callable=stop_kafka,
    )

