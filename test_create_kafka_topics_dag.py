from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    run_command,
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


# Cấu hình Kafka Topics
KAFKA_CONFIG = {
    'host': '192.168.80.122',
    'queue': 'node_122',
    'bootstrap_server': '192.168.80.122:9092',
    'topics': ['input', 'output'],
    'partitions': 1,
    'replication_factor': 1,
}


# ============== DAG: Create Kafka Topics ==============
with DAG(
    dag_id='test_create_kafka_topics',
    description='DAG tạo Kafka topics: input và output',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'kafka', 'topics'],
    params={
        'create_input': Param(True, type='boolean', description='Tạo topic input'),
        'create_output': Param(True, type='boolean', description='Tạo topic output'),
    }
) as dag_test_create_kafka_topics:

    def create_kafka_topic_input(**context):
        """Tạo Kafka topic 'input'"""
        if not context['params'].get('create_input', True):
            return {'skipped': True}
        
        topic_name = 'input'
        print(f"🚀 Creating Kafka topic '{topic_name}'...")
        print(f"Bootstrap server: {KAFKA_CONFIG['bootstrap_server']}")
        print(f"Partitions: {KAFKA_CONFIG['partitions']}")
        print(f"Replication factor: {KAFKA_CONFIG['replication_factor']}")
        
        command = (
            f"docker exec -i kafka kafka-topics "
            f"--create "
            f"--topic {topic_name} "
            f"--bootstrap-server {KAFKA_CONFIG['bootstrap_server']} "
            f"--partitions {KAFKA_CONFIG['partitions']} "
            f"--replication-factor {KAFKA_CONFIG['replication_factor']}"
        )
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=KAFKA_CONFIG['queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ Topic '{topic_name}' creation result:")
            print(f"Stdout: {output.get('stdout', '')}")
            if output.get('stderr'):
                print(f"Stderr: {output.get('stderr', '')}")
            
            # Kiểm tra nếu topic đã tồn tại (không phải lỗi)
            if output.get('return_code') != 0:
                stderr = output.get('stderr', '')
                if 'already exists' in stderr.lower():
                    print(f"ℹ️  Topic '{topic_name}' already exists (this is OK)")
                    return {
                        'task_id': result.id,
                        'status': 'success',
                        'topic': topic_name,
                        'bootstrap_server': KAFKA_CONFIG['bootstrap_server'],
                        'message': 'Topic already exists',
                        'output': output
                    }
                else:
                    raise Exception(f"Failed to create topic '{topic_name}': {stderr}")
            
            print(f"✅ Topic '{topic_name}' created successfully on {KAFKA_CONFIG['bootstrap_server']}")
            return {
                'task_id': result.id,
                'status': 'success',
                'topic': topic_name,
                'bootstrap_server': KAFKA_CONFIG['bootstrap_server'],
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to create topic '{topic_name}': {str(e)}")
            raise Exception(f"Failed to create topic '{topic_name}': {str(e)}")

    def create_kafka_topic_output(**context):
        """Tạo Kafka topic 'output'"""
        if not context['params'].get('create_output', True):
            return {'skipped': True}
        
        topic_name = 'output'
        print(f"🚀 Creating Kafka topic '{topic_name}'...")
        print(f"Bootstrap server: {KAFKA_CONFIG['bootstrap_server']}")
        print(f"Partitions: {KAFKA_CONFIG['partitions']}")
        print(f"Replication factor: {KAFKA_CONFIG['replication_factor']}")
        
        command = (
            f"docker exec -i kafka kafka-topics "
            f"--create "
            f"--topic {topic_name} "
            f"--bootstrap-server {KAFKA_CONFIG['bootstrap_server']} "
            f"--partitions {KAFKA_CONFIG['partitions']} "
            f"--replication-factor {KAFKA_CONFIG['replication_factor']}"
        )
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=KAFKA_CONFIG['queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ Topic '{topic_name}' creation result:")
            print(f"Stdout: {output.get('stdout', '')}")
            if output.get('stderr'):
                print(f"Stderr: {output.get('stderr', '')}")
            
            # Kiểm tra nếu topic đã tồn tại (không phải lỗi)
            if output.get('return_code') != 0:
                stderr = output.get('stderr', '')
                if 'already exists' in stderr.lower():
                    print(f"ℹ️  Topic '{topic_name}' already exists (this is OK)")
                    return {
                        'task_id': result.id,
                        'status': 'success',
                        'topic': topic_name,
                        'bootstrap_server': KAFKA_CONFIG['bootstrap_server'],
                        'message': 'Topic already exists',
                        'output': output
                    }
                else:
                    raise Exception(f"Failed to create topic '{topic_name}': {stderr}")
            
            print(f"✅ Topic '{topic_name}' created successfully on {KAFKA_CONFIG['bootstrap_server']}")
            return {
                'task_id': result.id,
                'status': 'success',
                'topic': topic_name,
                'bootstrap_server': KAFKA_CONFIG['bootstrap_server'],
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to create topic '{topic_name}': {str(e)}")
            raise Exception(f"Failed to create topic '{topic_name}': {str(e)}")

    def verify_topics(**context):
        """Verify cả 2 topics đã được tạo"""
        print(f"🔍 Verifying topics exist...")
        print(f"Bootstrap server: {KAFKA_CONFIG['bootstrap_server']}")
        
        command = f"docker exec -i kafka kafka-topics --list --bootstrap-server {KAFKA_CONFIG['bootstrap_server']}"
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=KAFKA_CONFIG['queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            topics_list = output.get('stdout', '')
            print(f"📋 Available topics:")
            print(topics_list)
            
            found_topics = []
            for topic in KAFKA_CONFIG['topics']:
                if topic in topics_list:
                    found_topics.append(topic)
                    print(f"✅ Topic '{topic}' found")
                else:
                    print(f"⚠️  Topic '{topic}' not found")
            
            if len(found_topics) == len(KAFKA_CONFIG['topics']):
                print(f"✅ All topics verified successfully")
                return {
                    'task_id': result.id,
                    'status': 'success',
                    'topics_found': found_topics,
                    'all_topics': topics_list
                }
            else:
                missing = set(KAFKA_CONFIG['topics']) - set(found_topics)
                raise Exception(f"Some topics are missing: {missing}")
                
        except Exception as e:
            print(f"❌ Failed to verify topics: {str(e)}")
            raise Exception(f"Failed to verify topics: {str(e)}")

    # Tasks
    task_create_input = PythonOperator(
        task_id='create_topic_input',
        python_callable=create_kafka_topic_input,
        retries=2,
        retry_delay=10,
    )

    task_create_output = PythonOperator(
        task_id='create_topic_output',
        python_callable=create_kafka_topic_output,
        retries=2,
        retry_delay=10,
    )

    task_verify_topics = PythonOperator(
        task_id='verify_topics',
        python_callable=verify_topics,
        retries=0,
    )

    # Dependencies
    # Tạo cả 2 topics song song, sau đó verify
    [task_create_input, task_create_output] >> task_verify_topics

