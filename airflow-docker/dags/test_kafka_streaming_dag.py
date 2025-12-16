from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    run_python_background,
    kill_process,
    check_kafka_topic,
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


# Cấu hình Streaming
STREAMING_CONFIG = {
    'host': '192.168.80.52',
    'queue': 'node_52',
    'streaming_dir': '~/tai_thuy/streaming',
    'streaming_script': 'kafka_streaming.py',
    'pid_file': '/tmp/kafka_streaming.pid',
    'log_file': '/tmp/kafka_streaming.log',
    'kafka_bootstrap': '192.168.80.122:9092',
    'kafka_topic': 'input',
    'kafka_queue': 'node_122',  # Queue cho Kafka host
}


# ============== DAG: Test Kafka Streaming ==============
with DAG(
    dag_id='test_kafka_streaming',
    description='DAG test streaming dữ liệu vào Kafka topic input',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'streaming', 'kafka'],
    params={
        'start_streaming': Param(True, type='boolean', description='Bắt đầu streaming'),
        'stop_streaming': Param(False, type='boolean', description='Dừng streaming'),
    }
) as dag_test_kafka_streaming:

    def check_topic_exists(**context):
        """Kiểm tra topic 'input' có tồn tại không"""
        print(f"🔍 Checking if Kafka topic '{STREAMING_CONFIG['kafka_topic']}' exists...")
        print(f"Bootstrap server: {STREAMING_CONFIG['kafka_bootstrap']}")
        
        result = check_kafka_topic.apply_async(
            args=[],
            kwargs={
                'bootstrap_server': STREAMING_CONFIG['kafka_bootstrap'],
                'topic_name': STREAMING_CONFIG['kafka_topic'],
            },
            queue=STREAMING_CONFIG['kafka_queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            print(f"Topic check result: {output}")
            
            if not output.get('exists'):
                raise Exception(
                    f"Topic '{STREAMING_CONFIG['kafka_topic']}' does not exist on "
                    f"{STREAMING_CONFIG['kafka_bootstrap']}. Please create it first."
                )
            
            print(f"✅ Topic '{STREAMING_CONFIG['kafka_topic']}' exists on {STREAMING_CONFIG['kafka_bootstrap']}")
            return {
                'task_id': result.id,
                'status': 'success',
                'topic': STREAMING_CONFIG['kafka_topic'],
                'bootstrap_server': STREAMING_CONFIG['kafka_bootstrap'],
                'exists': True
            }
        except Exception as e:
            print(f"❌ Topic check failed: {str(e)}")
            raise Exception(f"Topic check failed: {str(e)}")

    def start_streaming(**context):
        """Bắt đầu streaming dữ liệu vào Kafka topic input"""
        if not context['params'].get('start_streaming', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Kafka streaming on {STREAMING_CONFIG['host']}...")
        print(f"Script: {STREAMING_CONFIG['streaming_script']}")
        print(f"Directory: {STREAMING_CONFIG['streaming_dir']}")
        print(f"📡 Kafka Bootstrap Server: {STREAMING_CONFIG['kafka_bootstrap']}")
        print(f"📝 Kafka Topic: {STREAMING_CONFIG['kafka_topic']}")
        print(f"ℹ️  Script will stream data to topic '{STREAMING_CONFIG['kafka_topic']}' on {STREAMING_CONFIG['kafka_bootstrap']}")
        
        result = run_python_background.apply_async(
            args=[STREAMING_CONFIG['streaming_script']],
            kwargs={
                'working_dir': STREAMING_CONFIG['streaming_dir'],
                'pid_file': STREAMING_CONFIG['pid_file'],
                'log_file': STREAMING_CONFIG['log_file'],
            },
            queue=STREAMING_CONFIG['queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ Kafka streaming started successfully")
            print(f"Output: {output}")
            
            if not output.get('success'):
                raise Exception(f"Failed to start streaming: {output.get('error', 'Unknown error')}")
            
            pid = output.get('pid')
            if not pid:
                raise Exception("PID not returned from background process")
            
            print(f"📝 Streaming process PID: {pid}")
            print(f"📝 PID file: {STREAMING_CONFIG['pid_file']}")
            print(f"📝 Log file: {STREAMING_CONFIG['log_file']}")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'pid': pid,
                'pid_file': STREAMING_CONFIG['pid_file'],
                'log_file': STREAMING_CONFIG['log_file'],
                'host': STREAMING_CONFIG['host'],
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to start Kafka streaming: {str(e)}")
            raise Exception(f"Failed to start Kafka streaming: {str(e)}")

    def stop_streaming(**context):
        """Dừng streaming process"""
        if not context['params'].get('stop_streaming', False):
            return {'skipped': True}
        
        print(f"🛑 Stopping Kafka streaming...")
        print(f"PID file: {STREAMING_CONFIG['pid_file']}")
        
        result = kill_process.apply_async(
            args=[],
            kwargs={
                'pid_file': STREAMING_CONFIG['pid_file'],
                'process_name': 'kafka_streaming.py',
            },
            queue=STREAMING_CONFIG['queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ Kafka streaming stopped")
            print(f"Output: {output}")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to stop Kafka streaming: {str(e)}")
            raise Exception(f"Failed to stop Kafka streaming: {str(e)}")

    def check_streaming_status(**context):
        """Kiểm tra trạng thái streaming process"""
        print(f"🔍 Checking streaming status...")
        print(f"PID file: {STREAMING_CONFIG['pid_file']}")
        print(f"Log file: {STREAMING_CONFIG['log_file']}")
        
        # Đọc PID file để kiểm tra
        import subprocess
        check_cmd = f"test -f {STREAMING_CONFIG['pid_file']} && cat {STREAMING_CONFIG['pid_file']} || echo 'PID file not found'"
        
        result = subprocess.run(
            check_cmd,
            shell=True,
            capture_output=True,
            text=True
        )
        
        pid = result.stdout.strip() if result.stdout else None
        
        if pid and pid != 'PID file not found':
            print(f"📝 Found PID: {pid}")
            # Kiểm tra process có đang chạy không
            ps_cmd = f"ps -p {pid} > /dev/null 2>&1 && echo 'running' || echo 'not running'"
            ps_result = subprocess.run(ps_cmd, shell=True, capture_output=True, text=True)
            status = ps_result.stdout.strip()
            print(f"📊 Process status: {status}")
            return {'pid': pid, 'status': status}
        else:
            print(f"⚠️ PID file not found or empty")
            return {'pid': None, 'status': 'not found'}

    # Tasks
    task_check_topic = PythonOperator(
        task_id='check_topic_exists',
        python_callable=check_topic_exists,
        retries=2,
        retry_delay=10,
    )

    task_start_streaming = PythonOperator(
        task_id='start_streaming',
        python_callable=start_streaming,
        retries=0,
    )

    task_check_status = PythonOperator(
        task_id='check_streaming_status',
        python_callable=check_streaming_status,
        retries=0,
    )

    task_stop_streaming = PythonOperator(
        task_id='stop_streaming',
        python_callable=stop_streaming,
        retries=0,
    )

    # Dependencies
    # Check topic exists first -> Start streaming -> Check status
    task_check_topic >> task_start_streaming >> task_check_status
    # Stop streaming is independent (can be triggered separately via params)

