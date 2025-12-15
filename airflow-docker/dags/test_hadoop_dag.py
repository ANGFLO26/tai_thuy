from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
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


# Cấu hình Hadoop - cả namenode và datanode trên cùng một máy
HADOOP_CONFIG = {
    'host': '192.168.80.52',
    'queue': 'node_52',
    'sbin_path': '~/hadoop/sbin',  # Đường dẫn đến thư mục sbin của Hadoop
}


# ============== DAG: Test Hadoop Start ==============
with DAG(
    dag_id='test_hadoop_start',
    description='DAG test khởi động Hadoop cluster (Namenode + Datanode)',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'hadoop', 'start'],
) as dag_test_hadoop_start:

    def start_hadoop(**context):
        """Khởi động Hadoop cluster bằng script start-all.sh"""
        print(f"🚀 Starting Hadoop on {HADOOP_CONFIG['host']}...")
        
        # Chạy script start-all.sh
        command = f"cd {HADOOP_CONFIG['sbin_path']} && ./start-all.sh"
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=HADOOP_CONFIG['queue']
        )

        # Chờ kết quả từ Celery worker
        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Hadoop started successfully on {HADOOP_CONFIG['host']}")
            print(f"Stdout: {output.get('stdout', '')}")
            if output.get('stderr'):
                print(f"Stderr: {output.get('stderr', '')}")
            
            if output.get('return_code') != 0:
                raise Exception(f"Hadoop start script returned non-zero code: {output.get('return_code')}")
            
            return {
                'task_id': result.id,
                'host': HADOOP_CONFIG['host'],
                'queue': HADOOP_CONFIG['queue'],
                'command': command,
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Hadoop on {HADOOP_CONFIG['host']}: {str(e)}")
            raise Exception(f"Failed to start Hadoop on {HADOOP_CONFIG['host']}: {str(e)}")

    def check_hadoop_status(**context):
        """Kiểm tra trạng thái Hadoop bằng jps"""
        print("🔍 Checking Hadoop cluster status...")
        
        # Sử dụng jps để kiểm tra các Java processes của Hadoop
        command = "jps"
        
        try:
            result = run_command.apply_async(
                args=[command],
                kwargs={},
                queue=HADOOP_CONFIG['queue']
            )
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ Hadoop status checked on {HADOOP_CONFIG['host']}")
            print(f"Java processes: {output.get('stdout', '')}")
            
            if output.get('return_code') != 0:
                print(f"Warning: jps command returned non-zero code: {output.get('return_code')}")
            
            return {
                'host': HADOOP_CONFIG['host'],
                'status': 'success',
                'jps_output': output.get('stdout', ''),
                'stderr': output.get('stderr', '')
            }
        except Exception as e:
            print(f"❌ Failed to check Hadoop status on {HADOOP_CONFIG['host']}: {str(e)}")
            raise Exception(f"Failed to check Hadoop status: {str(e)}")

    # Tạo tasks
    task_start_hadoop = PythonOperator(
        task_id='start_hadoop',
        python_callable=start_hadoop,
    )

    task_check_status = PythonOperator(
        task_id='check_hadoop_status',
        python_callable=check_hadoop_status,
    )

    # Dependencies: Start -> Check Status
    task_start_hadoop >> task_check_status


# ============== DAG: Test Hadoop Stop ==============
with DAG(
    dag_id='test_hadoop_stop',
    description='DAG test dừng Hadoop cluster',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'hadoop', 'stop'],
) as dag_test_hadoop_stop:

    def stop_hadoop(**context):
        """Dừng Hadoop cluster bằng script stop-all.sh"""
        print(f"🛑 Stopping Hadoop on {HADOOP_CONFIG['host']}...")
        
        # Chạy script stop-all.sh
        command = f"cd {HADOOP_CONFIG['sbin_path']} && ./stop-all.sh"
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=HADOOP_CONFIG['queue']
        )

        # Chờ kết quả từ Celery worker
        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Hadoop stopped successfully on {HADOOP_CONFIG['host']}")
            print(f"Stdout: {output.get('stdout', '')}")
            if output.get('stderr'):
                print(f"Stderr: {output.get('stderr', '')}")
            
            if output.get('return_code') != 0:
                raise Exception(f"Hadoop stop script returned non-zero code: {output.get('return_code')}")
            
            return {
                'task_id': result.id,
                'host': HADOOP_CONFIG['host'],
                'command': command,
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to stop Hadoop on {HADOOP_CONFIG['host']}: {str(e)}")
            raise Exception(f"Failed to stop Hadoop on {HADOOP_CONFIG['host']}: {str(e)}")

    # Tạo task
    task_stop_hadoop = PythonOperator(
        task_id='stop_hadoop',
        python_callable=stop_hadoop,
    )

