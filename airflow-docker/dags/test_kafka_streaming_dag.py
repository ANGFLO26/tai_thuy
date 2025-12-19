from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    spark_submit,
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


# Cấu hình Streaming (Spark-based)
STREAMING_CONFIG = {
    'host': '192.168.80.52',
    'queue': 'node_52',
    'spark_master': '192.168.80.52',
    'spark_master_url': 'spark://192.168.80.52:7077',
    'streaming_script': '~/tai_thuy/streaming/kafka_streaming.py',
    'streaming_working_dir': '~/spark/bin',
    'kafka_bootstrap': '192.168.80.122:9092',
    'kafka_topic': 'input',
    'kafka_queue': 'node_122',  # Queue cho Kafka host (check topic)
    'pid_file': '/tmp/spark_kafka_streaming.pid',
    'log_file': '/tmp/spark_kafka_streaming.log',
}


# ============== DAG: Test Kafka Streaming (Spark-based) ==============
with DAG(
    dag_id='test_kafka_streaming',
    description='DAG test streaming dữ liệu từ HDFS vào Kafka topic input bằng Spark',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'streaming', 'kafka', 'spark'],
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
        """Bắt đầu Spark streaming job: đọc từ HDFS, ghi vào Kafka topic input"""
        if not context['params'].get('start_streaming', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Spark streaming job on {STREAMING_CONFIG['host']}...")
        print(f"Script: {STREAMING_CONFIG['streaming_script']}")
        print(f"Working directory: {STREAMING_CONFIG['streaming_working_dir']}")
        print(f"Spark Master: {STREAMING_CONFIG['spark_master_url']}")
        print(f"📡 Kafka Bootstrap Server: {STREAMING_CONFIG['kafka_bootstrap']}")
        print(f"📝 Kafka Topic: {STREAMING_CONFIG['kafka_topic']}")
        print(f"ℹ️  Job will read from HDFS and stream to topic '{STREAMING_CONFIG['kafka_topic']}'")
        
        # Spark config theo yêu cầu: executor-cores=8, cores.max=8
        # executor-cores được thêm vào conf với key 'spark.executor.cores' để function tự động thêm --executor-cores flag
        spark_conf = {
            'spark.executor.cores': '8',  # Sẽ được convert thành --executor-cores flag
            'spark.executor.instances': '1',
            'spark.cores.max': '8',
            'spark.blockManager.port': '40200',
            'spark.shuffle.io.port': '40100',
            'spark.driver.port': '40300',
            'spark.shuffle.io.connectionTimeout': '600s',
            'spark.network.timeout': '600s',
            'spark.executor.heartbeatInterval': '120s'
        }
        
        env_vars = {
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64',
            'PATH': '/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
        }
        
        result = spark_submit.apply_async(
            args=[STREAMING_CONFIG['streaming_script']],
            kwargs={
                'master_url': STREAMING_CONFIG['spark_master_url'],
                'executor_memory': '4G',
                'driver_memory': '1G',
                'packages': ['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1'],
                'conf': spark_conf,
                'working_dir': STREAMING_CONFIG['streaming_working_dir'],
                'env_vars': env_vars,
                'timeout': 3600,
                'detach': True,  # Chạy ở background vì là streaming job
                'pid_file': STREAMING_CONFIG['pid_file'],
                'log_file': STREAMING_CONFIG['log_file'],
            },
            queue=STREAMING_CONFIG['queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=120)
            print(f"✅ Spark streaming job started successfully")
            print(f"Output: {output}")
            
            if output.get('status') != 'success':
                error_msg = output.get('error', output.get('stderr', 'Unknown error'))
                raise Exception(f"Failed to start streaming job: {error_msg}")
            
            pid = output.get('pid')
            if not pid:
                raise Exception("PID not returned from background process")
            
            print(f"📝 Spark streaming process PID: {pid}")
            print(f"📝 PID file: {STREAMING_CONFIG['pid_file']}")
            print(f"📝 Log file: {STREAMING_CONFIG['log_file']}")
            print(f"ℹ️  Job is running in background, streaming from HDFS to '{STREAMING_CONFIG['kafka_topic']}'")
            
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
            print(f"❌ Failed to start Spark streaming job: {str(e)}")
            raise Exception(f"Failed to start Spark streaming job: {str(e)}")

    def stop_streaming(**context):
        """Dừng Spark streaming job"""
        if not context['params'].get('stop_streaming', False):
            return {'skipped': True}
        
        print(f"🛑 Stopping Spark streaming job...")
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
            print(f"✅ Spark streaming job stopped")
            print(f"Output: {output}")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to stop Spark streaming job: {str(e)}")
            raise Exception(f"Failed to stop Spark streaming job: {str(e)}")

    def check_streaming_status(**context):
        """Kiểm tra trạng thái Spark streaming job"""
        print(f"🔍 Checking Spark streaming job status...")
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
