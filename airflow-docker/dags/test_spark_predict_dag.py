from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    spark_submit,
    kill_process,
    check_hdfs_path,
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


# Cấu hình Spark Predict
PREDICT_CONFIG = {
    'host': '192.168.80.52',
    'queue': 'node_52',
    'spark_master': '192.168.80.52',
    'spark_master_url': 'spark://192.168.80.52:7077',
    'predict_script': '~/tai_thuy/predict/predict_fraud.py',
    'predict_working_dir': '~/spark/bin',
    'model_path': 'hdfs://192.168.80.52:9000/model',
    'kafka_bootstrap': '192.168.80.122:9092',
    'kafka_input_topic': 'input',
    'kafka_output_topic': 'output',
    'kafka_queue': 'node_122',
    'pid_file': '/tmp/spark_predict.pid',
    'log_file': '/tmp/spark_predict.log',
}


# ============== DAG: Test Spark Predict ==============
with DAG(
    dag_id='test_spark_predict',
    description='DAG test Spark streaming predict: đọc từ Kafka topic input, predict với model từ HDFS, ghi vào topic output',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'spark', 'predict', 'streaming'],
    params={
        'start_predict': Param(True, type='boolean', description='Bắt đầu Spark predict job'),
        'stop_predict': Param(False, type='boolean', description='Dừng Spark predict job'),
    }
) as dag_test_spark_predict:

    def check_prerequisites(**context):
        """Kiểm tra prerequisites: model trong HDFS và Kafka topics"""
        print("🔍 Checking prerequisites...")
        
        # 1. Kiểm tra model trong HDFS
        print(f"📦 Checking model in HDFS: {PREDICT_CONFIG['model_path']}")
        model_result = check_hdfs_path.apply_async(
            args=[PREDICT_CONFIG['model_path']],
            queue=PREDICT_CONFIG['queue']
        )
        
        # 2. Kiểm tra Kafka topic input
        print(f"📥 Checking Kafka topic '{PREDICT_CONFIG['kafka_input_topic']}'...")
        input_topic_result = check_kafka_topic.apply_async(
            args=[],
            kwargs={
                'bootstrap_server': PREDICT_CONFIG['kafka_bootstrap'],
                'topic_name': PREDICT_CONFIG['kafka_input_topic'],
            },
            queue=PREDICT_CONFIG['kafka_queue']
        )
        
        # 3. Kiểm tra Kafka topic output
        print(f"📤 Checking Kafka topic '{PREDICT_CONFIG['kafka_output_topic']}'...")
        output_topic_result = check_kafka_topic.apply_async(
            args=[],
            kwargs={
                'bootstrap_server': PREDICT_CONFIG['kafka_bootstrap'],
                'topic_name': PREDICT_CONFIG['kafka_output_topic'],
            },
            queue=PREDICT_CONFIG['kafka_queue']
        )
        
        try:
            # Chờ kết quả
            model_check = wait_for_celery_result(model_result, timeout=60)
            input_topic_check = wait_for_celery_result(input_topic_result, timeout=60)
            output_topic_check = wait_for_celery_result(output_topic_result, timeout=60)
            
            # Kiểm tra model
            if not model_check.get('exists'):
                raise Exception(
                    f"Model not found in HDFS: {PREDICT_CONFIG['model_path']}. "
                    f"Please train the model first."
                )
            print(f"✅ Model found in HDFS: {PREDICT_CONFIG['model_path']}")
            
            # Kiểm tra topic input
            if not input_topic_check.get('exists'):
                raise Exception(
                    f"Kafka topic '{PREDICT_CONFIG['kafka_input_topic']}' does not exist on "
                    f"{PREDICT_CONFIG['kafka_bootstrap']}. Please create it first."
                )
            print(f"✅ Kafka topic '{PREDICT_CONFIG['kafka_input_topic']}' exists")
            
            # Kiểm tra topic output
            if not output_topic_check.get('exists'):
                raise Exception(
                    f"Kafka topic '{PREDICT_CONFIG['kafka_output_topic']}' does not exist on "
                    f"{PREDICT_CONFIG['kafka_bootstrap']}. Please create it first."
                )
            print(f"✅ Kafka topic '{PREDICT_CONFIG['kafka_output_topic']}' exists")
            
            print("✅ All prerequisites checked successfully")
            return {
                'status': 'success',
                'model_exists': True,
                'input_topic_exists': True,
                'output_topic_exists': True,
            }
        except Exception as e:
            print(f"❌ Prerequisites check failed: {str(e)}")
            raise Exception(f"Prerequisites check failed: {str(e)}")

    def start_predict(**context):
        """Bắt đầu Spark streaming predict job"""
        if not context['params'].get('start_predict', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Spark predict job on {PREDICT_CONFIG['host']}...")
        print(f"Script: {PREDICT_CONFIG['predict_script']}")
        print(f"Working directory: {PREDICT_CONFIG['predict_working_dir']}")
        print(f"Spark Master: {PREDICT_CONFIG['spark_master_url']}")
        print(f"Model path: {PREDICT_CONFIG['model_path']}")
        print(f"📥 Kafka Input Topic: {PREDICT_CONFIG['kafka_input_topic']} on {PREDICT_CONFIG['kafka_bootstrap']}")
        print(f"📤 Kafka Output Topic: {PREDICT_CONFIG['kafka_output_topic']} on {PREDICT_CONFIG['kafka_bootstrap']}")
        
        # Spark config theo yêu cầu: executor-cores=12, cores.max=12
        # executor-cores được thêm vào conf với key 'spark.executor.cores' để function tự động thêm --executor-cores flag
        spark_conf = {
            'spark.executor.cores': '12',  # Sẽ được convert thành --executor-cores flag
            'spark.executor.instances': '1',
            'spark.cores.max': '12',
            'spark.blockManager.port': '40210',
            'spark.shuffle.io.port': '40110',
            'spark.driver.port': '40310',
            'spark.shuffle.io.connectionTimeout': '600s',
            'spark.network.timeout': '600s',
            'spark.executor.heartbeatInterval': '120s'
        }
        
        env_vars = {
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64',
            'PATH': '/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
        }
        
        result = spark_submit.apply_async(
            args=[PREDICT_CONFIG['predict_script']],
            kwargs={
                'master_url': PREDICT_CONFIG['spark_master_url'],
                'executor_memory': '4G',
                'driver_memory': '1G',
                'packages': ['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1'],
                'conf': spark_conf,
                'working_dir': PREDICT_CONFIG['predict_working_dir'],
                'env_vars': env_vars,
                'timeout': 3600,  # 1 giờ (nhưng sẽ detach nên không quan trọng)
                'detach': True,  # Chạy ở background vì là streaming job
                'pid_file': PREDICT_CONFIG['pid_file'],
                'log_file': PREDICT_CONFIG['log_file'],
            },
            queue=PREDICT_CONFIG['queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=120)
            print(f"✅ Spark predict job started successfully")
            print(f"Output: {output}")
            
            # Kiểm tra status (spark_submit trả về 'status': 'success' chứ không phải 'success': True)
            if output.get('status') != 'success':
                error_msg = output.get('error', output.get('stderr', 'Unknown error'))
                raise Exception(f"Failed to start predict job: {error_msg}")
            
            pid = output.get('pid')
            if not pid:
                raise Exception("PID not returned from background process")
            
            print(f"📝 Spark predict process PID: {pid}")
            print(f"📝 PID file: {PREDICT_CONFIG['pid_file']}")
            print(f"📝 Log file: {PREDICT_CONFIG['log_file']}")
            print(f"ℹ️  Job is running in background, reading from '{PREDICT_CONFIG['kafka_input_topic']}' and writing to '{PREDICT_CONFIG['kafka_output_topic']}'")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'pid': pid,
                'pid_file': PREDICT_CONFIG['pid_file'],
                'log_file': PREDICT_CONFIG['log_file'],
                'host': PREDICT_CONFIG['host'],
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to start Spark predict job: {str(e)}")
            raise Exception(f"Failed to start Spark predict job: {str(e)}")

    def stop_predict(**context):
        """Dừng Spark predict job"""
        if not context['params'].get('stop_predict', False):
            return {'skipped': True}
        
        print(f"🛑 Stopping Spark predict job...")
        print(f"PID file: {PREDICT_CONFIG['pid_file']}")
        
        result = kill_process.apply_async(
            args=[],
            kwargs={
                'pid_file': PREDICT_CONFIG['pid_file'],
                'process_name': 'predict_fraud.py',
            },
            queue=PREDICT_CONFIG['queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ Spark predict job stopped")
            print(f"Output: {output}")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to stop Spark predict job: {str(e)}")
            raise Exception(f"Failed to stop Spark predict job: {str(e)}")

    def check_predict_status(**context):
        """Kiểm tra trạng thái Spark predict job"""
        print(f"🔍 Checking Spark predict job status...")
        print(f"PID file: {PREDICT_CONFIG['pid_file']}")
        print(f"Log file: {PREDICT_CONFIG['log_file']}")
        
        # Đọc PID file để kiểm tra
        import subprocess
        check_cmd = f"test -f {PREDICT_CONFIG['pid_file']} && cat {PREDICT_CONFIG['pid_file']} || echo 'PID file not found'"
        
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
    task_check_prerequisites = PythonOperator(
        task_id='check_prerequisites',
        python_callable=check_prerequisites,
        retries=2,
        retry_delay=10,
    )

    task_start_predict = PythonOperator(
        task_id='start_predict',
        python_callable=start_predict,
        retries=0,
    )

    task_check_status = PythonOperator(
        task_id='check_predict_status',
        python_callable=check_predict_status,
        retries=0,
    )

    task_stop_predict = PythonOperator(
        task_id='stop_predict',
        python_callable=stop_predict,
        retries=0,
    )

    # Dependencies
    # Check prerequisites -> Start predict -> Check status
    task_check_prerequisites >> task_start_predict >> task_check_status
    # Stop predict is independent (can be triggered separately via params)
