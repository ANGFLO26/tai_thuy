from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    run_command,
    docker_compose_up,
    spark_submit,
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


# Cấu hình Full Pipeline
FULL_PIPELINE_CONFIG = {
    # Hadoop
    'hadoop_host': '192.168.80.52',
    'hadoop_queue': 'node_52',
    'hadoop_sbin_path': '~/hadoop/sbin',
    
    # Spark
    'spark_master': '192.168.80.52',
    'spark_master_url': 'spark://192.168.80.52:7077',
    'spark_master_queue': 'node_52',
    'spark_worker_1_host': '192.168.80.122',
    'spark_worker_1_queue': 'node_122',
    'spark_worker_2_host': '192.168.80.130',
    'spark_worker_2_queue': 'node_130',
    'spark_compose_path': '~/Documents/docker-spark/docker-compose.yml',
    
    # Kafka
    'kafka_host': '192.168.80.122',
    'kafka_queue': 'node_122',
    'kafka_compose_path': '~/kafka-docker/docker-compose.yml',
    'kafka_bootstrap': '192.168.80.122:9092',
    'kafka_input_topic': 'input',
    'kafka_output_topic': 'output',
    
    # Training
    'train_script': '~/tai_thuy/train_model/train_model.py',
    'train_working_dir': '~/spark/bin',
    'train_input': 'hdfs://192.168.80.52:9000/data/train.csv',
    'model_path': 'hdfs://192.168.80.52:9000/model',
    
    # Predict
    'predict_script': '~/tai_thuy/predict/predict_fraud.py',
    'predict_working_dir': '~/spark/bin',
    'predict_pid_file': '/tmp/spark_predict.pid',
    'predict_log_file': '/tmp/spark_predict.log',
    
    # Streaming
    'streaming_script': '~/tai_thuy/streaming/kafka_streaming.py',
    'streaming_working_dir': '~/spark/bin',
    'streaming_pid_file': '/tmp/spark_kafka_streaming.pid',
    'streaming_log_file': '/tmp/spark_kafka_streaming.log',
}


# ============== DAG: BigData Full Pipeline ==============
with DAG(
    dag_id='bigdata_full_pipeline',
    description='Full pipeline: Start infrastructure -> Train model -> Predict -> Stream',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['production', 'full', 'pipeline'],
    params={
        'start_infrastructure': Param(True, type='boolean', description='Khởi động infrastructure (Hadoop, Spark, Kafka)'),
        'train_model': Param(True, type='boolean', description='Training model'),
        'start_predict': Param(True, type='boolean', description='Start predict job'),
        'start_streaming': Param(True, type='boolean', description='Start streaming job'),
        'delay_before_streaming': Param(60, type='integer', description='Delay (seconds) before starting streaming after predict'),
    }
) as dag_bigdata_full_pipeline:

    # ========== PHASE 1: Infrastructure Setup ==========
    
    def start_hadoop(**context):
        """Khởi động Hadoop cluster - Từ test_hadoop_dag.py"""
        if not context['params'].get('start_infrastructure', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Hadoop on {FULL_PIPELINE_CONFIG['hadoop_host']}...")
        
        command = f"cd {FULL_PIPELINE_CONFIG['hadoop_sbin_path']} && ./start-all.sh"
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=FULL_PIPELINE_CONFIG['hadoop_queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Hadoop started successfully on {FULL_PIPELINE_CONFIG['hadoop_host']}")
            print(f"Stdout: {output.get('stdout', '')}")
            if output.get('stderr'):
                print(f"Stderr: {output.get('stderr', '')}")
            
            if output.get('return_code') != 0:
                raise Exception(f"Hadoop start script returned non-zero code: {output.get('return_code')}")
            
            return {
                'task_id': result.id,
                'host': FULL_PIPELINE_CONFIG['hadoop_host'],
                'queue': FULL_PIPELINE_CONFIG['hadoop_queue'],
                'command': command,
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Hadoop on {FULL_PIPELINE_CONFIG['hadoop_host']}: {str(e)}")
            raise Exception(f"Failed to start Hadoop on {FULL_PIPELINE_CONFIG['hadoop_host']}: {str(e)}")

    def start_spark_master(**context):
        """Khởi động Spark Master - Từ test_spark_dag.py và test_full_pipeline_dag.py"""
        if not context['params'].get('start_infrastructure', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Spark Master on {FULL_PIPELINE_CONFIG['spark_master']}...")
        
        result = docker_compose_up.apply_async(
            args=[FULL_PIPELINE_CONFIG['spark_compose_path']],
            kwargs={
                'services': ['spark-master'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=FULL_PIPELINE_CONFIG['spark_master_queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Spark Master started successfully on {FULL_PIPELINE_CONFIG['spark_master']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'host': FULL_PIPELINE_CONFIG['spark_master'],
                'queue': FULL_PIPELINE_CONFIG['spark_master_queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Spark Master on {FULL_PIPELINE_CONFIG['spark_master']}: {str(e)}")
            raise Exception(f"Failed to start Spark Master on {FULL_PIPELINE_CONFIG['spark_master']}: {str(e)}")

    def start_spark_worker_1(**context):
        """Khởi động Spark Worker 1 - Từ test_spark_dag.py và test_full_pipeline_dag.py"""
        if not context['params'].get('start_infrastructure', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Spark Worker 1 on {FULL_PIPELINE_CONFIG['spark_worker_1_host']}...")
        
        result = docker_compose_up.apply_async(
            args=[FULL_PIPELINE_CONFIG['spark_compose_path']],
            kwargs={
                'services': ['spark-worker'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=FULL_PIPELINE_CONFIG['spark_worker_1_queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Spark Worker 1 started successfully on {FULL_PIPELINE_CONFIG['spark_worker_1_host']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'host': FULL_PIPELINE_CONFIG['spark_worker_1_host'],
                'queue': FULL_PIPELINE_CONFIG['spark_worker_1_queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Spark Worker 1 on {FULL_PIPELINE_CONFIG['spark_worker_1_host']}: {str(e)}")
            raise Exception(f"Failed to start Spark Worker 1 on {FULL_PIPELINE_CONFIG['spark_worker_1_host']}: {str(e)}")

    def start_spark_worker_2(**context):
        """Khởi động Spark Worker 2 - Từ test_spark_dag.py và test_full_pipeline_dag.py"""
        if not context['params'].get('start_infrastructure', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Spark Worker 2 on {FULL_PIPELINE_CONFIG['spark_worker_2_host']}...")
        
        result = docker_compose_up.apply_async(
            args=[FULL_PIPELINE_CONFIG['spark_compose_path']],
            kwargs={
                'services': ['spark-worker'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=FULL_PIPELINE_CONFIG['spark_worker_2_queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Spark Worker 2 started successfully on {FULL_PIPELINE_CONFIG['spark_worker_2_host']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'host': FULL_PIPELINE_CONFIG['spark_worker_2_host'],
                'queue': FULL_PIPELINE_CONFIG['spark_worker_2_queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Spark Worker 2 on {FULL_PIPELINE_CONFIG['spark_worker_2_host']}: {str(e)}")
            raise Exception(f"Failed to start Spark Worker 2 on {FULL_PIPELINE_CONFIG['spark_worker_2_host']}: {str(e)}")

    def start_kafka(**context):
        """Khởi động Kafka - Từ test_full_pipeline_dag.py"""
        if not context['params'].get('start_infrastructure', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Kafka on {FULL_PIPELINE_CONFIG['kafka_host']}...")
        
        result = docker_compose_up.apply_async(
            args=[FULL_PIPELINE_CONFIG['kafka_compose_path']],
            kwargs={
                'services': ['kafka', 'zookeeper'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=FULL_PIPELINE_CONFIG['kafka_queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Kafka started successfully on {FULL_PIPELINE_CONFIG['kafka_host']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'host': FULL_PIPELINE_CONFIG['kafka_host'],
                'queue': FULL_PIPELINE_CONFIG['kafka_queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Kafka on {FULL_PIPELINE_CONFIG['kafka_host']}: {str(e)}")
            raise Exception(f"Failed to start Kafka on {FULL_PIPELINE_CONFIG['kafka_host']}: {str(e)}")

    def wait_for_services(**context):
        """Chờ services khởi động xong - Từ test_full_pipeline_dag.py"""
        print("⏳ Waiting for services to be ready...")
        time.sleep(30)  # Chờ 30 giây để services khởi động
        print("✅ Services should be ready now")
        return {'status': 'success', 'message': 'Services ready'}

    # ========== PHASE 2: Model Management & Training ==========
    
    def check_existing_model(**context):
        """Kiểm tra model cũ có tồn tại trong HDFS không"""
        if not context['params'].get('train_model', True):
            return {'skipped': True}
        
        print(f"🔍 Checking existing model in HDFS: {FULL_PIPELINE_CONFIG['model_path']}")
        
        result = check_hdfs_path.apply_async(
            args=[FULL_PIPELINE_CONFIG['model_path']],
            queue=FULL_PIPELINE_CONFIG['hadoop_queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            exists = output.get('exists', False)
            
            if exists:
                print(f"✅ Found existing model in HDFS: {FULL_PIPELINE_CONFIG['model_path']}")
            else:
                print(f"ℹ️  No existing model found in HDFS: {FULL_PIPELINE_CONFIG['model_path']}")
            
            return {
                'exists': exists,
                'model_path': FULL_PIPELINE_CONFIG['model_path'],
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to check existing model: {str(e)}")
            raise Exception(f"Failed to check existing model: {str(e)}")

    def delete_old_model(**context):
        """Xóa model cũ trong HDFS nếu tồn tại"""
        if not context['params'].get('train_model', True):
            return {'skipped': True}
        
        # Kiểm tra từ task trước
        ti = context['ti']
        check_result = ti.xcom_pull(task_ids='check_existing_model')
        
        if not check_result or not check_result.get('exists'):
            print("ℹ️  No existing model to delete")
            return {'skipped': True, 'reason': 'No existing model'}
        
        print(f"🗑️  Deleting old model from HDFS: {FULL_PIPELINE_CONFIG['model_path']}")
        
        command = f"hdfs dfs -rm -r {FULL_PIPELINE_CONFIG['model_path']}"
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=FULL_PIPELINE_CONFIG['hadoop_queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=120)
            print(f"✅ Old model deleted successfully")
            print(f"Stdout: {output.get('stdout', '')}")
            
            return {
                'task_id': result.id,
                'model_path': FULL_PIPELINE_CONFIG['model_path'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to delete old model: {str(e)}")
            raise Exception(f"Failed to delete old model: {str(e)}")

    def train_model(**context):
        """Training model từ HDFS và lưu về HDFS - Từ test_full_pipeline_dag.py"""
        if not context['params'].get('train_model', True):
            return {'skipped': True}
        
        print(f"🚀 Starting model training...")
        print(f"Input: {FULL_PIPELINE_CONFIG['train_input']}")
        print(f"Output: {FULL_PIPELINE_CONFIG['model_path']}")
        
        # Spark config
        spark_conf = {
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
            args=[FULL_PIPELINE_CONFIG['train_script']],
            kwargs={
                'master_url': FULL_PIPELINE_CONFIG['spark_master_url'],
                'executor_memory': '4G',
                'driver_memory': '1G',
                'conf': spark_conf,
                'working_dir': FULL_PIPELINE_CONFIG['train_working_dir'],
                'env_vars': env_vars,
                'timeout': 1800  # 30 phút
            },
            queue=FULL_PIPELINE_CONFIG['spark_master_queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=1800)
            print(f"✅ Model training completed")
            print(f"Stdout: {output.get('stdout', '')}")
            if output.get('stderr'):
                print(f"Stderr: {output.get('stderr', '')}")
            
            if output.get('return_code') != 0:
                raise Exception(f"Training failed: {output.get('stderr', '')}")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'output': output,
                'model_path': FULL_PIPELINE_CONFIG['model_path'],
            }
        except Exception as e:
            print(f"❌ Failed to train model: {str(e)}")
            raise Exception(f"Failed to train model: {str(e)}")

    def verify_model_saved(**context):
        """Verify model đã được lưu vào HDFS - Từ test_full_pipeline_dag.py"""
        if not context['params'].get('train_model', True):
            return {'skipped': True}
        
        print(f"🔍 Verifying model saved to HDFS: {FULL_PIPELINE_CONFIG['model_path']}")
        
        result = check_hdfs_path.apply_async(
            args=[FULL_PIPELINE_CONFIG['model_path']],
            queue=FULL_PIPELINE_CONFIG['hadoop_queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            
            if not output.get('exists'):
                raise Exception(f"Model was not saved to {FULL_PIPELINE_CONFIG['model_path']} after training")
            
            print(f"✅ Model verified in HDFS: {FULL_PIPELINE_CONFIG['model_path']}")
            
            return {
                'status': 'success',
                'model_path': FULL_PIPELINE_CONFIG['model_path'],
                'model_check': output
            }
        except Exception as e:
            print(f"❌ Failed to verify model: {str(e)}")
            raise Exception(f"Failed to verify model: {str(e)}")

    # ========== PHASE 3: Predict Job ==========
    
    def check_prerequisites(**context):
        """Kiểm tra prerequisites: model và Kafka topics - Từ test_spark_predict_dag.py"""
        if not context['params'].get('start_predict', True):
            return {'skipped': True}
        
        print("🔍 Checking prerequisites for predict job...")
        
        # 1. Kiểm tra model trong HDFS
        print(f"📦 Checking model in HDFS: {FULL_PIPELINE_CONFIG['model_path']}")
        model_result = check_hdfs_path.apply_async(
            args=[FULL_PIPELINE_CONFIG['model_path']],
            queue=FULL_PIPELINE_CONFIG['hadoop_queue']
        )
        
        # 2. Kiểm tra Kafka topic input
        print(f"📥 Checking Kafka topic '{FULL_PIPELINE_CONFIG['kafka_input_topic']}'...")
        input_topic_result = check_kafka_topic.apply_async(
            args=[],
            kwargs={
                'bootstrap_server': FULL_PIPELINE_CONFIG['kafka_bootstrap'],
                'topic_name': FULL_PIPELINE_CONFIG['kafka_input_topic'],
            },
            queue=FULL_PIPELINE_CONFIG['kafka_queue']
        )
        
        # 3. Kiểm tra Kafka topic output
        print(f"📤 Checking Kafka topic '{FULL_PIPELINE_CONFIG['kafka_output_topic']}'...")
        output_topic_result = check_kafka_topic.apply_async(
            args=[],
            kwargs={
                'bootstrap_server': FULL_PIPELINE_CONFIG['kafka_bootstrap'],
                'topic_name': FULL_PIPELINE_CONFIG['kafka_output_topic'],
            },
            queue=FULL_PIPELINE_CONFIG['kafka_queue']
        )
        
        try:
            # Chờ kết quả
            model_check = wait_for_celery_result(model_result, timeout=60)
            input_topic_check = wait_for_celery_result(input_topic_result, timeout=60)
            output_topic_check = wait_for_celery_result(output_topic_result, timeout=60)
            
            # Kiểm tra model
            if not model_check.get('exists'):
                raise Exception(
                    f"Model not found in HDFS: {FULL_PIPELINE_CONFIG['model_path']}. "
                    f"Please train the model first."
                )
            print(f"✅ Model found in HDFS: {FULL_PIPELINE_CONFIG['model_path']}")
            
            # Kiểm tra topic input
            if not input_topic_check.get('exists'):
                raise Exception(
                    f"Kafka topic '{FULL_PIPELINE_CONFIG['kafka_input_topic']}' does not exist on "
                    f"{FULL_PIPELINE_CONFIG['kafka_bootstrap']}. Please create it first."
                )
            print(f"✅ Kafka topic '{FULL_PIPELINE_CONFIG['kafka_input_topic']}' exists")
            
            # Kiểm tra topic output
            if not output_topic_check.get('exists'):
                raise Exception(
                    f"Kafka topic '{FULL_PIPELINE_CONFIG['kafka_output_topic']}' does not exist on "
                    f"{FULL_PIPELINE_CONFIG['kafka_bootstrap']}. Please create it first."
                )
            print(f"✅ Kafka topic '{FULL_PIPELINE_CONFIG['kafka_output_topic']}' exists")
            
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
        """Bắt đầu Spark streaming predict job - Từ test_spark_predict_dag.py"""
        if not context['params'].get('start_predict', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Spark predict job on {FULL_PIPELINE_CONFIG['spark_master']}...")
        print(f"Script: {FULL_PIPELINE_CONFIG['predict_script']}")
        print(f"Spark Master: {FULL_PIPELINE_CONFIG['spark_master_url']}")
        print(f"Model path: {FULL_PIPELINE_CONFIG['model_path']}")
        print(f"📥 Kafka Input Topic: {FULL_PIPELINE_CONFIG['kafka_input_topic']}")
        print(f"📤 Kafka Output Topic: {FULL_PIPELINE_CONFIG['kafka_output_topic']}")
        
        # Spark config
        spark_conf = {
            'spark.executor.cores': '12',
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
            args=[FULL_PIPELINE_CONFIG['predict_script']],
            kwargs={
                'master_url': FULL_PIPELINE_CONFIG['spark_master_url'],
                'executor_memory': '4G',
                'driver_memory': '1G',
                'packages': ['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1'],
                'conf': spark_conf,
                'working_dir': FULL_PIPELINE_CONFIG['predict_working_dir'],
                'env_vars': env_vars,
                'timeout': 3600,
                'detach': True,
                'pid_file': FULL_PIPELINE_CONFIG['predict_pid_file'],
                'log_file': FULL_PIPELINE_CONFIG['predict_log_file'],
            },
            queue=FULL_PIPELINE_CONFIG['spark_master_queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=120)
            print(f"✅ Spark predict job started successfully")
            print(f"Output: {output}")
            
            if output.get('status') != 'success':
                error_msg = output.get('error', output.get('stderr', 'Unknown error'))
                raise Exception(f"Failed to start predict job: {error_msg}")
            
            pid = output.get('pid')
            if not pid:
                raise Exception("PID not returned from background process")
            
            print(f"📝 Spark predict process PID: {pid}")
            print(f"📝 PID file: {FULL_PIPELINE_CONFIG['predict_pid_file']}")
            print(f"📝 Log file: {FULL_PIPELINE_CONFIG['predict_log_file']}")
            print(f"ℹ️  Job is running in background, reading from '{FULL_PIPELINE_CONFIG['kafka_input_topic']}' and writing to '{FULL_PIPELINE_CONFIG['kafka_output_topic']}'")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'pid': pid,
                'pid_file': FULL_PIPELINE_CONFIG['predict_pid_file'],
                'log_file': FULL_PIPELINE_CONFIG['predict_log_file'],
                'host': FULL_PIPELINE_CONFIG['spark_master'],
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to start Spark predict job: {str(e)}")
            raise Exception(f"Failed to start Spark predict job: {str(e)}")

    # ========== PHASE 4: Wait Before Streaming ==========
    
    def wait_before_streaming(**context):
        """Chờ một khoảng thời gian trước khi start streaming"""
        if not context['params'].get('start_streaming', True):
            return {'skipped': True}
        
        delay_seconds = context['params'].get('delay_before_streaming', 60)
        print(f"⏳ Waiting {delay_seconds} seconds before starting streaming...")
        time.sleep(delay_seconds)
        print(f"✅ Wait completed, ready to start streaming")
        return {'status': 'success', 'delay_seconds': delay_seconds}

    # ========== PHASE 5: Streaming Job ==========
    
    def check_topic_for_streaming(**context):
        """Kiểm tra topic input có tồn tại không - Từ test_kafka_streaming_dag.py"""
        if not context['params'].get('start_streaming', True):
            return {'skipped': True}
        
        print(f"🔍 Checking if Kafka topic '{FULL_PIPELINE_CONFIG['kafka_input_topic']}' exists...")
        print(f"Bootstrap server: {FULL_PIPELINE_CONFIG['kafka_bootstrap']}")
        
        result = check_kafka_topic.apply_async(
            args=[],
            kwargs={
                'bootstrap_server': FULL_PIPELINE_CONFIG['kafka_bootstrap'],
                'topic_name': FULL_PIPELINE_CONFIG['kafka_input_topic'],
            },
            queue=FULL_PIPELINE_CONFIG['kafka_queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            print(f"Topic check result: {output}")
            
            if not output.get('exists'):
                raise Exception(
                    f"Topic '{FULL_PIPELINE_CONFIG['kafka_input_topic']}' does not exist on "
                    f"{FULL_PIPELINE_CONFIG['kafka_bootstrap']}. Please create it first."
                )
            
            print(f"✅ Topic '{FULL_PIPELINE_CONFIG['kafka_input_topic']}' exists on {FULL_PIPELINE_CONFIG['kafka_bootstrap']}")
            return {
                'task_id': result.id,
                'status': 'success',
                'topic': FULL_PIPELINE_CONFIG['kafka_input_topic'],
                'bootstrap_server': FULL_PIPELINE_CONFIG['kafka_bootstrap'],
                'exists': True
            }
        except Exception as e:
            print(f"❌ Topic check failed: {str(e)}")
            raise Exception(f"Topic check failed: {str(e)}")

    def start_streaming(**context):
        """Bắt đầu Spark streaming job: đọc từ HDFS, ghi vào Kafka topic input - Từ test_kafka_streaming_dag.py"""
        if not context['params'].get('start_streaming', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Spark streaming job on {FULL_PIPELINE_CONFIG['spark_master']}...")
        print(f"Script: {FULL_PIPELINE_CONFIG['streaming_script']}")
        print(f"Spark Master: {FULL_PIPELINE_CONFIG['spark_master_url']}")
        print(f"📡 Kafka Bootstrap Server: {FULL_PIPELINE_CONFIG['kafka_bootstrap']}")
        print(f"📝 Kafka Topic: {FULL_PIPELINE_CONFIG['kafka_input_topic']}")
        print(f"ℹ️  Job will read from HDFS and stream to topic '{FULL_PIPELINE_CONFIG['kafka_input_topic']}'")
        
        # Spark config
        spark_conf = {
            'spark.executor.cores': '8',
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
            args=[FULL_PIPELINE_CONFIG['streaming_script']],
            kwargs={
                'master_url': FULL_PIPELINE_CONFIG['spark_master_url'],
                'executor_memory': '4G',
                'driver_memory': '1G',
                'packages': ['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1'],
                'conf': spark_conf,
                'working_dir': FULL_PIPELINE_CONFIG['streaming_working_dir'],
                'env_vars': env_vars,
                'timeout': 3600,
                'detach': True,
                'pid_file': FULL_PIPELINE_CONFIG['streaming_pid_file'],
                'log_file': FULL_PIPELINE_CONFIG['streaming_log_file'],
            },
            queue=FULL_PIPELINE_CONFIG['spark_master_queue']
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
            print(f"📝 PID file: {FULL_PIPELINE_CONFIG['streaming_pid_file']}")
            print(f"📝 Log file: {FULL_PIPELINE_CONFIG['streaming_log_file']}")
            print(f"ℹ️  Job is running in background, streaming from HDFS to '{FULL_PIPELINE_CONFIG['kafka_input_topic']}'")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'pid': pid,
                'pid_file': FULL_PIPELINE_CONFIG['streaming_pid_file'],
                'log_file': FULL_PIPELINE_CONFIG['streaming_log_file'],
                'host': FULL_PIPELINE_CONFIG['spark_master'],
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to start Spark streaming job: {str(e)}")
            raise Exception(f"Failed to start Spark streaming job: {str(e)}")

    # ========== TASKS ==========
    
    # Phase 1: Infrastructure
    task_start_hadoop = PythonOperator(
        task_id='start_hadoop',
        python_callable=start_hadoop,
        retries=0,
    )

    task_start_spark_master = PythonOperator(
        task_id='start_spark_master',
        python_callable=start_spark_master,
        retries=0,
    )

    task_start_spark_worker_1 = PythonOperator(
        task_id='start_spark_worker_1',
        python_callable=start_spark_worker_1,
        retries=0,
    )

    task_start_spark_worker_2 = PythonOperator(
        task_id='start_spark_worker_2',
        python_callable=start_spark_worker_2,
        retries=0,
    )

    task_start_kafka = PythonOperator(
        task_id='start_kafka',
        python_callable=start_kafka,
        retries=0,
    )

    task_wait_services = PythonOperator(
        task_id='wait_for_services',
        python_callable=wait_for_services,
        retries=0,
    )

    # Phase 2: Model Management & Training
    task_check_existing_model = PythonOperator(
        task_id='check_existing_model',
        python_callable=check_existing_model,
        retries=0,
    )

    task_delete_old_model = PythonOperator(
        task_id='delete_old_model',
        python_callable=delete_old_model,
        retries=0,
    )

    task_train_model = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        retries=2,
        retry_delay=60,
    )

    task_verify_model_saved = PythonOperator(
        task_id='verify_model_saved',
        python_callable=verify_model_saved,
        retries=2,
        retry_delay=10,
    )

    # Phase 3: Predict
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

    # Phase 4: Wait
    task_wait_before_streaming = PythonOperator(
        task_id='wait_before_streaming',
        python_callable=wait_before_streaming,
        retries=0,
    )

    # Phase 5: Streaming
    task_check_topic_for_streaming = PythonOperator(
        task_id='check_topic_for_streaming',
        python_callable=check_topic_for_streaming,
        retries=2,
        retry_delay=10,
    )

    task_start_streaming = PythonOperator(
        task_id='start_streaming',
        python_callable=start_streaming,
        retries=0,
    )

    # ========== DEPENDENCIES ==========
    
    # Phase 1: Infrastructure Setup
    # Hadoop start first, then Spark Master, then Workers (parallel), then Kafka can start in parallel
    task_start_hadoop >> task_start_spark_master >> [task_start_spark_worker_1, task_start_spark_worker_2]
    [task_start_spark_worker_1, task_start_spark_worker_2, task_start_kafka] >> task_wait_services
    
    # Phase 2: Model Management & Training
    # After services ready -> Check existing model -> Delete if exists -> Train -> Verify
    task_wait_services >> task_check_existing_model >> task_delete_old_model >> task_train_model >> task_verify_model_saved
    
    # Phase 3: Predict
    # After model verified -> Check prerequisites -> Start predict
    task_verify_model_saved >> task_check_prerequisites >> task_start_predict
    
    # Phase 4: Wait before streaming
    # After predict started -> Wait
    task_start_predict >> task_wait_before_streaming
    
    # Phase 5: Streaming
    # After wait -> Check topic -> Start streaming
    task_wait_before_streaming >> task_check_topic_for_streaming >> task_start_streaming

