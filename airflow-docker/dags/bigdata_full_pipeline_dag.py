from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    run_command,
    docker_compose_up,
    docker_compose_ps,
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
    'kafka_topic_partitions': 1,
    'kafka_topic_replication_factor': 1,
    
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

    def create_kafka_topic_if_not_exists(topic_name, bootstrap_server, queue, partitions=1, replication_factor=1):
        """Tạo Kafka topic nếu chưa tồn tại"""
        print(f"🔧 Creating Kafka topic '{topic_name}' if not exists...")
        print(f"Bootstrap server: {bootstrap_server}")
        print(f"Partitions: {partitions}, Replication factor: {replication_factor}")
        
        command = (
            f"docker exec -i kafka kafka-topics "
            f"--create "
            f"--topic {topic_name} "
            f"--bootstrap-server {bootstrap_server} "
            f"--partitions {partitions} "
            f"--replication-factor {replication_factor}"
        )
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=queue
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            
            # Nếu topic đã tồn tại thì OK (không phải lỗi)
            if output.get('return_code') != 0:
                stderr = output.get('stderr', '')
                if 'already exists' in stderr.lower():
                    print(f"ℹ️  Topic '{topic_name}' already exists (this is OK)")
                    return True
                else:
                    raise Exception(f"Failed to create topic '{topic_name}': {stderr}")
            
            print(f"✅ Topic '{topic_name}' created successfully")
            return True
        except Exception as e:
            print(f"❌ Failed to create topic '{topic_name}': {str(e)}")
            raise Exception(f"Failed to create topic '{topic_name}': {str(e)}")

    def start_kafka(**context):
        """Khởi động Kafka và tự động tạo topics input và output"""
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
            
            # Chờ một chút để Kafka sẵn sàng nhận lệnh tạo topic
            print("⏳ Waiting for Kafka to be ready before creating topics...")
            time.sleep(15)
            
            # Tự động tạo hai topic input và output
            print("🔧 Creating Kafka topics: 'input' and 'output'...")
            
            # Tạo topic input
            create_kafka_topic_if_not_exists(
                topic_name=FULL_PIPELINE_CONFIG['kafka_input_topic'],
                bootstrap_server=FULL_PIPELINE_CONFIG['kafka_bootstrap'],
                queue=FULL_PIPELINE_CONFIG['kafka_queue'],
                partitions=FULL_PIPELINE_CONFIG['kafka_topic_partitions'],
                replication_factor=FULL_PIPELINE_CONFIG['kafka_topic_replication_factor']
            )
            
            # Tạo topic output
            create_kafka_topic_if_not_exists(
                topic_name=FULL_PIPELINE_CONFIG['kafka_output_topic'],
                bootstrap_server=FULL_PIPELINE_CONFIG['kafka_bootstrap'],
                queue=FULL_PIPELINE_CONFIG['kafka_queue'],
                partitions=FULL_PIPELINE_CONFIG['kafka_topic_partitions'],
                replication_factor=FULL_PIPELINE_CONFIG['kafka_topic_replication_factor']
            )
            
            print(f"✅ Kafka topics '{FULL_PIPELINE_CONFIG['kafka_input_topic']}' and '{FULL_PIPELINE_CONFIG['kafka_output_topic']}' created/verified")
            
            return {
                'task_id': result.id,
                'host': FULL_PIPELINE_CONFIG['kafka_host'],
                'queue': FULL_PIPELINE_CONFIG['kafka_queue'],
                'output': output,
                'status': 'success',
                'topics_created': [
                    FULL_PIPELINE_CONFIG['kafka_input_topic'],
                    FULL_PIPELINE_CONFIG['kafka_output_topic']
                ]
            }
        except Exception as e:
            print(f"❌ Failed to start Kafka on {FULL_PIPELINE_CONFIG['kafka_host']}: {str(e)}")
            raise Exception(f"Failed to start Kafka on {FULL_PIPELINE_CONFIG['kafka_host']}: {str(e)}")

    # ========== PHASE 2: Wait for Services Ready ==========
    
    def check_hadoop_ready(**context):
        """Kiểm tra Hadoop đã sẵn sàng - Từ test_hadoop_dag.py"""
        if not context['params'].get('start_infrastructure', True):
            return {'skipped': True}
        
        print("🔍 Checking Hadoop cluster status...")
        
        # Sử dụng jps để kiểm tra các Java processes của Hadoop
        command = "jps"
        
        try:
            result = run_command.apply_async(
                args=[command],
                kwargs={},
                queue=FULL_PIPELINE_CONFIG['hadoop_queue']
            )
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ Hadoop status checked on {FULL_PIPELINE_CONFIG['hadoop_host']}")
            print(f"Java processes: {output.get('stdout', '')}")
            
            if output.get('return_code') != 0:
                print(f"Warning: jps command returned non-zero code: {output.get('return_code')}")
            
            return {
                'host': FULL_PIPELINE_CONFIG['hadoop_host'],
                'status': 'success',
                'jps_output': output.get('stdout', ''),
                'stderr': output.get('stderr', '')
            }
        except Exception as e:
            print(f"❌ Failed to check Hadoop status on {FULL_PIPELINE_CONFIG['hadoop_host']}: {str(e)}")
            raise Exception(f"Failed to check Hadoop status: {str(e)}")

    def check_spark_ready(**context):
        """Kiểm tra Spark đã sẵn sàng - Từ test_spark_dag.py"""
        if not context['params'].get('start_infrastructure', True):
            return {'skipped': True}
        
        print("🔍 Checking Spark cluster status...")
        
        results = {}
        spark_services = {
            'spark-master': {
                'host': FULL_PIPELINE_CONFIG['spark_master'],
                'queue': FULL_PIPELINE_CONFIG['spark_master_queue'],
                'path': FULL_PIPELINE_CONFIG['spark_compose_path']
            },
            'spark-worker-1': {
                'host': FULL_PIPELINE_CONFIG['spark_worker_1_host'],
                'queue': FULL_PIPELINE_CONFIG['spark_worker_1_queue'],
                'path': FULL_PIPELINE_CONFIG['spark_compose_path']
            },
            'spark-worker-2': {
                'host': FULL_PIPELINE_CONFIG['spark_worker_2_host'],
                'queue': FULL_PIPELINE_CONFIG['spark_worker_2_queue'],
                'path': FULL_PIPELINE_CONFIG['spark_compose_path']
            }
        }
        
        for service_name, config in spark_services.items():
            try:
                result = docker_compose_ps.apply_async(
                    args=[config['path']],
                    queue=config['queue']
                )
                output = wait_for_celery_result(result, timeout=60)
                results[service_name] = {
                    'host': config['host'],
                    'status': 'success',
                    'output': output.get('output', '')
                }
                print(f"✅ {service_name} status checked on {config['host']}")
            except Exception as e:
                results[service_name] = {
                    'host': config['host'],
                    'status': 'error',
                    'error': str(e)
                }
                print(f"❌ Failed to check {service_name} on {config['host']}: {str(e)}")
        
        return results

    def check_kafka_ready(**context):
        """Kiểm tra Kafka đã sẵn sàng"""
        if not context['params'].get('start_infrastructure', True):
            return {'skipped': True}
        
        print("🔍 Checking Kafka status...")
        
        try:
            result = docker_compose_ps.apply_async(
                args=[FULL_PIPELINE_CONFIG['kafka_compose_path']],
                queue=FULL_PIPELINE_CONFIG['kafka_queue']
            )
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ Kafka status checked on {FULL_PIPELINE_CONFIG['kafka_host']}")
            
            return {
                'host': FULL_PIPELINE_CONFIG['kafka_host'],
                'status': 'success',
                'output': output.get('output', '')
            }
        except Exception as e:
            print(f"❌ Failed to check Kafka status on {FULL_PIPELINE_CONFIG['kafka_host']}: {str(e)}")
            raise Exception(f"Failed to check Kafka status: {str(e)}")

    # ========== PHASE 3: Train Model ==========

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

    # ========== PHASE 4: Check Kafka Topics ==========
    
    def check_kafka_topics(**context):
        """Kiểm tra và tự động tạo Kafka topics (input và output) nếu chưa tồn tại"""
        if not context['params'].get('start_predict', True):
            return {'skipped': True}
        
        print("🔍 Checking Kafka topics for predict job...")
        
        # 1. Kiểm tra Kafka topic input
        print(f"📥 Checking Kafka topic '{FULL_PIPELINE_CONFIG['kafka_input_topic']}'...")
        input_topic_result = check_kafka_topic.apply_async(
            args=[],
            kwargs={
                'bootstrap_server': FULL_PIPELINE_CONFIG['kafka_bootstrap'],
                'topic_name': FULL_PIPELINE_CONFIG['kafka_input_topic'],
            },
            queue=FULL_PIPELINE_CONFIG['kafka_queue']
        )
        
        # 2. Kiểm tra Kafka topic output
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
            input_topic_check = wait_for_celery_result(input_topic_result, timeout=60)
            output_topic_check = wait_for_celery_result(output_topic_result, timeout=60)
            
            # Kiểm tra và tạo topic input nếu chưa tồn tại
            if not input_topic_check.get('exists'):
                print(f"⚠️  Topic '{FULL_PIPELINE_CONFIG['kafka_input_topic']}' does not exist. Creating it...")
                create_kafka_topic_if_not_exists(
                    topic_name=FULL_PIPELINE_CONFIG['kafka_input_topic'],
                    bootstrap_server=FULL_PIPELINE_CONFIG['kafka_bootstrap'],
                    queue=FULL_PIPELINE_CONFIG['kafka_queue'],
                    partitions=FULL_PIPELINE_CONFIG['kafka_topic_partitions'],
                    replication_factor=FULL_PIPELINE_CONFIG['kafka_topic_replication_factor']
                )
            else:
                print(f"✅ Kafka topic '{FULL_PIPELINE_CONFIG['kafka_input_topic']}' exists")
            
            # Kiểm tra và tạo topic output nếu chưa tồn tại
            if not output_topic_check.get('exists'):
                print(f"⚠️  Topic '{FULL_PIPELINE_CONFIG['kafka_output_topic']}' does not exist. Creating it...")
                create_kafka_topic_if_not_exists(
                    topic_name=FULL_PIPELINE_CONFIG['kafka_output_topic'],
                    bootstrap_server=FULL_PIPELINE_CONFIG['kafka_bootstrap'],
                    queue=FULL_PIPELINE_CONFIG['kafka_queue'],
                    partitions=FULL_PIPELINE_CONFIG['kafka_topic_partitions'],
                    replication_factor=FULL_PIPELINE_CONFIG['kafka_topic_replication_factor']
                )
            else:
                print(f"✅ Kafka topic '{FULL_PIPELINE_CONFIG['kafka_output_topic']}' exists")
            
            print("✅ All Kafka topics checked/created successfully")
            return {
                'status': 'success',
                'input_topic_exists': True,
                'output_topic_exists': True,
            }
        except Exception as e:
            print(f"❌ Kafka topics check/creation failed: {str(e)}")
            raise Exception(f"Kafka topics check/creation failed: {str(e)}")

    # ========== PHASE 5: Start Predict Job ==========

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

    # ========== PHASE 6: Wait Before Streaming ==========
    
    def wait_before_streaming(**context):
        """Chờ một khoảng thời gian trước khi start streaming"""
        if not context['params'].get('start_streaming', True):
            return {'skipped': True}
        
        delay_seconds = context['params'].get('delay_before_streaming', 60)
        print(f"⏳ Waiting {delay_seconds} seconds before starting streaming...")
        time.sleep(delay_seconds)
        print(f"✅ Wait completed, ready to start streaming")
        return {'status': 'success', 'delay_seconds': delay_seconds}

    # ========== PHASE 7: Start Streaming Job ==========

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

    # Phase 2: Wait for Services Ready
    task_check_hadoop_ready = PythonOperator(
        task_id='check_hadoop_ready',
        python_callable=check_hadoop_ready,
        retries=2,
        retry_delay=10,
    )

    task_check_spark_ready = PythonOperator(
        task_id='check_spark_ready',
        python_callable=check_spark_ready,
        retries=2,
        retry_delay=10,
    )

    task_check_kafka_ready = PythonOperator(
        task_id='check_kafka_ready',
        python_callable=check_kafka_ready,
        retries=2,
        retry_delay=10,
    )

    # Phase 3: Train Model
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

    # Phase 4: Check Kafka Topics
    task_check_kafka_topics = PythonOperator(
        task_id='check_kafka_topics',
        python_callable=check_kafka_topics,
        retries=2,
        retry_delay=10,
    )

    task_start_predict = PythonOperator(
        task_id='start_predict',
        python_callable=start_predict,
        retries=0,
    )

    # Phase 6: Wait Before Streaming
    task_wait_before_streaming = PythonOperator(
        task_id='wait_before_streaming',
        python_callable=wait_before_streaming,
        retries=0,
    )

    # Phase 7: Start Streaming

    task_start_streaming = PythonOperator(
        task_id='start_streaming',
        python_callable=start_streaming,
        retries=0,
    )

    # ========== DEPENDENCIES ==========
    
    # Phase 1: Infrastructure Setup
    # Hadoop start first, then Spark Master, then Workers (parallel), then Kafka
    task_start_hadoop >> task_start_spark_master >> [task_start_spark_worker_1, task_start_spark_worker_2]
    [task_start_spark_worker_1, task_start_spark_worker_2] >> task_start_kafka
    
    # Phase 2: Wait for Services Ready
    # After all services started -> Check all services ready (parallel)
    # Wait for all infrastructure tasks to complete before checking
    # Note: Cannot use [list] >> [list] directly in Airflow, need to set each upstream task
    for upstream_task in [task_start_hadoop, task_start_spark_master, task_start_spark_worker_1, task_start_spark_worker_2, task_start_kafka]:
        upstream_task >> [task_check_hadoop_ready, task_check_spark_ready, task_check_kafka_ready]
    
    # Phase 3: Train Model
    # After services ready -> Train model -> Verify model saved
    [task_check_hadoop_ready, task_check_spark_ready, task_check_kafka_ready] >> task_train_model
    task_train_model >> task_verify_model_saved
    
    # Phase 4: Check Kafka Topics
    # After model verified -> Check Kafka topics (input + output)
    task_verify_model_saved >> task_check_kafka_topics
    
    # Phase 5: Start Predict Job
    # After topics checked -> Start predict job
    task_check_kafka_topics >> task_start_predict
    
    # Phase 6: Wait Before Streaming
    # After predict started -> Wait 60 seconds
    task_start_predict >> task_wait_before_streaming
    
    # Phase 7: Start Streaming
    # After wait -> Start streaming job
    task_wait_before_streaming >> task_start_streaming

