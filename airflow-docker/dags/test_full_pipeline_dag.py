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


# Cấu hình Pipeline
PIPELINE_CONFIG = {
    'hadoop_host': '192.168.80.52',
    'hadoop_queue': 'node_52',
    'hadoop_sbin_path': '~/hadoop/sbin',
    'spark_master': '192.168.80.52',
    'spark_master_queue': 'node_52',
    'spark_worker_1_host': '192.168.80.122',
    'spark_worker_1_queue': 'node_122',
    'spark_worker_2_host': '192.168.80.130',
    'spark_worker_2_queue': 'node_130',
    'spark_compose_path': '~/Documents/docker-spark/docker-compose.yml',
    'kafka_host': '192.168.80.122',
    'kafka_queue': 'node_122',
    'kafka_compose_path': '~/kafka-docker/docker-compose.yml',
    'train_script': '~/tai_thuy/train_model/train_model.py',
    'train_working_dir': '~/spark/bin',
    'train_input': 'hdfs://192.168.80.52:9000/data/train.csv',
    'train_output': 'hdfs://192.168.80.52:9000/model',
}


# ============== DAG: Test Full Pipeline ==============
with DAG(
    dag_id='test_full_pipeline',
    description='DAG test khởi động tất cả services và train model',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'full', 'pipeline'],
    params={
        'start_hadoop': Param(True, type='boolean', description='Khởi động Hadoop'),
        'start_spark': Param(True, type='boolean', description='Khởi động Spark'),
        'start_kafka': Param(True, type='boolean', description='Khởi động Kafka'),
        'train_model': Param(True, type='boolean', description='Training model'),
    }
) as dag_test_full_pipeline:

    def start_hadoop(**context):
        """Khởi động Hadoop cluster bằng script start-all.sh"""
        if not context['params'].get('start_hadoop', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Hadoop on {PIPELINE_CONFIG['hadoop_host']}...")
        
        command = f"cd {PIPELINE_CONFIG['hadoop_sbin_path']} && ./start-all.sh"
        
        result = run_command.apply_async(
            args=[command],
            kwargs={},
            queue=PIPELINE_CONFIG['hadoop_queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Hadoop started successfully on {PIPELINE_CONFIG['hadoop_host']}")
            print(f"Stdout: {output.get('stdout', '')}")
            if output.get('stderr'):
                print(f"Stderr: {output.get('stderr', '')}")
            
            if output.get('return_code') != 0:
                raise Exception(f"Hadoop start script returned non-zero code: {output.get('return_code')}")
            
            return {
                'task_id': result.id,
                'host': PIPELINE_CONFIG['hadoop_host'],
                'queue': PIPELINE_CONFIG['hadoop_queue'],
                'command': command,
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Hadoop on {PIPELINE_CONFIG['hadoop_host']}: {str(e)}")
            raise Exception(f"Failed to start Hadoop on {PIPELINE_CONFIG['hadoop_host']}: {str(e)}")

    def start_spark_master(**context):
        """Khởi động Spark Master"""
        if not context['params'].get('start_spark', True):
            print("⏭️ Skipping Spark Master")
            return {'skipped': True}
        
        print(f"🚀 Starting Spark Master on {PIPELINE_CONFIG['spark_master']}...")
        
        result = docker_compose_up.apply_async(
            args=[PIPELINE_CONFIG['spark_compose_path']],
            kwargs={
                'services': ['spark-master'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=PIPELINE_CONFIG['spark_master_queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Spark Master started successfully on {PIPELINE_CONFIG['spark_master']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'host': PIPELINE_CONFIG['spark_master'],
                'queue': PIPELINE_CONFIG['spark_master_queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Spark Master on {PIPELINE_CONFIG['spark_master']}: {str(e)}")
            raise Exception(f"Failed to start Spark Master on {PIPELINE_CONFIG['spark_master']}: {str(e)}")

    def start_spark_worker_1(**context):
        """Khởi động Spark Worker 1"""
        if not context['params'].get('start_spark', True):
            print("⏭️ Skipping Spark Worker 1")
            return {'skipped': True}
        
        print(f"🚀 Starting Spark Worker 1 on {PIPELINE_CONFIG['spark_worker_1_host']}...")
        
        result = docker_compose_up.apply_async(
            args=[PIPELINE_CONFIG['spark_compose_path']],
            kwargs={
                'services': ['spark-worker'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=PIPELINE_CONFIG['spark_worker_1_queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Spark Worker 1 started successfully on {PIPELINE_CONFIG['spark_worker_1_host']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'host': PIPELINE_CONFIG['spark_worker_1_host'],
                'queue': PIPELINE_CONFIG['spark_worker_1_queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Spark Worker 1 on {PIPELINE_CONFIG['spark_worker_1_host']}: {str(e)}")
            raise Exception(f"Failed to start Spark Worker 1 on {PIPELINE_CONFIG['spark_worker_1_host']}: {str(e)}")

    def start_spark_worker_2(**context):
        """Khởi động Spark Worker 2"""
        if not context['params'].get('start_spark', True):
            print("⏭️ Skipping Spark Worker 2")
            return {'skipped': True}
        
        print(f"🚀 Starting Spark Worker 2 on {PIPELINE_CONFIG['spark_worker_2_host']}...")
        
        result = docker_compose_up.apply_async(
            args=[PIPELINE_CONFIG['spark_compose_path']],
            kwargs={
                'services': ['spark-worker'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=PIPELINE_CONFIG['spark_worker_2_queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Spark Worker 2 started successfully on {PIPELINE_CONFIG['spark_worker_2_host']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'host': PIPELINE_CONFIG['spark_worker_2_host'],
                'queue': PIPELINE_CONFIG['spark_worker_2_queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Spark Worker 2 on {PIPELINE_CONFIG['spark_worker_2_host']}: {str(e)}")
            raise Exception(f"Failed to start Spark Worker 2 on {PIPELINE_CONFIG['spark_worker_2_host']}: {str(e)}")

    def start_kafka(**context):
        """Khởi động Kafka và Zookeeper"""
        if not context['params'].get('start_kafka', True):
            return {'skipped': True}
        
        print(f"🚀 Starting Kafka on {PIPELINE_CONFIG['kafka_host']}...")
        
        result = docker_compose_up.apply_async(
            args=[PIPELINE_CONFIG['kafka_compose_path']],
            kwargs={
                'services': ['kafka', 'zookeeper'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=PIPELINE_CONFIG['kafka_queue']
        )

        try:
            output = wait_for_celery_result(result, timeout=300)
            print(f"✅ Kafka started successfully on {PIPELINE_CONFIG['kafka_host']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'host': PIPELINE_CONFIG['kafka_host'],
                'queue': PIPELINE_CONFIG['kafka_queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start Kafka on {PIPELINE_CONFIG['kafka_host']}: {str(e)}")
            raise Exception(f"Failed to start Kafka on {PIPELINE_CONFIG['kafka_host']}: {str(e)}")

    def wait_for_services(**context):
        """Chờ một chút để services khởi động xong"""
        print("⏳ Waiting for services to be ready...")
        time.sleep(30)  # Chờ 30 giây để services khởi động
        print("✅ Services should be ready now")
        return {'status': 'success', 'message': 'Services ready'}

    def train_model(**context):
        """Training model từ HDFS và lưu về HDFS"""
        if not context['params'].get('train_model', True):
            return {'skipped': True}
        
        print(f"🚀 Starting model training...")
        print(f"Input: {PIPELINE_CONFIG['train_input']}")
        print(f"Output: {PIPELINE_CONFIG['train_output']}")
        
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
            args=[PIPELINE_CONFIG['train_script']],
            kwargs={
                'master_url': f"spark://{PIPELINE_CONFIG['spark_master']}:7077",
                'executor_memory': '4G',
                'driver_memory': '1G',
                'conf': spark_conf,
                'working_dir': PIPELINE_CONFIG['train_working_dir'],
                'env_vars': env_vars,
                'timeout': 1800  # 30 phút
            },
            queue=PIPELINE_CONFIG['spark_master_queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=1800)
            print(f"✅ Model training completed")
            print(f"Stdout: {output.get('stdout', '')}")
            if output.get('stderr'):
                print(f"Stderr: {output.get('stderr', '')}")
            
            if output.get('return_code') != 0:
                raise Exception(f"Training failed: {output.get('stderr', '')}")
            
            # Verify model was saved to HDFS
            print(f"🔍 Verifying model saved to HDFS: {PIPELINE_CONFIG['train_output']}")
            check_result = check_hdfs_path.apply_async(
                args=[PIPELINE_CONFIG['train_output']],
                queue=PIPELINE_CONFIG['hadoop_queue']
            )
            model_check = wait_for_celery_result(check_result, timeout=60)
            
            if not model_check.get('exists'):
                raise Exception(f"Model was not saved to {PIPELINE_CONFIG['train_output']} after training")
            
            print(f"✅ Model verified in HDFS: {PIPELINE_CONFIG['train_output']}")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'output': output,
                'model_path': PIPELINE_CONFIG['train_output'],
                'model_check': model_check
            }
        except Exception as e:
            print(f"❌ Failed to train model: {str(e)}")
            raise Exception(f"Failed to train model: {str(e)}")

    # Tasks
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

    task_train_model = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        retries=2,
        retry_delay=60,
    )

    # Dependencies
    # Start all services in parallel
    [task_start_hadoop, task_start_spark_master, task_start_spark_worker_1, 
     task_start_spark_worker_2, task_start_kafka] >> task_wait_services
    # After services ready, train model
    task_wait_services >> task_train_model

