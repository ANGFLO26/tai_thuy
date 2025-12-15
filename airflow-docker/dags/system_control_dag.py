from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    run_command,
    docker_compose_up,
    docker_compose_down,
    docker_compose_ps,
    docker_compose_logs,
    spark_submit,
    run_python_background,
    kill_process,
    check_service_status,
    check_hdfs_path,
    check_kafka_topic
)


def wait_for_celery_result(result, timeout=60, poll_interval=2):
    elapsed = 0
    while elapsed < timeout:
        if result.ready():
            if result.successful():
                return result.result
            else:
                # Task failed
                raise Exception(f"Celery task failed: {result.result}")
        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"Celery task {result.id} timed out after {timeout} seconds")


# ============== DAG 4: Docker Compose Stop ==============

with DAG(
    dag_id='docker_compose_stop',
    description='DAG dừng Docker Compose services',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['docker', 'compose', 'stop'],
    params={
        'compose_path': Param('~/bd/spark/docker-compose.yml', type='string', description='Đường dẫn file docker-compose.yml'),
        'services': Param('', type='string', description='Services cần dừng (vd: spark-master,spark-worker). Để trống = tất cả'),
        'remove_volumes': Param(False, type='boolean', description='Xóa volumes'),
        'remove_orphans': Param(False, type='boolean', description='Xóa orphan containers'),
    }
) as dag_compose_stop:

    def task_compose_stop(**context):
        """Dừng docker-compose services"""
        params = context['params']
        path = params.get('compose_path')
        services = params.get('services', None)
        volumes = params.get('remove_volumes', False)
        remove_orphans = params.get('remove_orphans', False)

        # Parse services nếu là string
        if services and isinstance(services, str) and services.strip():
            services = [s.strip() for s in services.split(',')]
        else:
            services = None

        result = docker_compose_down.delay(path, services, volumes, remove_orphans)
        return {'task_id': result.id, 'compose_path': path, 'services': services}

    stop_compose_task = PythonOperator(
        task_id='stop_docker_compose',
        python_callable=task_compose_stop,
    )


# ============== DAG 5: Big Data Pipeline ==============
# Pipeline chạy trên nhiều node khác nhau

# Định nghĩa cấu hình các services
BIGDATA_SERVICES = {
    'spark-master': {
        'host': '192.168.80.52',
        'queue': 'node_52',
        'path': '~/bd/spark/docker-compose.yml',
        'service': 'spark-master',
    },
    'spark-worker-1': {
        'host': '192.168.80.122',
        'queue': 'node_122',
        'path': '~/bd/spark/docker-compose.yml',
        'service': 'spark-worker',
    },
    'spark-worker-2': {
        'host': '192.168.80.130',
        'queue': 'node_130',
        'path': '~/bd/spark/docker-compose.yml',
        'service': 'spark-worker',
    },
    'hadoop-namenode': {
        'host': '192.168.80.52',
        'queue': 'node_52',
        'path': '~/bd/hadoop/docker-compose.namenode.yml',
        'service': None,  # Chạy tất cả services trong file
    },
    'hadoop-datanode': {
        'host': '192.168.80.52',
        'queue': 'node_52',
        'path': '~/bd/hadoop/docker-compose.datanode.yml',
        'service': None,
    },
    'kafka': {
        'host': '192.168.80.122',
        'queue': 'node_122',
        'path': '~/bd/kafka/docker-compose.yml',
        'service': None,
    },
}


with DAG(
    dag_id='bigdata_pipeline_start',
    description='full path from start dockers to submit to Spark',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bigdata', 'pipeline', 'start'],
    params={
        'start_hadoop': Param(True, type='boolean', description='Khởi động Hadoop (Namenode + Datanode)'),
        'start_spark': Param(True, type='boolean', description='Khởi động Spark (Master + Worker)'),
        'start_kafka': Param(True, type='boolean', description='Khởi động Kafka'),
    }
) as dag_bigdata_start:

    def start_service(service_name, timeout=300, **context):
        config = BIGDATA_SERVICES[service_name]

        result = docker_compose_up.apply_async(
            args=[config['path']],
            kwargs={
                'services': config['service'],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=config['queue']
        )

        # Chờ kết quả từ Celery worker bằng polling
        try:
            output = wait_for_celery_result(result, timeout=timeout)
            return {
                'task_id': result.id,
                'service': service_name,
                'host': config['host'],
                'queue': config['queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            raise Exception(f"Failed to start {service_name} on {config['host']}: {str(e)}")

    # Hadoop tasks
    def start_hadoop_namenode(**context):
        if not context['params'].get('start_hadoop', True):
            return {'skipped': True}
        return start_service('hadoop-namenode', **context)

    def start_hadoop_datanode(**context):
        if not context['params'].get('start_hadoop', True):
            return {'skipped': True}
        return start_service('hadoop-datanode', **context)

    # Spark tasks
    def start_spark_master(**context):
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
        return start_service('spark-master', **context)

    def start_spark_worker_1(**context):
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
        return start_service('spark-worker-1', **context)

    def start_spark_worker_2(**context):
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
        return start_service('spark-worker-2', **context)

    # Kafka task
    def start_kafka(**context):
        if not context['params'].get('start_kafka', True):
            return {'skipped': True}
        return start_service('kafka', **context)

    def train_model(**context):
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
            
        command = "sh ~/bd/fp_pr_tasks/credit_card/exes/train.sh"
        # Spark master info
        queue = 'node_52' 
        host = '192.168.80.52'
        
        # Set JAVA_HOME to Java 17 for Spark
        env_vars = {
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64',
            'PATH': '/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
        }
        
        result = run_command.apply_async(
            args=[command],
            kwargs={'env_vars': env_vars},
            queue=queue
        )
        
        try:
            output = wait_for_celery_result(result, timeout=600) # Spark jobs might take longer
            return {
                'task_id': result.id,
                'command': command,
                'host': host,
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            raise Exception(f"Failed to submit spark job on {host}: {str(e)}")
    
    def streaming_data(**context):
        if not context['params'].get('start_spark', True):
            return {'skipped': True}

        command = 'sh ~/bd/fp_pr_tasks/credit_card/exes/producer.sh'
        # Spark master info
        queue = 'node_52' 
        host = '192.168.80.52'
        
        # Set JAVA_HOME to Java 17 for Spark
        env_vars = {
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64',
            'PATH': '/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
        }
        
        result = run_command.apply_async(
            args=[command],
            kwargs={'env_vars': env_vars},
            queue=queue
        )
        
        try:
            output = wait_for_celery_result(result, timeout=600) # Spark jobs might take longer
            return {
                'task_id': result.id,
                'command': command,
                'host': host,
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            raise Exception(f"Failed to submit spark job on {host}: {str(e)}")
    
    def predict(**context):
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
            
        command = 'sh ~/bd/fp_pr_tasks/credit_card/exes/predict.sh'
        # Spark master info
        queue = 'node_52' 
        host = '192.168.80.52'
        
        # Set JAVA_HOME to Java 17 for Spark
        env_vars = {
            'JAVA_HOME': '/usr/lib/jvm/java-17-openjdk-amd64',
            'PATH': '/usr/lib/jvm/java-17-openjdk-amd64/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
        }
        
        result = run_command.apply_async(
            args=[command],
            kwargs={'env_vars': env_vars},
            queue=queue
        )
        
        try:
            output = wait_for_celery_result(result, timeout=600) # Spark jobs might take longer
            return {
                'task_id': result.id,
                'command': command,
                'host': host,
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            raise Exception(f"Failed to submit spark job on {host}: {str(e)}")

    # --------- Tạo tasks -------------

    task_hadoop_namenode = PythonOperator(
        task_id='start_hadoop_namenode',
        python_callable=start_hadoop_namenode,
    )

    task_hadoop_datanode = PythonOperator(
        task_id='start_hadoop_datanode',
        python_callable=start_hadoop_datanode,
    )

    task_spark_master = PythonOperator(
        task_id='start_spark_master',
        python_callable=start_spark_master,
    )

    task_spark_worker_1 = PythonOperator(
        task_id='start_spark_worker_1',
        python_callable=start_spark_worker_1,
    )

    task_spark_worker_2 = PythonOperator(
        task_id='start_spark_worker_2',
        python_callable=start_spark_worker_2,
    )

    task_kafka = PythonOperator(
        task_id='start_kafka',
        python_callable=start_kafka,
    )

    train_model = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    streaming_data = PythonOperator(
        task_id='streaming_data',
        python_callable=streaming_data,
    )

    predict = PythonOperator(
        task_id='predict',
        python_callable=predict,
    )

    # ----------- ----------- -------------

    # hadoop
    task_hadoop_namenode >> task_hadoop_datanode

    # spark
    task_spark_master >> [task_spark_worker_1, task_spark_worker_2]

    # kafka
    task_kafka

    # full
    [task_hadoop_datanode, task_spark_worker_1, task_spark_worker_2, task_kafka] >> train_model >> predict		
    [task_hadoop_datanode, task_spark_worker_1, task_spark_worker_2, task_kafka] >> train_model >> streaming_data 





# ============== DAG 6: Big Data Pipeline Stop ==============
with DAG(
    dag_id='bigdata_pipeline_stop',
    description='Pipeline dừng Big Data cluster',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bigdata', 'pipeline', 'stop'],
    params={
        'stop_hadoop': Param(True, type='boolean', description='Dừng Hadoop'),
        'stop_spark': Param(True, type='boolean', description='Dừng Spark'),
        'stop_kafka': Param(True, type='boolean', description='Dừng Kafka'),
        'remove_volumes': Param(False, type='boolean', description='Xóa volumes'),
    }
) as dag_bigdata_stop:

    def stop_service(service_name, remove_volumes=False, timeout=300, **context):
        """Dừng một service trên node tương ứng và chờ kết quả"""
        config = BIGDATA_SERVICES[service_name]

        result = docker_compose_down.apply_async(
            args=[config['path']],
            kwargs={
                'services': config['service'],
                'volumes': remove_volumes,
                'remove_orphans': False,
            },
            queue=config['queue']
        )

        # Chờ kết quả từ Celery worker bằng polling
        try:
            output = wait_for_celery_result(result, timeout=timeout)
            return {
                'task_id': result.id,
                'service': service_name,
                'host': config['host'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            raise Exception(f"Failed to stop {service_name} on {config['host']}: {str(e)}")

    # Stop tasks
    def stop_kafka(**context):
        if not context['params'].get('stop_kafka', True):
            return {'skipped': True}
        return stop_service('kafka', context['params'].get('remove_volumes', False), **context)

    def stop_spark_worker_1(**context):
        if not context['params'].get('stop_spark', True):
            return {'skipped': True}
        return stop_service('spark-worker-1', context['params'].get('remove_volumes', False), **context)

    def stop_spark_worker_2(**context):
        if not context['params'].get('stop_spark', True):
            return {'skipped': True}
        return stop_service('spark-worker-2', context['params'].get('remove_volumes', False), **context)

    def stop_spark_master(**context):
        if not context['params'].get('stop_spark', True):
            return {'skipped': True}
        return stop_service('spark-master', context['params'].get('remove_volumes', False), **context)

    def stop_hadoop_datanode(**context):
        if not context['params'].get('stop_hadoop', True):
            return {'skipped': True}
        return stop_service('hadoop-datanode', context['params'].get('remove_volumes', False), **context)

    def stop_hadoop_namenode(**context):
        if not context['params'].get('stop_hadoop', True):
            return {'skipped': True}
        return stop_service('hadoop-namenode', context['params'].get('remove_volumes', False), **context)

    # Tạo tasks
    task_stop_kafka = PythonOperator(
        task_id='stop_kafka',
        python_callable=stop_kafka,
    )

    task_stop_spark_worker_1 = PythonOperator(
        task_id='stop_spark_worker_1',
        python_callable=stop_spark_worker_1,
    )

    task_stop_spark_worker_2 = PythonOperator(
        task_id='stop_spark_worker_2',
        python_callable=stop_spark_worker_2,
    )

    task_stop_spark_master = PythonOperator(
        task_id='stop_spark_master',
        python_callable=stop_spark_master,
    )

    task_stop_hadoop_datanode = PythonOperator(
        task_id='stop_hadoop_datanode',
        python_callable=stop_hadoop_datanode,
    )

    task_stop_hadoop_namenode = PythonOperator(
        task_id='stop_hadoop_namenode',
        python_callable=stop_hadoop_namenode,
    )

    # Cấu hình dependency dừng services (tách biệt các cụm)
    # 1. Hadoop cluster: Datanode -> Namenode (Dừng Data trước)
    task_stop_hadoop_datanode >> task_stop_hadoop_namenode

    # 2. Spark cluster: Workers -> Master (Dừng Workers trước)
    [task_stop_spark_worker_1, task_stop_spark_worker_2] >> task_stop_spark_master

    # 3. Kafka: Độc lập
    task_stop_kafka


# ============== DAG 7: Big Data Pipeline Start (New) ==============
with DAG(
    dag_id='bigdata_pipeline_start_new',
    description='Pipeline khởi động infrastructure, training model và setup Kafka topics',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bigdata', 'pipeline', 'start', 'new'],
    params={
        'start_hadoop': Param(True, type='boolean', description='Khởi động Hadoop'),
        'start_spark': Param(True, type='boolean', description='Khởi động Spark'),
        'start_kafka': Param(True, type='boolean', description='Khởi động Kafka'),
        'train_model': Param(True, type='boolean', description='Training model'),
    }
) as dag_bigdata_start_new:

    # Cấu hình
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
        'kafka_bootstrap': '192.168.80.122:9092',
        'train_script': '~/tai_thuy/train_model/train_model.py',
        'train_working_dir': '~/spark/bin',
        'train_input': 'hdfs://192.168.80.52:9000/data/train.csv',
        'train_output': 'hdfs://192.168.80.52:9000/model',
    }

    def start_hadoop(**context):
        """Khởi động Hadoop"""
        if not context['params'].get('start_hadoop', True):
            return {'skipped': True}
        
        command = f"cd {PIPELINE_CONFIG['hadoop_sbin_path']} && ./start-all.sh"
        result = run_command.apply_async(
            args=[command],
            kwargs={'timeout': 300},
            queue=PIPELINE_CONFIG['hadoop_queue']
        )
        output = wait_for_celery_result(result, timeout=300)
        if output.get('return_code') != 0:
            raise Exception(f"Hadoop start failed: {output.get('stderr')}")
        return {'status': 'success', 'output': output}

    def start_spark_master(**context):
        """Khởi động Spark Master"""
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
        
        result = docker_compose_up.apply_async(
            args=[PIPELINE_CONFIG['spark_compose_path']],
            kwargs={'services': ['spark-master'], 'detach': True},
            queue=PIPELINE_CONFIG['spark_master_queue']
        )
        output = wait_for_celery_result(result, timeout=300)
        return {'status': 'success', 'output': output}

    def start_spark_worker_1(**context):
        """Khởi động Spark Worker 1"""
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
        
        result = docker_compose_up.apply_async(
            args=[PIPELINE_CONFIG['spark_compose_path']],
            kwargs={'services': ['spark-worker'], 'detach': True},
            queue=PIPELINE_CONFIG['spark_worker_1_queue']
        )
        output = wait_for_celery_result(result, timeout=300)
        return {'status': 'success', 'output': output}

    def start_spark_worker_2(**context):
        """Khởi động Spark Worker 2"""
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
        
        result = docker_compose_up.apply_async(
            args=[PIPELINE_CONFIG['spark_compose_path']],
            kwargs={'services': ['spark-worker'], 'detach': True},
            queue=PIPELINE_CONFIG['spark_worker_2_queue']
        )
        output = wait_for_celery_result(result, timeout=300)
        return {'status': 'success', 'output': output}

    def start_kafka(**context):
        """Khởi động Kafka"""
        if not context['params'].get('start_kafka', True):
            return {'skipped': True}
        
        result = docker_compose_up.apply_async(
            args=[PIPELINE_CONFIG['kafka_compose_path']],
            kwargs={'services': ['kafka', 'zookeeper'], 'detach': True},
            queue=PIPELINE_CONFIG['kafka_queue']
        )
        output = wait_for_celery_result(result, timeout=300)
        return {'status': 'success', 'output': output}

    def wait_for_services_ready(**context):
        """Chờ tất cả services sẵn sàng"""
        print("🔍 Checking services status...")
        
        # Check Hadoop
        result_hadoop = check_service_status.apply_async(
            args=['hadoop'],
            queue=PIPELINE_CONFIG['hadoop_queue']
        )
        hadoop_status = wait_for_celery_result(result_hadoop, timeout=60)
        
        # Check Spark
        result_spark = check_service_status.apply_async(
            args=['spark'],
            kwargs={'host': PIPELINE_CONFIG['spark_master']},
            queue=PIPELINE_CONFIG['spark_master_queue']
        )
        spark_status = wait_for_celery_result(result_spark, timeout=60)
        
        # Check Kafka
        result_kafka = check_service_status.apply_async(
            args=['kafka'],
            queue=PIPELINE_CONFIG['kafka_queue']
        )
        kafka_status = wait_for_celery_result(result_kafka, timeout=60)
        
        if not hadoop_status.get('ready'):
            raise Exception("Hadoop is not ready")
        if not spark_status.get('ready'):
            raise Exception("Spark is not ready")
        if not kafka_status.get('ready'):
            raise Exception("Kafka is not ready")
        
        print("✅ All services are ready")
        return {
            'hadoop': hadoop_status,
            'spark': spark_status,
            'kafka': kafka_status
        }

    def train_model(**context):
        """Training model từ HDFS"""
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
        
        output = wait_for_celery_result(result, timeout=1800)
        if output.get('return_code') != 0:
            raise Exception(f"Training failed: {output.get('stderr')}")
        
        # Verify model was saved
        check_result = check_hdfs_path.apply_async(
            args=[PIPELINE_CONFIG['train_output']],
            queue=PIPELINE_CONFIG['hadoop_queue']
        )
        model_check = wait_for_celery_result(check_result, timeout=60)
        
        if not model_check.get('exists'):
            raise Exception(f"Model was not saved to {PIPELINE_CONFIG['train_output']}")
        
        print("✅ Model training completed and saved")
        return {'status': 'success', 'output': output, 'model_path': PIPELINE_CONFIG['train_output']}

    def create_kafka_topic_input(**context):
        """Tạo Kafka topic input"""
        command = (
            f"docker exec -i kafka kafka-topics "
            f"--create "
            f"--topic input "
            f"--bootstrap-server {PIPELINE_CONFIG['kafka_bootstrap']} "
            f"--partitions 1 "
            f"--replication-factor 1"
        )
        
        result = run_command.apply_async(
            args=[command],
            kwargs={'timeout': 60},
            queue=PIPELINE_CONFIG['kafka_queue']
        )
        output = wait_for_celery_result(result, timeout=60)
        
        # Check if topic exists (ignore if already exists)
        if output.get('return_code') != 0 and 'already exists' not in output.get('stderr', '').lower():
            raise Exception(f"Failed to create topic input: {output.get('stderr')}")
        
        return {'status': 'success', 'output': output}

    def create_kafka_topic_output(**context):
        """Tạo Kafka topic output"""
        command = (
            f"docker exec -i kafka kafka-topics "
            f"--create "
            f"--topic output "
            f"--bootstrap-server {PIPELINE_CONFIG['kafka_bootstrap']} "
            f"--partitions 1 "
            f"--replication-factor 1"
        )
        
        result = run_command.apply_async(
            args=[command],
            kwargs={'timeout': 60},
            queue=PIPELINE_CONFIG['kafka_queue']
        )
        output = wait_for_celery_result(result, timeout=60)
        
        if output.get('return_code') != 0 and 'already exists' not in output.get('stderr', '').lower():
            raise Exception(f"Failed to create topic output: {output.get('stderr')}")
        
        return {'status': 'success', 'output': output}

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
        task_id='wait_for_services_ready',
        python_callable=wait_for_services_ready,
        retries=3,
        retry_delay=30,
    )

    task_train_model = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        retries=2,
        retry_delay=60,
    )

    task_create_topic_input = PythonOperator(
        task_id='create_kafka_topic_input',
        python_callable=create_kafka_topic_input,
        retries=2,
        retry_delay=10,
    )

    task_create_topic_output = PythonOperator(
        task_id='create_kafka_topic_output',
        python_callable=create_kafka_topic_output,
        retries=2,
        retry_delay=10,
    )

    # Dependencies
    [task_start_hadoop, task_start_spark_master, task_start_spark_worker_1, 
     task_start_spark_worker_2, task_start_kafka] >> task_wait_services
    task_wait_services >> task_train_model
    task_train_model >> [task_create_topic_input, task_create_topic_output]


# ============== DAG 8: Big Data Pipeline Streaming ==============
with DAG(
    dag_id='bigdata_pipeline_streaming',
    description='Pipeline streaming data, predict và hiển thị UI',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bigdata', 'pipeline', 'streaming', 'new'],
    params={
        'start_streaming': Param(True, type='boolean', description='Start streaming'),
        'start_predict': Param(True, type='boolean', description='Start predict'),
        'start_ui': Param(True, type='boolean', description='Start UI'),
    }
) as dag_bigdata_streaming:

    PIPELINE_CONFIG = {
        'node_52_queue': 'node_52',
        'spark_master': '192.168.80.52',
        'kafka_bootstrap': '192.168.80.122:9092',
        'model_path': 'hdfs://192.168.80.52:9000/model',
        'streaming_script': '~/tai_thuy/streaming/kafka_streaming.py',
        'streaming_working_dir': '~/tai_thuy/streaming',
        'predict_script': '~/tai_thuy/predict/predict_fraud.py',
        'predict_working_dir': '~/spark/bin',
        'ui_script': '~/tai_thuy/ui/server.py',
        'ui_working_dir': '~/tai_thuy/ui',
    }

    def check_prerequisites(**context):
        """Kiểm tra prerequisites"""
        print("🔍 Checking prerequisites...")
        
        # Check Kafka topics (chạy trên node có Kafka)
        result_input = check_kafka_topic.apply_async(
            args=[PIPELINE_CONFIG['kafka_bootstrap'], 'input'],
            queue='node_122'  # Kafka ở node_122
        )
        result_output = check_kafka_topic.apply_async(
            args=[PIPELINE_CONFIG['kafka_bootstrap'], 'output'],
            queue='node_122'  # Kafka ở node_122
        )
        
        input_exists = wait_for_celery_result(result_input, timeout=60)
        output_exists = wait_for_celery_result(result_output, timeout=60)
        
        if not input_exists.get('exists'):
            raise Exception("Kafka topic 'input' does not exist")
        if not output_exists.get('exists'):
            raise Exception("Kafka topic 'output' does not exist")
        
        # Check model exists
        result_model = check_hdfs_path.apply_async(
            args=[PIPELINE_CONFIG['model_path']],
            queue=PIPELINE_CONFIG['node_52_queue']
        )
        model_exists = wait_for_celery_result(result_model, timeout=60)
        
        if not model_exists.get('exists'):
            raise Exception(f"Model not found at {PIPELINE_CONFIG['model_path']}")
        
        print("✅ All prerequisites met")
        return {
            'input_topic': input_exists,
            'output_topic': output_exists,
            'model': model_exists
        }

    def start_streaming(**context):
        """Start streaming process"""
        if not context['params'].get('start_streaming', True):
            return {'skipped': True}
        
        print("🚀 Starting streaming process...")
        
        result = run_python_background.apply_async(
            args=[PIPELINE_CONFIG['streaming_script']],
            kwargs={
                'working_dir': PIPELINE_CONFIG['streaming_working_dir'],
                'pid_file': '/tmp/streaming.pid',
                'log_file': '/tmp/streaming.log'
            },
            queue=PIPELINE_CONFIG['node_52_queue']
        )
        
        output = wait_for_celery_result(result, timeout=60)
        print(f"✅ Streaming started with PID: {output.get('pid')}")
        return {'status': 'success', 'pid': output.get('pid'), 'pid_file': output.get('pid_file')}

    def start_predict(**context):
        """Start predict Spark job"""
        if not context['params'].get('start_predict', True):
            return {'skipped': True}
        
        print("🚀 Starting predict Spark job...")
        
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
            args=[PIPELINE_CONFIG['predict_script']],
            kwargs={
                'master_url': f"spark://{PIPELINE_CONFIG['spark_master']}:7077",
                'executor_memory': '4G',
                'driver_memory': '1G',
                'packages': ['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1'],
                'conf': spark_conf,
                'working_dir': PIPELINE_CONFIG['predict_working_dir'],
                'env_vars': env_vars,
                'detach': True,  # Chạy ở background vì là streaming job
                'pid_file': '/tmp/predict.pid',
                'log_file': '/tmp/predict.log'
            },
            queue=PIPELINE_CONFIG['node_52_queue']
        )
        
        # Note: Spark streaming job sẽ chạy liên tục ở background
        output = wait_for_celery_result(result, timeout=120)  # 2 phút để start job
        print(f"✅ Predict Spark job started with PID: {output.get('pid')}")
        return {'status': 'success', 'pid': output.get('pid'), 'pid_file': output.get('pid_file'), 'output': output}

    def start_ui(**context):
        """Start UI server"""
        if not context['params'].get('start_ui', True):
            return {'skipped': True}
        
        print("🚀 Starting UI server...")
        
        result = run_python_background.apply_async(
            args=[PIPELINE_CONFIG['ui_script']],
            kwargs={
                'working_dir': PIPELINE_CONFIG['ui_working_dir'],
                'pid_file': '/tmp/ui.pid',
                'log_file': '/tmp/ui.log'
            },
            queue=PIPELINE_CONFIG['node_52_queue']
        )
        
        output = wait_for_celery_result(result, timeout=60)
        print(f"✅ UI server started with PID: {output.get('pid')}")
        return {'status': 'success', 'pid': output.get('pid'), 'pid_file': output.get('pid_file')}

    # Tasks
    task_check_prerequisites = PythonOperator(
        task_id='check_prerequisites',
        python_callable=check_prerequisites,
        retries=3,
        retry_delay=20,
    )

    task_start_streaming = PythonOperator(
        task_id='start_streaming',
        python_callable=start_streaming,
        retries=0,
    )

    task_start_predict = PythonOperator(
        task_id='start_predict',
        python_callable=start_predict,
        retries=0,
    )

    task_start_ui = PythonOperator(
        task_id='start_ui',
        python_callable=start_ui,
        retries=0,
    )

    # Dependencies
    task_check_prerequisites >> [task_start_streaming, task_start_predict, task_start_ui]


# ============== DAG 9: Big Data Pipeline Stop (New) ==============
with DAG(
    dag_id='bigdata_pipeline_stop_new',
    description='Pipeline dừng tất cả services và processes',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['bigdata', 'pipeline', 'stop', 'new'],
    params={
        'stop_streaming': Param(True, type='boolean', description='Dừng streaming'),
        'stop_predict': Param(True, type='boolean', description='Dừng predict'),
        'stop_ui': Param(True, type='boolean', description='Dừng UI'),
        'stop_kafka': Param(True, type='boolean', description='Dừng Kafka'),
        'stop_spark': Param(True, type='boolean', description='Dừng Spark'),
        'stop_hadoop': Param(True, type='boolean', description='Dừng Hadoop'),
    }
) as dag_bigdata_stop_new:

    PIPELINE_CONFIG = {
        'node_52_queue': 'node_52',
        'node_122_queue': 'node_122',
        'node_130_queue': 'node_130',
        'hadoop_sbin_path': '~/hadoop/sbin',
        'spark_compose_path': '~/Documents/docker-spark/docker-compose.yml',
        'kafka_compose_path': '~/kafka-docker/docker-compose.yml',
    }

    def stop_streaming(**context):
        """Dừng streaming process"""
        if not context['params'].get('stop_streaming', True):
            return {'skipped': True}
        
        print("🛑 Stopping streaming process...")
        
        result = kill_process.apply_async(
            kwargs={
                'pid_file': '/tmp/streaming.pid',
                'process_name': 'kafka_streaming.py',
                'signal_type': 'TERM'
            },
            queue=PIPELINE_CONFIG['node_52_queue']
        )
        
        output = wait_for_celery_result(result, timeout=60)
        print(f"✅ Streaming stopped: {output.get('killed_pids')}")
        return {'status': 'success', 'output': output}

    def stop_predict(**context):
        """Dừng predict Spark job"""
        if not context['params'].get('stop_predict', True):
            return {'skipped': True}
        
        print("🛑 Stopping predict Spark job...")
        
        # Kill Spark job by PID file hoặc process name
        result = kill_process.apply_async(
            kwargs={
                'pid_file': '/tmp/predict.pid',
                'process_name': 'predict_fraud.py',
                'signal_type': 'TERM'
            },
            queue=PIPELINE_CONFIG['node_52_queue']
        )
        
        output = wait_for_celery_result(result, timeout=60)
        print(f"✅ Predict job stopped: {output.get('killed_pids')}")
        return {'status': 'success', 'output': output}

    def stop_ui(**context):
        """Dừng UI server"""
        if not context['params'].get('stop_ui', True):
            return {'skipped': True}
        
        print("🛑 Stopping UI server...")
        
        result = kill_process.apply_async(
            kwargs={
                'pid_file': '/tmp/ui.pid',
                'process_name': 'server.py',
                'signal_type': 'TERM'
            },
            queue=PIPELINE_CONFIG['node_52_queue']
        )
        
        output = wait_for_celery_result(result, timeout=60)
        print(f"✅ UI server stopped: {output.get('killed_pids')}")
        return {'status': 'success', 'output': output}

    def stop_kafka(**context):
        """Dừng Kafka"""
        if not context['params'].get('stop_kafka', True):
            return {'skipped': True}
        
        command = f"cd ~/kafka-docker && docker compose stop kafka zookeeper"
        result = run_command.apply_async(
            args=[command],
            kwargs={'timeout': 300},
            queue=PIPELINE_CONFIG['node_122_queue']
        )
        output = wait_for_celery_result(result, timeout=300)
        return {'status': 'success', 'output': output}

    def stop_spark_workers(**context):
        """Dừng Spark Workers"""
        if not context['params'].get('stop_spark', True):
            return {'skipped': True}
        
        result_1 = docker_compose_down.apply_async(
            args=[PIPELINE_CONFIG['spark_compose_path']],
            kwargs={'services': ['spark-worker'], 'volumes': False},
            queue=PIPELINE_CONFIG['node_122_queue']
        )
        result_2 = docker_compose_down.apply_async(
            args=[PIPELINE_CONFIG['spark_compose_path']],
            kwargs={'services': ['spark-worker'], 'volumes': False},
            queue=PIPELINE_CONFIG['node_130_queue']
        )
        
        output_1 = wait_for_celery_result(result_1, timeout=300)
        output_2 = wait_for_celery_result(result_2, timeout=300)
        return {'status': 'success', 'worker_1': output_1, 'worker_2': output_2}

    def stop_spark_master(**context):
        """Dừng Spark Master"""
        if not context['params'].get('stop_spark', True):
            return {'skipped': True}
        
        result = docker_compose_down.apply_async(
            args=[PIPELINE_CONFIG['spark_compose_path']],
            kwargs={'services': ['spark-master'], 'volumes': False},
            queue=PIPELINE_CONFIG['node_52_queue']
        )
        output = wait_for_celery_result(result, timeout=300)
        return {'status': 'success', 'output': output}

    def stop_hadoop(**context):
        """Dừng Hadoop"""
        if not context['params'].get('stop_hadoop', True):
            return {'skipped': True}
        
        command = f"cd {PIPELINE_CONFIG['hadoop_sbin_path']} && ./stop-all.sh"
        result = run_command.apply_async(
            args=[command],
            kwargs={'timeout': 300},
            queue=PIPELINE_CONFIG['node_52_queue']
        )
        output = wait_for_celery_result(result, timeout=300)
        return {'status': 'success', 'output': output}

    # Tasks
    task_stop_streaming = PythonOperator(
        task_id='stop_streaming',
        python_callable=stop_streaming,
        retries=0,
    )

    task_stop_predict = PythonOperator(
        task_id='stop_predict',
        python_callable=stop_predict,
        retries=0,
    )

    task_stop_ui = PythonOperator(
        task_id='stop_ui',
        python_callable=stop_ui,
        retries=0,
    )

    task_stop_kafka = PythonOperator(
        task_id='stop_kafka',
        python_callable=stop_kafka,
        retries=0,
    )

    task_stop_spark_workers = PythonOperator(
        task_id='stop_spark_workers',
        python_callable=stop_spark_workers,
        retries=0,
    )

    task_stop_spark_master = PythonOperator(
        task_id='stop_spark_master',
        python_callable=stop_spark_master,
        retries=0,
    )

    task_stop_hadoop = PythonOperator(
        task_id='stop_hadoop',
        python_callable=stop_hadoop,
        retries=0,
    )

    # Dependencies
    [task_stop_streaming, task_stop_predict, task_stop_ui] >> task_stop_kafka
    task_stop_kafka >> task_stop_spark_workers
    task_stop_spark_workers >> task_stop_spark_master
    task_stop_spark_master >> task_stop_hadoop
