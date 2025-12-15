from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import os
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    run_command,
    docker_run,
    docker_stop,
    docker_remove,
    docker_ps,
    docker_compose_up,
    docker_compose_down,
    docker_compose_ps,
    docker_compose_logs
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


# ============== TASK FUNCTIONS ==============

def task_docker_run(**context):
    """Task chạy Docker container"""
    params = context['params']
    image = params.get('image', 'hello-world')
    container_name = params.get('container_name', None)
    ports = params.get('ports', None)

    # Parse ports nếu là string
    if ports and isinstance(ports, str) and ports.strip():
        ports = [p.strip() for p in ports.split(',')]
    else:
        ports = None

    result = docker_run.delay(image, container_name, ports)
    return {'task_id': result.id, 'image': image}


def task_docker_stop(**context):
    """Task dừng Docker container"""
    params = context['params']
    container_name = params.get('container_name', '')

    result = docker_stop.delay(container_name)
    return {'task_id': result.id, 'container_name': container_name}


def task_docker_remove(**context):
    """Task xóa Docker container"""
    params = context['params']
    container_name = params.get('container_name', '')

    result = docker_remove.delay(container_name)
    return {'task_id': result.id, 'container_name': container_name}


def task_docker_ps(**context):
    """Task liệt kê Docker containers"""
    params = context['params']
    all_containers = params.get('all_containers', False)

    result = docker_ps.delay(all_containers)
    return {'task_id': result.id}


def task_docker_compose_up(**context):
    """Task chạy docker-compose up"""
    params = context['params']
    path = params.get('compose_path', '/path/to/docker-compose.yml')
    services = params.get('services', None)
    detach = params.get('detach', True)
    build = params.get('build', False)
    force_recreate = params.get('force_recreate', False)

    # Parse services nếu là string
    if services and isinstance(services, str) and services.strip():
        services = [s.strip() for s in services.split(',')]
    else:
        services = None

    result = docker_compose_up.delay(path, services, detach, build, force_recreate)
    return {'task_id': result.id, 'compose_path': path, 'services': services}


def task_docker_compose_down(**context):
    """Task chạy docker-compose down"""
    params = context['params']
    path = params.get('compose_path', '/path/to/docker-compose.yml')
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


def task_docker_compose_ps(**context):
    """Task liệt kê containers của docker-compose"""
    params = context['params']
    path = params.get('compose_path', '/path/to/docker-compose.yml')

    result = docker_compose_ps.delay(path)
    return {'task_id': result.id, 'compose_path': path}


def task_docker_compose_logs(**context):
    """Task lấy logs từ docker-compose"""
    params = context['params']
    path = params.get('compose_path', '/path/to/docker-compose.yml')
    service = params.get('service', None)
    tail = params.get('tail', 100)

    if service and isinstance(service, str) and not service.strip():
        service = None

    result = docker_compose_logs.delay(path, service, tail)
    return {'task_id': result.id, 'compose_path': path}


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

    def prepare_data(**context):
        if not context['params'].get('start_spark', True):
            return {'skipped': True}
            
        command = "sh ~/bd/fp_pr_tasks/credit_card/exes/prepare.sh"
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

    prepare_data = PythonOperator(
        task_id='prepare_data',
        python_callable=prepare_data,
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
    [task_hadoop_datanode, task_spark_worker_1, task_spark_worker_2, task_kafka] >> prepare_data >> train_model >> predict		
    [task_hadoop_datanode, task_spark_worker_1, task_spark_worker_2, task_kafka] >> prepare_data >> train_model>> streaming_data 





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
