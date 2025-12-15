from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    docker_compose_up,
    docker_compose_down,
    docker_compose_ps
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


# Cấu hình Spark services
SPARK_SERVICES = {
    'spark-master': {
        'host': '192.168.80.52',
        'queue': 'node_52',
        'path': '~/Documents/docker-spark/docker-compose.yml',
        'service': 'spark-master',
    },
    'spark-worker-1': {
        'host': '192.168.80.122',
        'queue': 'node_122',
        'path': '~/Documents/docker-spark/docker-compose.yml',
        'service': 'spark-worker',
    },
    'spark-worker-2': {
        'host': '192.168.80.130',
        'queue': 'node_130',
        'path': '~/Documents/docker-spark/docker-compose.yml',
        'service': 'spark-worker',
    },
}


# ============== DAG: Test Spark Start ==============
with DAG(
    dag_id='test_spark_start',
    description='DAG test khởi động Spark cluster (Master + Worker)',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'spark', 'start'],
    params={
        'start_master': Param(True, type='boolean', description='Khởi động Spark Master'),
        'start_worker_1': Param(True, type='boolean', description='Khởi động Spark Worker 1 (192.168.80.122)'),
        'start_worker_2': Param(True, type='boolean', description='Khởi động Spark Worker 2 (192.168.80.130)'),
    }
) as dag_test_spark_start:

    def start_service(service_name, timeout=300, **context):
        """Khởi động một Spark service"""
        config = SPARK_SERVICES[service_name]

        result = docker_compose_up.apply_async(
            args=[config['path']],
            kwargs={
                'services': [config['service']],
                'detach': True,
                'build': False,
                'force_recreate': False,
            },
            queue=config['queue']
        )

        # Chờ kết quả từ Celery worker
        try:
            output = wait_for_celery_result(result, timeout=timeout)
            print(f"✅ {service_name} started successfully on {config['host']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'service': service_name,
                'host': config['host'],
                'queue': config['queue'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to start {service_name} on {config['host']}: {str(e)}")
            raise Exception(f"Failed to start {service_name} on {config['host']}: {str(e)}")

    def start_spark_master(**context):
        """Khởi động Spark Master"""
        if not context['params'].get('start_master', True):
            print("⏭️ Skipping Spark Master")
            return {'skipped': True}
        return start_service('spark-master', **context)

    def start_spark_worker_1(**context):
        """Khởi động Spark Worker 1"""
        if not context['params'].get('start_worker_1', True):
            print("⏭️ Skipping Spark Worker 1")
            return {'skipped': True}
        return start_service('spark-worker-1', **context)

    def start_spark_worker_2(**context):
        """Khởi động Spark Worker 2"""
        if not context['params'].get('start_worker_2', True):
            print("⏭️ Skipping Spark Worker 2")
            return {'skipped': True}
        return start_service('spark-worker-2', **context)

    def check_spark_status(**context):
        """Kiểm tra trạng thái Spark containers"""
        print("🔍 Checking Spark cluster status...")
        
        results = {}
        for service_name, config in SPARK_SERVICES.items():
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

    # Tạo tasks
    task_start_master = PythonOperator(
        task_id='start_spark_master',
        python_callable=start_spark_master,
    )

    task_start_worker_1 = PythonOperator(
        task_id='start_spark_worker_1',
        python_callable=start_spark_worker_1,
    )

    task_start_worker_2 = PythonOperator(
        task_id='start_spark_worker_2',
        python_callable=start_spark_worker_2,
    )

    task_check_status = PythonOperator(
        task_id='check_spark_status',
        python_callable=check_spark_status,
    )

    # Dependencies: Master -> Workers (parallel) -> Check Status
    task_start_master >> [task_start_worker_1, task_start_worker_2] >> task_check_status


# ============== DAG: Test Spark Stop ==============
with DAG(
    dag_id='test_spark_stop',
    description='DAG test dừng Spark cluster',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'spark', 'stop'],
    params={
        'stop_master': Param(True, type='boolean', description='Dừng Spark Master'),
        'stop_worker_1': Param(True, type='boolean', description='Dừng Spark Worker 1 (192.168.80.122)'),
        'stop_worker_2': Param(True, type='boolean', description='Dừng Spark Worker 2 (192.168.80.130)'),
        'remove_volumes': Param(False, type='boolean', description='Xóa volumes khi dừng'),
    }
) as dag_test_spark_stop:

    def stop_service(service_name, remove_volumes=False, timeout=300, **context):
        """Dừng một Spark service"""
        config = SPARK_SERVICES[service_name]

        result = docker_compose_down.apply_async(
            args=[config['path']],
            kwargs={
                'services': [config['service']],
                'volumes': remove_volumes,
                'remove_orphans': False,
            },
            queue=config['queue']
        )

        # Chờ kết quả từ Celery worker
        try:
            output = wait_for_celery_result(result, timeout=timeout)
            print(f"✅ {service_name} stopped successfully on {config['host']}")
            print(f"Output: {output}")
            return {
                'task_id': result.id,
                'service': service_name,
                'host': config['host'],
                'output': output,
                'status': 'success'
            }
        except Exception as e:
            print(f"❌ Failed to stop {service_name} on {config['host']}: {str(e)}")
            raise Exception(f"Failed to stop {service_name} on {config['host']}: {str(e)}")

    def stop_spark_worker_1(**context):
        """Dừng Spark Worker 1"""
        if not context['params'].get('stop_worker_1', True):
            print("⏭️ Skipping Spark Worker 1 stop")
            return {'skipped': True}
        return stop_service('spark-worker-1', context['params'].get('remove_volumes', False), **context)

    def stop_spark_worker_2(**context):
        """Dừng Spark Worker 2"""
        if not context['params'].get('stop_worker_2', True):
            print("⏭️ Skipping Spark Worker 2 stop")
            return {'skipped': True}
        return stop_service('spark-worker-2', context['params'].get('remove_volumes', False), **context)

    def stop_spark_master(**context):
        """Dừng Spark Master"""
        if not context['params'].get('stop_master', True):
            print("⏭️ Skipping Spark Master stop")
            return {'skipped': True}
        return stop_service('spark-master', context['params'].get('remove_volumes', False), **context)

    # Tạo tasks
    task_stop_worker_1 = PythonOperator(
        task_id='stop_spark_worker_1',
        python_callable=stop_spark_worker_1,
    )

    task_stop_worker_2 = PythonOperator(
        task_id='stop_spark_worker_2',
        python_callable=stop_spark_worker_2,
    )

    task_stop_master = PythonOperator(
        task_id='stop_spark_master',
        python_callable=stop_spark_master,
    )

    # Dependencies: Workers (parallel) -> Master (dừng Workers trước)
    [task_stop_worker_1, task_stop_worker_2] >> task_stop_master

