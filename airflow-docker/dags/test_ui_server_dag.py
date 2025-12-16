from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param
from datetime import datetime
import time

# Import tasks từ system_worker
from mycelery.system_worker import (
    run_python_background,
    kill_process,
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


# Cấu hình UI Server
UI_CONFIG = {
    'host': '192.168.80.52',
    'queue': 'node_52',
    'ui_dir': '~/tai_thuy/ui',
    'ui_script': 'server.py',
    'pid_file': '/tmp/ui_server.pid',
    'log_file': '/tmp/ui_server.log',
}


# ============== DAG: Test UI Server ==============
with DAG(
    dag_id='test_ui_server',
    description='DAG test chạy UI server để hiển thị giao diện',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test', 'ui', 'server'],
    params={
        'start_ui': Param(True, type='boolean', description='Bắt đầu UI server'),
        'stop_ui': Param(False, type='boolean', description='Dừng UI server'),
    }
) as dag_test_ui_server:

    def start_ui(**context):
        """Bắt đầu UI server"""
        if not context['params'].get('start_ui', True):
            return {'skipped': True}
        
        print(f"🚀 Starting UI server on {UI_CONFIG['host']}...")
        print(f"Script: {UI_CONFIG['ui_script']}")
        print(f"Directory: {UI_CONFIG['ui_dir']}")
        print(f"ℹ️  UI server will run continuously in background")
        
        result = run_python_background.apply_async(
            args=[UI_CONFIG['ui_script']],
            kwargs={
                'working_dir': UI_CONFIG['ui_dir'],
                'pid_file': UI_CONFIG['pid_file'],
                'log_file': UI_CONFIG['log_file'],
            },
            queue=UI_CONFIG['queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ UI server started successfully")
            print(f"Output: {output}")
            
            if not output.get('success'):
                raise Exception(f"Failed to start UI server: {output.get('error', 'Unknown error')}")
            
            pid = output.get('pid')
            if not pid:
                raise Exception("PID not returned from background process")
            
            print(f"📝 UI server process PID: {pid}")
            print(f"📝 PID file: {UI_CONFIG['pid_file']}")
            print(f"📝 Log file: {UI_CONFIG['log_file']}")
            print(f"🌐 UI server is running in background")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'pid': pid,
                'pid_file': UI_CONFIG['pid_file'],
                'log_file': UI_CONFIG['log_file'],
                'host': UI_CONFIG['host'],
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to start UI server: {str(e)}")
            raise Exception(f"Failed to start UI server: {str(e)}")

    def stop_ui(**context):
        """Dừng UI server"""
        if not context['params'].get('stop_ui', False):
            return {'skipped': True}
        
        print(f"🛑 Stopping UI server...")
        print(f"PID file: {UI_CONFIG['pid_file']}")
        
        result = kill_process.apply_async(
            args=[],
            kwargs={
                'pid_file': UI_CONFIG['pid_file'],
                'process_name': 'server.py',
            },
            queue=UI_CONFIG['queue']
        )
        
        try:
            output = wait_for_celery_result(result, timeout=60)
            print(f"✅ UI server stopped")
            print(f"Output: {output}")
            
            return {
                'task_id': result.id,
                'status': 'success',
                'output': output
            }
        except Exception as e:
            print(f"❌ Failed to stop UI server: {str(e)}")
            raise Exception(f"Failed to stop UI server: {str(e)}")

    def check_ui_status(**context):
        """Kiểm tra trạng thái UI server"""
        print(f"🔍 Checking UI server status...")
        print(f"PID file: {UI_CONFIG['pid_file']}")
        print(f"Log file: {UI_CONFIG['log_file']}")
        
        # Đọc PID file để kiểm tra
        import subprocess
        check_cmd = f"test -f {UI_CONFIG['pid_file']} && cat {UI_CONFIG['pid_file']} || echo 'PID file not found'"
        
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
            
            # Nếu đang chạy, có thể thử kiểm tra port (nếu server.py có expose port)
            if status == 'running':
                print(f"✅ UI server is running (PID: {pid})")
                print(f"ℹ️  Check log file {UI_CONFIG['log_file']} for server details")
            
            return {'pid': pid, 'status': status}
        else:
            print(f"⚠️ PID file not found or empty")
            return {'pid': None, 'status': 'not found'}

    # Tasks
    task_start_ui = PythonOperator(
        task_id='start_ui',
        python_callable=start_ui,
        retries=0,
    )

    task_check_status = PythonOperator(
        task_id='check_ui_status',
        python_callable=check_ui_status,
        retries=0,
    )

    task_stop_ui = PythonOperator(
        task_id='stop_ui',
        python_callable=stop_ui,
        retries=0,
    )

    # Dependencies
    # Start UI -> Check status
    task_start_ui >> task_check_status
    # Stop UI is independent (can be triggered separately via params)

