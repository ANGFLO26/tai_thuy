from celery import Celery
import subprocess
import os

app = Celery(
    'system_worker',
    broker='redis://192.168.80.98:6379/0',
    backend='db+postgresql://airflow:airflow@192.168.80.98/airflow'
)

# Cấu hình Celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Ho_Chi_Minh',
    enable_utc=True,
    # Định nghĩa routes cho các queue khác nhau
    # Lưu ý: Tất cả các task (run_command, docker_compose_*) không có routing mặc định
    # để có thể chạy trên các queue khác nhau qua apply_async(queue='...')
    # Ví dụ: run_command.apply_async(args=[...], queue='node_52')
    #        docker_compose_up.apply_async(args=[...], queue='node_52')
    task_routes={},
)

# Định nghĩa các node trong cluster
CLUSTER_NODES = {
    'spark-master': {'host': '192.168.80.52', 'queue': 'node_52'},
    'spark-worker-1': {'host': '192.168.80.122', 'queue': 'node_122'},
    'spark-worker-2': {'host': '192.168.80.130', 'queue': 'node_130'},
    'hadoop-namenode': {'host': '192.168.80.52', 'queue': 'node_52'},
    'hadoop-datanode': {'host': '192.168.80.52', 'queue': 'node_52'},
    'kafka': {'host': '192.168.80.122', 'queue': 'node_122'},
}

@app.task(bind=True)
def run_command(self, command, env_vars=None):
    """Chạy một lệnh shell"""
    try:
        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)

        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300,
            env=env
        )
        return {
            'status': 'success',
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except subprocess.TimeoutExpired:
        return {
            'status': 'error',
            'message': 'Command execution timed out (300s)'
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_run(self, image, container_name=None, ports=None, volumes=None, env_vars=None, detach=True):
    """Chạy Docker container"""
    try:
        cmd = ['docker', 'run']

        if detach:
            cmd.append('-d')

        if container_name:
            cmd.extend(['--name', container_name])

        if ports:
            for port in ports:
                cmd.extend(['-p', port])

        if volumes:
            for volume in volumes:
                cmd.extend(['-v', volume])

        if env_vars:
            for key, value in env_vars.items():
                cmd.extend(['-e', f'{key}={value}'])

        cmd.append(image)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120
        )
        return {
            'status': 'success',
            'container_id': result.stdout.strip(),
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_stop(self, container_name):
    """Dừng Docker container"""
    try:
        result = subprocess.run(
            ['docker', 'stop', container_name],
            capture_output=True,
            text=True,
            timeout=60
        )
        return {
            'status': 'success',
            'message': f'Container {container_name} stopped',
            'return_code': result.returncode
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_remove(self, container_name):
    """Xóa Docker container"""
    try:
        result = subprocess.run(
            ['docker', 'rm', container_name],
            capture_output=True,
            text=True,
            timeout=60
        )
        return {
            'status': 'success',
            'message': f'Container {container_name} removed',
            'return_code': result.returncode
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_ps(self, all_containers=False):
    """Liệt kê Docker containers"""
    try:
        cmd = ['docker', 'ps']
        if all_containers:
            cmd.append('-a')

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        return {
            'status': 'success',
            'output': result.stdout,
            'return_code': result.returncode
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_compose_up(self, path, services=None, detach=True, build=False, force_recreate=False):
    """Chạy docker-compose up với path được chỉ định

    Args:
        path: Đường dẫn file docker-compose.yml
        services: Service cụ thể hoặc list services (vd: 'spark-master' hoặc ['spark-master', 'spark-worker'])
        detach: Chạy ở chế độ background
        build: Build images trước khi start
        force_recreate: Force recreate containers
    """
    # Expand ~ thành home directory
    path = os.path.expanduser(path)
    cmd = ['docker', 'compose', '-f', path, 'up']

    if detach:
        cmd.append('-d')

    if build:
        cmd.append('--build')

    if force_recreate:
        cmd.append('--force-recreate')

    # Thêm services vào cuối command
    if services:
        if isinstance(services, str):
            cmd.append(services)
        elif isinstance(services, list):
            cmd.extend(services)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600
        )
    except subprocess.TimeoutExpired:
        raise Exception('Docker compose up timed out (600s)')

    # Kiểm tra return code và raise exception nếu lỗi
    if result.returncode != 0:
        raise Exception(f"Docker compose up failed: {result.stderr}")

    return {
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
        'return_code': result.returncode
    }


@app.task(bind=True)
def docker_compose_down(self, path, services=None, volumes=False, remove_orphans=False):
    """Dừng và xóa containers với docker-compose down

    Args:
        path: Đường dẫn file docker-compose.yml
        services: Service cụ thể hoặc list services (để trống = tất cả)
        volumes: Xóa volumes
        remove_orphans: Xóa orphan containers
    """
    # Expand ~ thành home directory
    path = os.path.expanduser(path)

    try:
        # Nếu có services cụ thể, dùng stop + rm thay vì down
        if services:
            if isinstance(services, str):
                services = [services]

            # Stop services
            stop_cmd = ['docker', 'compose', '-f', path, 'stop'] + services
            stop_result = subprocess.run(stop_cmd, capture_output=True, text=True, timeout=120)

            # Remove services
            rm_cmd = ['docker', 'compose', '-f', path, 'rm', '-f'] + services
            if volumes:
                rm_cmd.insert(5, '-v')
            result = subprocess.run(rm_cmd, capture_output=True, text=True, timeout=120)
        else:
            cmd = ['docker', 'compose', '-f', path, 'down']
            if volumes:
                cmd.append('-v')
            if remove_orphans:
                cmd.append('--remove-orphans')
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except subprocess.TimeoutExpired:
        raise Exception('Docker compose down timed out (300s)')

    # Kiểm tra return code và raise exception nếu lỗi
    if result.returncode != 0:
        raise Exception(f"Docker compose down failed: {result.stderr}")

    return {
        'status': 'success',
        'stdout': result.stdout,
        'stderr': result.stderr,
        'return_code': result.returncode
    }


@app.task(bind=True)
def docker_compose_ps(self, path):
    """Liệt kê containers của docker-compose"""
    # Expand ~ thành home directory
    path = os.path.expanduser(path)
    cmd = ['docker', 'compose', '-f', path, 'ps']

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
    except subprocess.TimeoutExpired:
        raise Exception('Docker compose ps timed out (60s)')

    # Kiểm tra return code và raise exception nếu lỗi
    if result.returncode != 0:
        raise Exception(f"Docker compose ps failed: {result.stderr}")

    return {
        'status': 'success',
        'output': result.stdout,
        'stderr': result.stderr,
        'return_code': result.returncode
    }


@app.task(bind=True)
def docker_compose_logs(self, path, service=None, tail=100):
    """Lấy logs từ docker-compose"""
    # Expand ~ thành home directory
    path = os.path.expanduser(path)
    cmd = ['docker', 'compose', '-f', path, 'logs', '--tail', str(tail)]

    if service:
        cmd.append(service)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
    except subprocess.TimeoutExpired:
        raise Exception('Docker compose logs timed out (60s)')

    # Kiểm tra return code và raise exception nếu lỗi
    if result.returncode != 0:
        raise Exception(f"Docker compose logs failed: {result.stderr}")

    return {
        'status': 'success',
        'output': result.stdout,
        'stderr': result.stderr,
        'return_code': result.returncode
    }
