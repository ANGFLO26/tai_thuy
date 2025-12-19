#!/usr/bin/env python3
"""
Script để sửa các lỗi indentation trong system_worker.py từ máy node_130
"""

import re

# Đọc file từ code người dùng gửi
code = """from celery import Celery
import subprocess
import os
import signal
import time
import json
import sys

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
def run_command(self, command, env_vars=None, timeout=300, working_dir=None):
    \"\"\"Chạy một lệnh shell
    
    Args:
        command: Lệnh shell cần chạy
        env_vars: Dictionary các biến môi trường
        timeout: Timeout tính bằng giây (mặc định 300s)
        working_dir: Thư mục làm việc (mặc định None)
    \"\"\"
    try:
        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)

        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
            cwd=working_dir
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
            'message': f'Command execution timed out ({timeout}s)'
        }
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }


@app.task(bind=True)
def docker_run(self, image, container_name=None, ports=None, volumes=None, env_vars=None, detach=True):
    \"\"\"Chạy Docker container\"\"\"
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
    \"\"\"Dừng Docker container\"\"\"
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
    \"\"\"Xóa Docker container\"\"\"
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
    \"\"\"Liệt kê Docker containers\"\"\"
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
    \"\"\"Chạy docker-compose up với path được chỉ định

    Args:
        path: Đường dẫn file docker-compose.yml
        services: Service cụ thể hoặc list services (vd: 'spark-master' hoặc ['spark-master', 'spark-worker'])
        detach: Chạy ở chế độ background
        build: Build images trước khi start
        force_recreate: Force recreate containers
    \"\"\"
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
    \"\"\"Dừng và xóa containers với docker-compose down

    Args:
        path: Đường dẫn file docker-compose.yml
        services: Service cụ thể hoặc list services (để trống = tất cả)
        volumes: Xóa volumes
        remove_orphans: Xóa orphan containers
    \"\"\"
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
    \"\"\"Liệt kê containers của docker-compose\"\"\"
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
    \"\"\"Lấy logs từ docker-compose\"\"\"
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


@app.task(bind=True)
def spark_submit(self, script_path, master_url, executor_memory='4G', driver_memory='1G', 
                 packages=None, conf=None, working_dir=None, env_vars=None, timeout=3600, 
                 detach=False, pid_file=None, log_file=None):
    \"\"\"Chạy Spark submit với cấu hình đầy đủ
    
    Args:
        script_path: Đường dẫn đến Spark script
        master_url: Spark master URL (vd: spark://192.168.80.52:7077)
        executor_memory: Memory cho executor (mặc định 4G)
        driver_memory: Memory cho driver (mặc định 1G)
        packages: List packages cần download (vd: ['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1'])
        conf: Dictionary các Spark configs
        working_dir: Thư mục làm việc (mặc định ~/spark/bin)
        env_vars: Dictionary các biến môi trường
        timeout: Timeout tính bằng giây (mặc định 3600s)
        detach: Chạy ở background (mặc định False)
        pid_file: File để lưu PID nếu detach=True
        log_file: File để lưu log nếu detach=True
    \"\"\"
    try:
        # Expand paths
        script_path = os.path.expanduser(script_path)
        script_name = os.path.basename(script_path).replace('.py', '')
        
        if working_dir:
            working_dir = os.path.expanduser(working_dir)
        else:
            working_dir = os.path.expanduser('~/spark/bin')
        
        if not pid_file:
            pid_file = f'/tmp/spark_{script_name}.pid'
        if not log_file:
            log_file = f'/tmp/spark_{script_name}.log'
        
        # Build spark-submit command
        cmd = ['./spark-submit']
        cmd.extend(['--master', master_url])
        cmd.extend(['--executor-memory', executor_memory])
        cmd.extend(['--driver-memory', driver_memory])
        
        # Add packages
        if packages:
            if isinstance(packages, str):
                cmd.extend(['--packages', packages])
            elif isinstance(packages, list):
                cmd.extend(['--packages', ','.join(packages)])
        
        # Add Spark configs
        if conf:
            for key, value in conf.items():
                cmd.extend(['--conf', f'{key}={value}'])
        
        # Add script path
        cmd.append(script_path)
        
        # Setup environment
        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)
        
        # Set JAVA_HOME if not provided
        if 'JAVA_HOME' not in env:
            env['JAVA_HOME'] = '/usr/lib/jvm/java-17-openjdk-amd64'
        
        # Run command
        if detach:
            # Chạy ở background với nohup
            full_cmd = f"cd {working_dir} && nohup {' '.join(cmd)} > {log_file} 2>&1 & echo $! > {pid_file}"
            result = subprocess.run(
                full_cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30,
                env=env
            )
            
            if result.returncode != 0:
                raise Exception(f"Failed to start Spark job in background: {result.stderr}")
            
            # Read PID
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
            except:
                # Try to find process
                find_cmd = f"pgrep -f '{script_name}'"
                find_result = subprocess.run(find_cmd, shell=True, capture_output=True, text=True)
                if find_result.returncode == 0:
                    pid = int(find_result.stdout.strip().split('\\n')[0])
                else:
                    raise Exception("Could not determine PID")
            
            # Verify process is running
            time.sleep(2)
            try:
                os.kill(pid, 0)
            except OSError:
                raise Exception(f"Process {pid} started but immediately exited. Check {log_file}")
            
            return {
                'status': 'success',
                'pid': pid,
                'pid_file': pid_file,
                'log_file': log_file,
                'command': ' '.join(cmd),
                'detached': True
            }
        else:
            # Chạy bình thường và đợi kết quả
            result = subprocess.run(
                ' '.join(cmd),
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
                cwd=working_dir
            )
            
            if result.returncode != 0:
                raise Exception(f"Spark submit failed: {result.stderr}")
            
            return {
                'status': 'success',
                'stdout': result.stdout,
                'stderr': result.stderr,
                'return_code': result.returncode,
                'command': ' '.join(cmd),
                'detached': False
            }
    except subprocess.TimeoutExpired:
        if detach:
            raise Exception(f'Spark submit background start timed out')
        else:
            raise Exception(f'Spark submit timed out ({timeout}s)')
    except Exception as e:
        raise Exception(f"Spark submit error: {str(e)}")


@app.task(bind=True)
def run_python_background(self, script_path, working_dir=None, env_vars=None, 
                         pid_file=None, log_file=None):
    \"\"\"Chạy Python script ở background và lưu PID
    
    Args:
        script_path: Đường dẫn đến Python script
        working_dir: Thư mục làm việc
        env_vars: Dictionary các biến môi trường
        pid_file: Đường dẫn file để lưu PID (mặc định /tmp/{script_name}.pid)
        log_file: Đường dẫn file log (mặc định /tmp/{script_name}.log)
    \"\"\"
    try:
        script_path = os.path.expanduser(script_path)
        # Đảm bảo script_path là absolute path
        if not os.path.isabs(script_path):
            if working_dir:
                script_path = os.path.join(os.path.expanduser(working_dir), script_path)
            else:
                script_path = os.path.abspath(script_path)
        
        script_name = os.path.basename(script_path).replace('.py', '')
        
        if working_dir:
            working_dir = os.path.expanduser(working_dir)
            if not os.path.isabs(working_dir):
                working_dir = os.path.abspath(working_dir)
        else:
            working_dir = os.path.dirname(script_path)
        
        if not pid_file:
            pid_file = f'/tmp/{script_name}.pid'
        
        if not log_file:
            log_file = f'/tmp/{script_name}.log'
        
        # Setup environment
        env = os.environ.copy()
        if env_vars:
            env.update(env_vars)
        
        # Đảm bảo script_path là absolute path (final check)
        script_path = os.path.abspath(script_path)
        
        # Kiểm tra script có tồn tại không
        if not os.path.exists(script_path):
            raise Exception(f"Script not found: {script_path}")
        
        # Kiểm tra working directory có tồn tại không
        if not os.path.exists(working_dir):
            raise Exception(f"Working directory not found: {working_dir}")
        
        # Xác định Python interpreter để dùng
        # Ưu tiên dùng sys.executable (cùng Python với Celery worker) để đảm bảo cùng environment
        python_executable = sys.executable if sys.executable else 'python3'
        
        # Kiểm tra Python có sẵn không
        python_check = subprocess.run(
            [python_executable, '--version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        python_version = python_check.stdout.strip() if python_check.returncode == 0 else "Unknown"
        
        # Lấy Python path
        which_result = subprocess.run(
            ['which', python_executable] if python_executable != sys.executable else [python_executable, '-c', 'import sys; print(sys.executable)'],
            capture_output=True,
            text=True
        )
        python_path = which_result.stdout.strip() if which_result.returncode == 0 else python_executable
        
        # Kiểm tra PYTHONPATH và thêm vào env nếu cần
        if 'PYTHONPATH' not in env or not env.get('PYTHONPATH'):
            # Thêm working directory vào PYTHONPATH để script có thể import modules
            current_pythonpath = os.environ.get('PYTHONPATH', '')
            if current_pythonpath:
                env['PYTHONPATH'] = f"{working_dir}:{current_pythonpath}"
            else:
                env['PYTHONPATH'] = working_dir
        else:
            # Thêm working directory vào đầu PYTHONPATH
            env['PYTHONPATH'] = f"{working_dir}:{env['PYTHONPATH']}"
        
        # Mở log file để ghi (append mode để không mất log cũ)
        # Ghi thông tin debug vào log trước khi start process
        with open(log_file, 'a') as f:
            f.write(f"\\n{'='*60}\\n")
            f.write(f"[DEBUG] Starting process at {time.strftime('%Y-%m-%d %H:%M:%S')}\\n")
            f.write(f"[DEBUG] Script: {script_path}\\n")
            f.write(f"[DEBUG] Working dir: {working_dir}\\n")
            f.write(f"[DEBUG] Python executable: {python_executable}\\n")
            f.write(f"[DEBUG] Python version: {python_version}\\n")
            f.write(f"[DEBUG] Python path: {python_path}\\n")
            f.write(f"[DEBUG] PYTHONPATH: {env.get('PYTHONPATH', 'Not set')}\\n")
            f.write(f"[DEBUG] PID file: {pid_file}\\n")
            f.write(f"[DEBUG] Current user: {os.getenv('USER', 'unknown')}\\n")
            f.write(f"[DEBUG] Current directory: {os.getcwd()}\\n")
            f.write(f"[DEBUG] Script exists: {os.path.exists(script_path)}\\n")
            f.write(f"[DEBUG] Working dir exists: {os.path.exists(working_dir)}\\n")
            f.write(f"{'='*60}\\n")
        
        # Mở log file để redirect stdout/stderr của child process
        log_fd = open(log_file, 'a')
        
        # Ghi thông báo bắt đầu vào log
        log_fd.write(f"[INFO] Starting Python process: {python_executable} -u {script_path}\\n")
        log_fd.write(f"[INFO] Working directory: {working_dir}\\n")
        log_fd.write(f"[INFO] Environment PYTHONPATH: {env.get('PYTHONPATH', 'Not set')}\\n")
        log_fd.flush()  # Đảm bảo ghi ngay lập tức
        
        # Test script có thể compile được không (syntax check)
        try:
            compile_check = subprocess.run(
                [python_executable, '-m', 'py_compile', script_path],
                cwd=working_dir,
                capture_output=True,
                text=True,
                timeout=10,
                env=env
            )
            if compile_check.returncode != 0:
                error_msg = f"Script syntax error: {compile_check.stderr}"
                log_fd.write(f"[ERROR] {error_msg}\\n")
                log_fd.flush()
                log_fd.close()
                raise Exception(error_msg)
            log_fd.write(f"[INFO] Script syntax check passed\\n")
            log_fd.flush()
        except subprocess.TimeoutExpired:
            log_fd.write(f"[WARNING] Script syntax check timed out, continuing anyway\\n")
            log_fd.flush()
        except Exception as e:
            log_fd.write(f"[WARNING] Could not check script syntax: {str(e)}, continuing anyway\\n")
            log_fd.flush()
        
        # Chạy process với Popen để có thể lấy PID trực tiếp
        # Sử dụng sys.executable để đảm bảo dùng cùng Python environment với Celery worker
        # -u flag để unbuffered output (quan trọng cho việc xem log real-time)
        try:
            process = subprocess.Popen(
                [python_executable, '-u', script_path],
                cwd=working_dir,
                stdout=log_fd,
                stderr=subprocess.STDOUT,
                env=env,
                preexec_fn=os.setsid  # Tạo process group mới để dễ kill sau này
                # Không dùng start_new_session=True vì đã có preexec_fn=os.setsid
            )
            log_fd.write(f"[INFO] Process started with PID: {process.pid}\\n")
            log_fd.flush()
        except Exception as e:
            error_msg = f"Failed to start process: {str(e)}"
            log_fd.write(f"[ERROR] {error_msg}\\n")
            log_fd.flush()
            log_fd.close()
            raise Exception(error_msg)
        
        pid = process.pid
        
        # Lưu PID vào file
        try:
            with open(pid_file, 'w') as f:
                f.write(str(pid))
        except Exception as e:
            log_fd.close()
            process.terminate()
            raise Exception(f"Failed to write PID file: {str(e)}")
        
        # Đợi một chút để process có thời gian start và ghi log
        time.sleep(0.5)
        
        # Flush log để đảm bảo tất cả output được ghi
        try:
            log_fd.flush()
        except Exception:
            pass
        
        # Đóng file descriptor trong parent process
        # Child process đã có bản copy của file descriptor nên vẫn có thể ghi log
        # NHƯNG: Không đóng ngay, để process có thời gian ghi lỗi nếu có
        # Chỉ đóng sau khi verify process đang chạy
        
        # Verify process is running sau 1.5 giây (tăng thời gian để process có thể ghi lỗi)
        time.sleep(1.5)
        
        # Đóng file descriptor sau khi đã đợi đủ thời gian
        try:
            log_fd.close()
        except Exception:
            # Ignore error khi đóng file descriptor
            pass
        
        # Check process status bằng poll() - không raise exception, chỉ return returncode
        returncode = process.poll()
        if returncode is not None:
            # Process đã exit, đọc log để xem lỗi
            error_log = ""
            try:
                with open(log_file, 'r') as f:
                    full_log = f.read()
                    if full_log:
                        # Lấy 2000 ký tự cuối để có đủ thông tin
                        error_log = full_log[-2000:]
            except Exception as e:
                error_log = f"Could not read log file: {str(e)}"
            
            # Thêm thông tin về returncode
            error_summary = f"Process {pid} started but immediately exited (returncode={returncode})"
            if error_log:
                error_summary += f"\\n\\nLast log output:\\n{error_log}"
            else:
                error_summary += f"\\n\\nNo log output found. Check if log file exists: {log_file}"
        
            raise Exception(f"{error_summary}\\n\\nCheck full log at: {log_file}")
        
        # Double check bằng os.kill để đảm bảo process thực sự đang chạy
        try:
            os.kill(pid, 0)  # Signal 0 chỉ kiểm tra process có tồn tại không
        except OSError:
            # Process không tồn tại, đọc log để xem lỗi
            error_log = ""
            try:
                with open(log_file, 'r') as f:
                    full_log = f.read()
                    if full_log:
                        error_log = full_log[-2000:]
            except:
                pass
            
            error_msg = f"Process {pid} does not exist."
            if error_log:
                error_msg += f"\\n\\nLast log output:\\n{error_log}"
            else:
                error_msg += f"\\n\\nNo log output found. Check if log file exists: {log_file}"
            error_msg += f"\\n\\nCheck full log at: {log_file}"
            raise Exception(error_msg)
        
        return {
            'success': True,
            'status': 'success',
            'pid': pid,
            'pid_file': pid_file,
            'log_file': log_file,
            'script': script_path,
            'working_dir': working_dir
        }
    except Exception as e:
        raise Exception(f"Failed to start background Python process: {str(e)}")


@app.task(bind=True)
def kill_process(self, pid_file=None, process_name=None, signal_type='TERM'):
    \"\"\"Kill process từ PID file hoặc process name
    
    Args:
        pid_file: Đường dẫn file chứa PID
        process_name: Tên process để tìm (vd: 'kafka_streaming.py')
        signal_type: Loại signal ('TERM' hoặc 'KILL')
    \"\"\"
    try:
        pids = []
        
        # Get PID from file
        if pid_file:
            pid_file = os.path.expanduser(pid_file)
            if os.path.exists(pid_file):
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                    pids.append(pid)
        
        # Find PID by process name
        if process_name:
            find_cmd = f"pgrep -f '{process_name}'"
            find_result = subprocess.run(find_cmd, shell=True, capture_output=True, text=True)
            if find_result.returncode == 0:
                found_pids = [int(p) for p in find_result.stdout.strip().split('\\n') if p]
                pids.extend(found_pids)
        
        if not pids:
            return {
                'status': 'warning',
                'message': 'No process found to kill'
            }
        
        # Remove duplicates
        pids = list(set(pids))
        
        # Kill processes
        killed = []
        failed = []
        signal_num = signal.SIGTERM if signal_type == 'TERM' else signal.SIGKILL
        
        for pid in pids:
            try:
                # Kiểm tra process có tồn tại không
                os.kill(pid, 0)  # Signal 0 chỉ kiểm tra, không kill
                
                # Thử kill process group (vì process được tạo với os.setsid)
                # PID của process chính là process group ID nếu nó là leader
                try:
                    pgid = os.getpgid(pid)
                    # Nếu PID == PGID, process là process group leader
                    # Ta có thể kill process group an toàn
                    if pid == pgid:
                        os.killpg(pgid, signal_num)
                        killed.append(f"Process group {pid} (leader)")
                    else:
                        # Process không phải leader, chỉ kill process đó
                        os.kill(pid, signal_num)
                        killed.append(f"Process {pid}")
                except (ProcessLookupError, OSError):
                    # Nếu không lấy được process group, chỉ kill process
                    os.kill(pid, signal_num)
                    killed.append(f"Process {pid}")
                
                time.sleep(1)
            except ProcessLookupError:
                failed.append(f"PID {pid} not found")
            except PermissionError:
                failed.append(f"Permission denied for PID {pid}")
            except Exception as e:
                failed.append(f"Error killing PID {pid}: {str(e)}")
        
        # Clean up PID file if exists
        if pid_file and os.path.exists(pid_file):
            try:
                os.remove(pid_file)
            except:
                pass
        
        return {
            'status': 'success',
            'killed_pids': killed,
            'failed': failed,
            'signal': signal_type
        }
    except Exception as e:
        raise Exception(f"Failed to kill process: {str(e)}")


@app.task(bind=True)
def check_service_status(self, service_type, host=None, port=None):
    \"\"\"Kiểm tra service đã sẵn sàng chưa
    
    Args:
        service_type: Loại service ('hadoop', 'spark', 'kafka', 'hdfs')
        host: Host của service (mặc định lấy từ CLUSTER_NODES)
        port: Port của service
    \"\"\"
    try:
        if service_type == 'hadoop':
            # Check Hadoop Namenode bằng jps
            cmd = "jps | grep -i namenode"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            if result.returncode == 0 and 'NameNode' in result.stdout:
                return {'status': 'success', 'service': 'hadoop', 'ready': True}
            return {'status': 'success', 'service': 'hadoop', 'ready': False}
        
        elif service_type == 'spark':
            # Check Spark Master bằng curl
            if not host:
                host = CLUSTER_NODES['spark-master']['host']
            if not port:
                port = 8080
            
            cmd = f"curl -s -o /dev/null -w '%{{http_code}}' http://{host}:{port} || echo '000'"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            if result.stdout.strip() == '200':
                return {'status': 'success', 'service': 'spark', 'ready': True}
            return {'status': 'success', 'service': 'spark', 'ready': False}
        
        elif service_type == 'kafka':
            # Check Kafka container
            cmd = "docker ps --filter 'name=kafka' --format '{{.Names}}'"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            if result.returncode == 0 and 'kafka' in result.stdout.lower():
                return {'status': 'success', 'service': 'kafka', 'ready': True}
            return {'status': 'success', 'service': 'kafka', 'ready': False}
        
        elif service_type == 'hdfs':
            # Check HDFS bằng hdfs dfsadmin
            cmd = "hdfs dfsadmin -report 2>&1 | head -5"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            if result.returncode == 0 and 'Live datanodes' in result.stdout:
                return {'status': 'success', 'service': 'hdfs', 'ready': True}
            return {'status': 'success', 'service': 'hdfs', 'ready': False}
        
        else:
            raise Exception(f"Unknown service type: {service_type}")
    
    except Exception as e:
        raise Exception(f"Failed to check service status: {str(e)}")


@app.task(bind=True)
def check_hdfs_path(self, hdfs_path):
    \"\"\"Kiểm tra path trong HDFS có tồn tại không
    
    Args:
        hdfs_path: Đường dẫn HDFS (vd: hdfs://192.168.80.52:9000/model)
    \"\"\"
    try:
        cmd = f"hdfs dfs -test -e {hdfs_path}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        
        exists = result.returncode == 0
        return {
            'status': 'success',
            'path': hdfs_path,
            'exists': exists,
            'stderr': result.stderr
        }
    except Exception as e:
        raise Exception(f"Failed to check HDFS path: {str(e)}")


@app.task(bind=True)
def check_kafka_topic(self, bootstrap_server, topic_name):
    \"\"\"Kiểm tra Kafka topic có tồn tại không
    
    Args:
        bootstrap_server: Kafka bootstrap server (vd: 192.168.80.122:9092)
        topic_name: Tên topic cần kiểm tra
    \"\"\"
    try:
        cmd = (
            f"docker exec -i kafka kafka-topics "
            f"--bootstrap-server {bootstrap_server} "
            f"--list | grep -w {topic_name}"
        )
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        
        exists = result.returncode == 0 and topic_name in result.stdout
        return {
            'status': 'success',
            'topic': topic_name,
            'bootstrap_server': bootstrap_server,
            'exists': exists,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
    except Exception as e:
        raise Exception(f"Failed to check Kafka topic: {str(e)}")
"""

# Sửa các lỗi indentation
fixed_code = code

# Sửa lỗi 1: if env_vars thiếu indentation
fixed_code = re.sub(r'(\s+if volumes:\s+for volume in volumes:\s+cmd\.extend\(\[\'-v\', volume\]\))\nif env_vars:', r'\1\n        if env_vars:', fixed_code)

# Sửa lỗi 2: cmd.append('-d') thiếu indentation  
fixed_code = re.sub(r'(\s+if detach:)\ncmd\.append\(', r'\1\n        cmd.append(', fixed_code)

# Sửa lỗi 3: log_file thiếu indentation
fixed_code = re.sub(r'(\s+if not log_file:)\nlog_file =', r'\1\n            log_file =', fixed_code)

# Sửa lỗi 4: python_executable thiếu indentation
fixed_code = re.sub(r'(\s+# Xác định Python interpreter để dùng.*?)\npython_executable =', r'\1\n        python_executable =', fixed_code, flags=re.DOTALL)

# Sửa lỗi 5: log_fd.flush() thiếu indentation
fixed_code = re.sub(r'(\s+log_fd\.write\(f\"\[INFO\] Environment PYTHONPATH.*?\))\nlog_fd\.flush\(\)', r'\1\n        log_fd.flush()', fixed_code, flags=re.DOTALL)

# Sửa lỗi 6: env=env thiếu indentation
fixed_code = re.sub(r'(\s+timeout=10,)\nenv=env', r'\1\n                env=env', fixed_code)

# Sửa lỗi 7: time.sleep(1.5) thiếu indentation
fixed_code = re.sub(r'(\s+# Chỉ đóng sau khi verify process đang chạy)\ntime\.sleep\(1\.5\)', r'\1\n        \n        # Verify process is running sau 1.5 giây (tăng thời gian để process có thể ghi lỗi)\n        time.sleep(1.5)', fixed_code)

# Sửa lỗi 8: signal_type docstring thiếu indentation
fixed_code = re.sub(r'(\s+process_name: Tên process để tìm.*?)\nsignal_type:', r'\1\n        signal_type:', fixed_code, flags=re.DOTALL)

# Sửa lỗi 9: os.kill trong else block thiếu indentation
fixed_code = re.sub(r'(\s+else:\s+# Process không phải leader, chỉ kill process đó)\nos\.kill\(pid, signal_num\)\s+killed\.append\(f"Process \{pid\}"\)', r'\1\n                        os.kill(pid, signal_num)\n                        killed.append(f"Process {pid}")', fixed_code)

# Sửa lỗi 10: result = subprocess.run trong check_hdfs_path thiếu indentation
fixed_code = re.sub(r'(\s+cmd = f"hdfs dfs -test -e \{hdfs_path\}")\nresult =', r'\1\n        result =', fixed_code)

# Sửa lỗi 11: 'signal': signal_type thiếu indentation
fixed_code = re.sub(r'(\s+\'failed\': failed,)\n\'signal\':', r'\1\n            \'signal\':', fixed_code)

# Ghi file đã sửa
with open('/home/labsit/tai_thuy/airflow-docker/mycelery/system_worker_fixed.py', 'w') as f:
    f.write(fixed_code)

print("File đã được sửa và lưu tại: /home/labsit/tai_thuy/airflow-docker/mycelery/system_worker_fixed.py")
print("Bạn có thể copy file này sang máy node_130")

