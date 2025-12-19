#!/usr/bin/env python3
"""
Script kiểm tra trạng thái Celery worker và queues
Sử dụng: python3 check_celery_worker.py
"""

import sys
import os
import subprocess

# Thêm đường dẫn để import mycelery
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'mycelery'))

from mycelery.system_worker import app, check_celery_worker_status


def check_celery_processes():
    """Kiểm tra Celery worker processes đang chạy"""
    print("=" * 60)
    print("🔍 Checking Celery Worker Processes...")
    print("=" * 60)
    
    try:
        # Kiểm tra process bằng ps
        result = subprocess.run(
            ['ps', 'aux'],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        celery_processes = [line for line in result.stdout.split('\n') 
                          if 'celery' in line.lower() and 'worker' in line.lower()]
        
        if celery_processes:
            print(f"✅ Found {len(celery_processes)} Celery worker process(es):")
            for proc in celery_processes:
                print(f"   {proc}")
        else:
            print("❌ No Celery worker processes found")
        
        return len(celery_processes) > 0
    except Exception as e:
        print(f"❌ Error checking processes: {str(e)}")
        return False


def check_celery_inspect():
    """Kiểm tra Celery worker bằng inspect"""
    print("\n" + "=" * 60)
    print("🔍 Checking Celery Worker Status (via inspect)...")
    print("=" * 60)
    
    try:
        from celery import current_app
        
        inspect_obj = current_app.control.inspect()
        
        # Active workers
        active_workers = inspect_obj.active()
        print(f"\n📊 Active Workers: {len(active_workers) if active_workers else 0}")
        if active_workers:
            for worker_name, tasks in active_workers.items():
                print(f"   - {worker_name}: {len(tasks)} active task(s)")
        
        # Registered tasks
        registered_tasks = inspect_obj.registered()
        print(f"\n📋 Registered Tasks:")
        if registered_tasks:
            for worker_name, tasks in registered_tasks.items():
                print(f"   - {worker_name}: {len(tasks)} task(s)")
                if tasks:
                    print(f"     Tasks: {', '.join(tasks[:5])}{'...' if len(tasks) > 5 else ''}")
        else:
            print("   ❌ No registered tasks found")
        
        # Active queues
        active_queues = inspect_obj.active_queues()
        print(f"\n📬 Active Queues:")
        if active_queues:
            for worker_name, queues in active_queues.items():
                print(f"   - {worker_name}:")
                for queue in queues:
                    queue_name = queue.get('name', 'unknown')
                    print(f"     • {queue_name}")
        else:
            print("   ❌ No active queues found")
        
        # Stats
        stats = inspect_obj.stats()
        if stats:
            print(f"\n📈 Worker Statistics:")
            for worker_name, stat in stats.items():
                print(f"   - {worker_name}:")
                print(f"     • Pool: {stat.get('pool', {}).get('implementation', 'N/A')}")
                print(f"     • Processes: {stat.get('pool', {}).get('max-concurrency', 'N/A')}")
                print(f"     • Total tasks: {stat.get('total', {}).get('mycelery.system_worker.run_command', 'N/A')}")
        
        return active_workers is not None and len(active_workers) > 0
        
    except Exception as e:
        print(f"❌ Error checking via inspect: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def check_broker_connection():
    """Kiểm tra kết nối đến broker"""
    print("\n" + "=" * 60)
    print("🔍 Checking Broker Connection...")
    print("=" * 60)
    
    try:
        broker_url = app.conf.broker_url
        print(f"📡 Broker URL: {broker_url}")
        
        # Kiểm tra kết nối
        inspect_obj = app.control.inspect()
        active_workers = inspect_obj.active()
        
        if active_workers:
            print("✅ Broker connection: OK")
            return True
        else:
            print("⚠️  Broker connection: OK but no active workers")
            return False
            
    except Exception as e:
        print(f"❌ Broker connection failed: {str(e)}")
        return False


def main():
    """Main function"""
    print("\n" + "=" * 60)
    print("🚀 Celery Worker Status Checker")
    print("=" * 60)
    
    # Check 1: Celery processes
    has_processes = check_celery_processes()
    
    # Check 2: Broker connection
    broker_ok = check_broker_connection()
    
    # Check 3: Celery inspect
    has_workers = check_celery_inspect()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Summary")
    print("=" * 60)
    print(f"Celery Processes: {'✅ Running' if has_processes else '❌ Not found'}")
    print(f"Broker Connection: {'✅ OK' if broker_ok else '❌ Failed'}")
    print(f"Active Workers: {'✅ Found' if has_workers else '❌ Not found'}")
    
    if has_processes and broker_ok and has_workers:
        print("\n✅ Celery worker is ready to process tasks!")
        return 0
    else:
        print("\n⚠️  Celery worker may not be ready. Please check the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

