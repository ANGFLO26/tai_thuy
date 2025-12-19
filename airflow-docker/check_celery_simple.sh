#!/bin/bash
# Script đơn giản để kiểm tra Celery worker

echo "=========================================="
echo "🔍 Checking Celery Worker Status"
echo "=========================================="
echo ""

# 1. Kiểm tra Celery worker processes
echo "1️⃣ Checking Celery worker processes..."
CELERY_PROCS=$(ps aux | grep -i "celery.*worker" | grep -v grep)
if [ -z "$CELERY_PROCS" ]; then
    echo "   ❌ No Celery worker processes found"
else
    echo "   ✅ Found Celery worker process(es):"
    echo "$CELERY_PROCS" | while read line; do
        echo "      $line"
    done
fi
echo ""

# 2. Kiểm tra Celery bằng inspect (nếu có celery command)
echo "2️⃣ Checking Celery active workers..."
if command -v celery &> /dev/null; then
    cd "$(dirname "$0")" || exit 1
    celery -A mycelery.system_worker inspect active 2>/dev/null | head -20
    echo ""
    echo "3️⃣ Checking Celery registered tasks..."
    celery -A mycelery.system_worker inspect registered 2>/dev/null | head -20
    echo ""
    echo "4️⃣ Checking Celery active queues..."
    celery -A mycelery.system_worker inspect active_queues 2>/dev/null | head -20
else
    echo "   ⚠️  Celery command not found. Install celery or use Python script."
fi
echo ""

# 3. Kiểm tra Redis connection (broker)
echo "5️⃣ Checking Redis broker connection..."
if command -v redis-cli &> /dev/null; then
    REDIS_HOST=$(echo "$CELERY_BROKER_URL" | grep -oP 'redis://\K[^:]+' || echo "192.168.80.98")
    REDIS_PORT=$(echo "$CELERY_BROKER_URL" | grep -oP 'redis://[^:]+:\K[^/]+' || echo "6379")
    if redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping 2>/dev/null | grep -q PONG; then
        echo "   ✅ Redis broker is accessible at $REDIS_HOST:$REDIS_PORT"
    else
        echo "   ❌ Cannot connect to Redis broker at $REDIS_HOST:$REDIS_PORT"
    fi
else
    echo "   ⚠️  redis-cli not found. Cannot check Redis connection."
fi
echo ""

echo "=========================================="
echo "✅ Check completed!"
echo "=========================================="

