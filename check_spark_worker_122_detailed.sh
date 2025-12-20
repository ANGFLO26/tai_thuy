#!/bin/bash
# Script chi tiết để kiểm tra Spark Worker 122

echo "=========================================="
echo "🔍 Detailed Check: Spark Worker 122"
echo "=========================================="
echo ""

echo "1️⃣ Container Status:"
ssh labsit-13@192.168.80.122 << 'REMOTE_SCRIPT'
CONTAINER_ID=$(docker ps | grep spark-worker | awk '{print $1}')
if [ -z "$CONTAINER_ID" ]; then
    echo "✗ No running spark-worker container found"
    exit 1
fi

echo "Container ID: $CONTAINER_ID"
echo "Container Status:"
docker ps | grep spark-worker
echo ""

echo "2️⃣ Container Logs (last 100 lines):"
docker logs --tail 100 $CONTAINER_ID 2>&1
echo ""

echo "3️⃣ Environment Variables in Container:"
docker exec $CONTAINER_ID env | grep -i spark
echo ""

echo "4️⃣ SPARK_MASTER_URL check:"
docker exec $CONTAINER_ID env | grep SPARK_MASTER || echo "SPARK_MASTER not found in env"
echo ""

echo "5️⃣ Network connectivity from container to Master:"
docker exec $CONTAINER_ID bash -c 'timeout 5 bash -c "cat < /dev/null > /dev/tcp/192.168.80.52/7077" 2>/dev/null' && echo "✓ Port 7077 reachable" || echo "✗ Cannot reach port 7077"
echo ""

echo "6️⃣ Checking if worker process is running inside container:"
docker exec $CONTAINER_ID ps aux | grep -i spark | head -10
echo ""

echo "7️⃣ Checking docker-compose.yml configuration:"
if [ -f ~/Documents/docker-spark/docker-compose.yml ]; then
    echo "--- SPARK_MASTER configuration ---"
    grep -A 20 "spark-worker:" ~/Documents/docker-spark/docker-compose.yml | grep -E "SPARK_MASTER|environment" -A 5
else
    echo "✗ docker-compose.yml not found"
fi
REMOTE_SCRIPT

echo ""
echo "=========================================="
echo "Comparison with Worker 130:"
echo "=========================================="
echo ""

echo "8️⃣ Worker 130 container logs (last 30 lines):"
ssh labsit04@192.168.80.130 << 'REMOTE_SCRIPT'
CONTAINER_ID=$(docker ps | grep spark-worker | awk '{print $1}')
if [ -n "$CONTAINER_ID" ]; then
    echo "Container ID: $CONTAINER_ID"
    docker logs --tail 30 $CONTAINER_ID 2>&1 | grep -E "Registered|Master|ERROR|WARN" || docker logs --tail 30 $CONTAINER_ID 2>&1 | tail -10
else
    echo "No running spark-worker container found"
fi
REMOTE_SCRIPT

echo ""
echo "=========================================="
