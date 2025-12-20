#!/bin/bash
# Script để kiểm tra tại sao Spark Worker 122 không xuất hiện trên Master

echo "=========================================="
echo "🔍 Checking Spark Worker 122 Status"
echo "=========================================="
echo ""

# 1. Kiểm tra Docker container trên máy 122
echo "1️⃣ Checking Docker container on 192.168.80.122..."
ssh labsit-13@192.168.80.122 << 'REMOTE_SCRIPT'
echo "--- Docker containers ---"
docker ps -a | grep -i spark || echo "No Spark containers found"

echo ""
echo "--- Spark worker container logs (last 50 lines) ---"
docker logs $(docker ps -a | grep spark-worker | head -1 | awk '{print $1}') 2>&1 | tail -50 || echo "Cannot get logs"

echo ""
echo "--- Checking if container is running ---"
docker ps | grep spark-worker && echo "✓ Container is running" || echo "✗ Container is not running"
REMOTE_SCRIPT

echo ""
echo "2️⃣ Checking network connectivity from 122 to Master..."
ssh labsit-13@192.168.80.122 << 'REMOTE_SCRIPT'
echo "--- Testing connection to Spark Master (192.168.80.52:7077) ---"
timeout 5 bash -c 'cat < /dev/null > /dev/tcp/192.168.80.52/7077' 2>/dev/null && echo "✓ Port 7077 is reachable" || echo "✗ Cannot connect to port 7077"

echo ""
echo "--- Testing connection to Spark Master Web UI (192.168.80.52:8080) ---"
timeout 5 bash -c 'cat < /dev/null > /dev/tcp/192.168.80.52/8080' 2>/dev/null && echo "✓ Port 8080 is reachable" || echo "✗ Cannot connect to port 8080"
REMOTE_SCRIPT

echo ""
echo "3️⃣ Checking Spark Master configuration..."
ssh labsit-13@192.168.80.122 << 'REMOTE_SCRIPT'
echo "--- Checking docker-compose.yml for Spark Master URL ---"
if [ -f ~/Documents/docker-spark/docker-compose.yml ]; then
    echo "Found docker-compose.yml"
    grep -A 5 -B 5 "SPARK_MASTER" ~/Documents/docker-spark/docker-compose.yml | head -20 || echo "SPARK_MASTER not found in config"
else
    echo "✗ docker-compose.yml not found at ~/Documents/docker-spark/docker-compose.yml"
fi
REMOTE_SCRIPT

echo ""
echo "4️⃣ Checking Spark Worker 130 for comparison..."
ssh labsit04@192.168.80.130 << 'REMOTE_SCRIPT'
echo "--- Docker containers on 130 ---"
docker ps -a | grep -i spark | head -5

echo ""
echo "--- Spark worker container logs (last 20 lines) ---"
docker logs $(docker ps | grep spark-worker | head -1 | awk '{print $1}') 2>&1 | tail -20 || echo "Cannot get logs"
REMOTE_SCRIPT

echo ""
echo "=========================================="
echo "Analysis complete!"
echo "=========================================="

