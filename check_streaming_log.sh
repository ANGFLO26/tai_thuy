#!/bin/bash
# Script để kiểm tra log của streaming job

LOG_FILE="/tmp/kafka_streaming.log"
PID_FILE="/tmp/kafka_streaming.pid"

echo "=========================================="
echo "Kiểm tra Kafka Streaming Log"
echo "=========================================="
echo ""

# Kiểm tra log file
if [ -f "$LOG_FILE" ]; then
    echo "✅ Log file tồn tại: $LOG_FILE"
    echo ""
    echo "📋 Nội dung log (50 dòng cuối):"
    echo "----------------------------------------"
    tail -n 50 "$LOG_FILE"
    echo "----------------------------------------"
else
    echo "❌ Log file không tồn tại: $LOG_FILE"
    echo "   Có thể process chưa chạy hoặc đã fail ngay từ đầu"
fi

echo ""
echo "=========================================="

# Kiểm tra PID file
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    echo "📝 PID file tồn tại: $PID_FILE"
    echo "   PID: $PID"
    
    # Kiểm tra process có đang chạy không
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "✅ Process đang chạy (PID: $PID)"
        ps -p "$PID" -o pid,ppid,cmd
    else
        echo "❌ Process không còn chạy (PID: $PID)"
        echo "   Process có thể đã exit"
    fi
else
    echo "⚠️  PID file không tồn tại: $PID_FILE"
fi

echo ""
echo "=========================================="
echo "Kiểm tra thủ công:"
echo "=========================================="
echo ""
echo "1. Kiểm tra file CSV có tồn tại không:"
echo "   ls -la ~/tai_thuy/streaming/stream.csv"
echo ""
echo "2. Kiểm tra script có tồn tại không:"
echo "   ls -la ~/tai_thuy/streaming/kafka_streaming.py"
echo ""
echo "3. Chạy thử script thủ công:"
echo "   cd ~/tai_thuy/streaming && python3 kafka_streaming.py"
echo ""
echo "4. Kiểm tra Kafka có đang chạy không:"
echo "   docker ps | grep kafka"
echo "   telnet 192.168.80.122 9092"
echo ""

