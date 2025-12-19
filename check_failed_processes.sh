#!/bin/bash
# Script để kiểm tra các process bị fail

echo "=========================================="
echo "Kiểm tra các process bị fail"
echo "=========================================="
echo ""

# 1. Kiểm tra kafka_streaming.log
echo "1. Kafka Streaming Log:"
echo "----------------------------------------"
if [ -f "/tmp/kafka_streaming.log" ]; then
    echo "📋 Nội dung log (50 dòng cuối):"
    tail -n 50 /tmp/kafka_streaming.log
else
    echo "❌ Log file không tồn tại: /tmp/kafka_streaming.log"
fi
echo ""

# 2. Kiểm tra ui_server.log
echo "2. UI Server Log:"
echo "----------------------------------------"
if [ -f "/tmp/ui_server.log" ]; then
    echo "📋 Nội dung log (50 dòng cuối):"
    tail -n 50 /tmp/ui_server.log
else
    echo "❌ Log file không tồn tại: /tmp/ui_server.log"
fi
echo ""

# 3. Kiểm tra PID files
echo "3. PID Files:"
echo "----------------------------------------"
if [ -f "/tmp/kafka_streaming.pid" ]; then
    PID=$(cat /tmp/kafka_streaming.pid)
    echo "📝 kafka_streaming.pid: $PID"
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "   ✅ Process đang chạy"
    else
        echo "   ❌ Process không còn chạy"
    fi
else
    echo "⚠️  /tmp/kafka_streaming.pid không tồn tại"
fi

if [ -f "/tmp/ui_server.pid" ]; then
    PID=$(cat /tmp/ui_server.pid)
    echo "📝 ui_server.pid: $PID"
    if ps -p "$PID" > /dev/null 2>&1; then
        echo "   ✅ Process đang chạy"
    else
        echo "   ❌ Process không còn chạy"
    fi
else
    echo "⚠️  /tmp/ui_server.pid không tồn tại"
fi
echo ""

# 4. Kiểm tra Python và dependencies
echo "4. Kiểm tra Python và dependencies:"
echo "----------------------------------------"
echo "Python version:"
python3 --version
echo ""

echo "Kiểm tra pandas:"
python3 -c "import pandas; print('✅ pandas OK')" 2>&1 || echo "❌ pandas NOT FOUND"
echo ""

echo "Kiểm tra kafka:"
python3 -c "import kafka; print('✅ kafka OK')" 2>&1 || echo "❌ kafka NOT FOUND"
echo ""

# 5. Kiểm tra scripts có tồn tại không
echo "5. Kiểm tra scripts:"
echo "----------------------------------------"
if [ -f "$HOME/tai_thuy/streaming/kafka_streaming.py" ]; then
    echo "✅ kafka_streaming.py tồn tại"
else
    echo "❌ kafka_streaming.py KHÔNG tồn tại"
fi

if [ -f "$HOME/tai_thuy/ui/server.py" ]; then
    echo "✅ server.py tồn tại"
else
    echo "❌ server.py KHÔNG tồn tại"
fi
echo ""

# 6. Test chạy thủ công
echo "6. Hướng dẫn test thủ công:"
echo "----------------------------------------"
echo "Test kafka_streaming:"
echo "  cd ~/tai_thuy/streaming"
echo "  python3 kafka_streaming.py"
echo ""
echo "Test ui_server:"
echo "  cd ~/tai_thuy/ui"
echo "  python3 server.py"
echo ""




