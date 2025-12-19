#!/bin/bash
# Script để copy system_worker.py sang máy node_130

SOURCE_FILE="/home/labsit/tai_thuy/airflow-docker/mycelery/system_worker.py"
DEST_HOST="labsit04@192.168.80.130"
DEST_PATH="~/tai_thuy/airflow-docker/mycelery/system_worker.py"

echo "Copying $SOURCE_FILE to $DEST_HOST:$DEST_PATH"
scp "$SOURCE_FILE" "$DEST_HOST:$DEST_PATH"

if [ $? -eq 0 ]; then
    echo "✓ File copied successfully!"
    echo ""
    echo "Now on node_130, run:"
    echo "  cd ~/tai_thuy/airflow-docker"
    echo "  python3 -m py_compile mycelery/system_worker.py"
    echo "  # If no errors, start Celery worker:"
    echo "  nohup uv run celery -A mycelery.system_worker.app worker --loglevel=INFO -E -Q node_130 > output.log 2>&1 &"
else
    echo "✗ Failed to copy file"
    exit 1
fi
