# Hướng dẫn kiểm tra log khi streaming job fail

## 1. SSH vào máy chạy task (192.168.80.52)

```bash
ssh user@192.168.80.52
# Thay 'user' bằng username của bạn
```

## 2. Kiểm tra log file

```bash
# Xem nội dung log
cat /tmp/kafka_streaming.log

# Hoặc xem với tail để theo dõi real-time
tail -f /tmp/kafka_streaming.log

# Xem 50 dòng cuối cùng
tail -n 50 /tmp/kafka_streaming.log
```

## 3. Kiểm tra PID file (nếu có)

```bash
cat /tmp/kafka_streaming.pid
```

## 4. Kiểm tra process có đang chạy không

```bash
# Nếu có PID
ps -p $(cat /tmp/kafka_streaming.pid)

# Hoặc tìm process theo tên
ps aux | grep kafka_streaming.py
```

## 5. Các nguyên nhân thường gặp

### a) File CSV không tồn tại
Script đang tìm file tại: `/home/labsit-13/tai_thuy/streaming/stream.csv`

Kiểm tra:
```bash
ls -la /home/labsit-13/tai_thuy/streaming/stream.csv
# Hoặc
ls -la ~/tai_thuy/streaming/stream.csv
```

### b) Thiếu thư viện Python
```bash
python3 -c "import pandas; import kafka"
```

### c) Không kết nối được Kafka
```bash
# Kiểm tra Kafka có đang chạy không
docker ps | grep kafka

# Kiểm tra kết nối
telnet 192.168.80.122 9092
```

### d) Đường dẫn script sai
Script được gọi từ: `~/tai_thuy/streaming/kafka_streaming.py`

Kiểm tra:
```bash
ls -la ~/tai_thuy/streaming/kafka_streaming.py
```

## 6. Chạy thử script thủ công để debug

```bash
cd ~/tai_thuy/streaming
python3 kafka_streaming.py
```

## 7. Sửa lỗi đường dẫn CSV trong script

Nếu đường dẫn CSV sai, sửa file `~/tai_thuy/streaming/kafka_streaming.py`:

```python
# Dòng 11, thay đổi từ:
csv_path = "/home/labsit-13/tai_thuy/streaming/stream.csv"

# Thành đường dẫn đúng của bạn, ví dụ:
csv_path = "/home/labsit/tai_thuy/streaming/stream.csv"
# hoặc
csv_path = os.path.expanduser("~/tai_thuy/streaming/stream.csv")
```

