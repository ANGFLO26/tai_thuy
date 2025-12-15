from kafka import KafkaConsumer
import json
from datetime import datetime
import sys
import time

KAFKA_BOOTSTRAP_SERVERS = "192.168.80.122:9092"

# Check if topics exist and have data
def check_topic_status(topic_name):
    """Check if topic exists and has messages"""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            consumer_timeout_ms=5000
        )
        
        topics = consumer.topics()
        if topic_name not in topics:
            print(f"❌ ERROR: Topic '{topic_name}' does not exist!")
            print(f"Available topics: {topics}")
            consumer.close()
            return False
            
        # Check partitions
        partitions = consumer.partitions_for_topic(topic_name)
        print(f"✓ Topic '{topic_name}' exists with {len(partitions)} partition(s)")
        
        consumer.close()
        return True
    except Exception as e:
        print(f"❌ ERROR checking topic: {e}")
        return False

def view_input_data():
    """View input topic with transaction features (no labels)"""
    print("="*80)
    print("📥 VIEWING INPUT DATA TOPIC")
    print("="*80)
    print(f"Connected to: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: input")
    
    if not check_topic_status('input'):
        return
    
    print("-"*80)
    print(f"{'Timestamp':<26} {'TxID':<16} {'Time':<10} {'Amount':<12}")
    print("-"*80)
    
    consumer = KafkaConsumer(
        'input',
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='input-viewer-group'
    )
    
    try:
        for message in consumer:
            data = message.value
            timestamp = datetime.fromtimestamp(message.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"{timestamp:<26} "
                  f"{data.get('transaction_id', 'N/A'):<16} "
                  f"{data.get('Time', 'N/A'):<10} "
                  f"{data.get('Amount', 'N/A'):<12}")
            
    except KeyboardInterrupt:
        print("\n\n✓ Stopped viewing input_data")
    finally:
        consumer.close()


def view_predictions():
    """View output topic with fraud predictions"""
    print("="*100)
    print("🔮 VIEWING PREDICTIONS TOPIC")
    print("="*100)
    print(f"Connected to: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: output")
    
    if not check_topic_status('output'):
        return
    
    print("-"*100)
    print(f"{'Timestamp':<26} {'TxID':<16} {'FraudPred':<10} {'Score':<14}")
    print("-"*100)
    
    consumer = KafkaConsumer(
        'output',
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='predictions-viewer-group'
    )
    
    try:
        for message in consumer:
            data = message.value
            timestamp = datetime.fromtimestamp(message.timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            fraud_pred = data.get('fraud_prediction', 'N/A')
            score = data.get('fraud_score', 'N/A')
            print(f"{timestamp:<26} "
                  f"{data.get('transaction_id', 'N/A'):<16} "
                  f"{fraud_pred:<10} "
                  f"{score:<14}")
            
    except KeyboardInterrupt:
        print("\n✓ Stopped viewing output topic")
    finally:
        consumer.close()


def view_both_topics():
    """View both topics side by side (requires threading)"""
    import threading
    
    def input_thread():
        view_input_data()
    
    def prediction_thread():
        view_predictions()
    
    print("Starting viewers for both topics...")
    print("Press Ctrl+C to stop\n")
    
    t1 = threading.Thread(target=input_thread)
    t2 = threading.Thread(target=prediction_thread)
    
    t1.start()
    t2.start()
    
    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("\n✓ Stopped all viewers")


if __name__ == "__main__":
    print("\n🔍 Kafka Topic Viewer")
    print("=" * 50)
    print("Select option:")
    print("1. View input_data topic")
    print("2. View predictions topic")
    print("3. View both topics (split view)")
    print("=" * 50)
    
    choice = input("Enter choice (1/2/3): ").strip()
    
    if choice == "1":
        view_input_data()
    elif choice == "2":
        view_predictions()
    elif choice == "3":
        view_both_topics()
    else:
        print("Invalid choice. Exiting.")
        sys.exit(1)