from flask import Flask
from flask_socketio import SocketIO, emit
from kafka import KafkaConsumer
import json
import threading

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

KAFKA_BOOTSTRAP_SERVERS = "192.168.80.122:9092"
KAFKA_INPUT_TOPIC = "input"    # fraud input topic
KAFKA_OUTPUT_TOPIC = "output"  # fraud prediction topic

def consume_input_data():
    """Consume from input topic and emit to frontend"""
    consumer = KafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"✓ Started consuming from input topic '{KAFKA_INPUT_TOPIC}'")
    
    for message in consumer:
        data = message.value
        # Expected keys from fraud input:
        # Time, V1..V28, Amount, transaction_id, timestamp
        tx_id = data.get("transaction_id", "unknown")
        amount = data.get("Amount", "N/A")
        ts = data.get("timestamp", "N/A")
        print(f"→ Input: tx_id={tx_id}, amount={amount}, timestamp={ts}")
        socketio.emit('input_data', data)

def consume_predictions():
    """Consume from output topic and emit to frontend"""
    consumer = KafkaConsumer(
        KAFKA_OUTPUT_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"✓ Started consuming from output topic '{KAFKA_OUTPUT_TOPIC}'")
    
    for message in consumer:
        data = message.value
        # Expected keys from fraud prediction:
        # transaction_id, timestamp, fraud_prediction, fraud_score
        tx_id = data.get("transaction_id", "unknown")
        pred = data.get("fraud_prediction", "N/A")
        score = data.get("fraud_score", "N/A")
        print(f"← Prediction: tx_id={tx_id}, fraud_prediction={pred}, fraud_score={score}")
        
        socketio.emit('prediction_data', data)

@app.route('/')
def index():
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Kafka Monitor Backend</title>
        <style>
            body { 
                font-family: Arial; 
                padding: 20px; 
                background: #1a1a1a; 
                color: white; 
            }
            code { 
                background: #333; 
                padding: 2px 6px; 
                border-radius: 3px; 
            }
            .status { 
                color: #4CAF50; 
                font-weight: bold; 
            }
        </style>
    </head>
    <body>
        <h1>✓ Kafka Stream Monitor Backend</h1>
        <p class="status">WebSocket server is running successfully!</p>
        <p>Open <code>monitor.html</code> in your browser to view the dashboard.</p>
        <hr>
        <h3>Configuration:</h3>
        <ul>
            <li>Kafka Server: <code>192.168.80.122:9092</code></li>
            <li>Input Topic: <code>input</code></li>
            <li>Output Topic: <code>output</code></li>
            <li>WebSocket: <code>ws://0.0.0.0:5000</code></li>
        </ul>
    </body>
    </html>
    '''

@socketio.on('connect')
def handle_connect():
    print('✓ Client connected')
    emit('connection_response', {'status': 'connected'})

@socketio.on('disconnect')
def handle_disconnect():
    print('✗ Client disconnected')

if __name__ == '__main__':
    # Start Kafka consumers in background threads
    input_thread = threading.Thread(target=consume_input_data, daemon=True)
    prediction_thread = threading.Thread(target=consume_predictions, daemon=True)
    
    input_thread.start()
    prediction_thread.start()
    
    print("\n" + "="*60)
    print("🚀 Kafka Monitor Backend Server - Iris Classification")
    print("="*60)
    print(f"→ Server: http://0.0.0.0:5000")
    print(f"→ Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"→ Input Topic: {KAFKA_INPUT_TOPIC}")
    print(f"→ Output Topic: {KAFKA_OUTPUT_TOPIC}")
    print(f"→ Classes: 0=setosa, 1=versicolor, 2=virginica")
    print("="*60 + "\n")
    
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)