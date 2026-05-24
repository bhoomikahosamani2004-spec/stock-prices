import os
import json
import threading
from collections import deque
from confluent_kafka import Consumer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap.kafka.svc:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "")

kafka_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': KAFKA_USERNAME,
    'sasl.password': KAFKA_PASSWORD,
    'auto.offset.reset': 'latest',
}

# Store last 50 events per topic
price_data = deque(maxlen=50)
fraud_data = deque(maxlen=50)
alert_data = deque(maxlen=50)

def consume_topic(topic, storage, group_id):
    config = {**kafka_config, 'group.id': group_id}
    consumer = Consumer(config)
    consumer.subscribe([topic])
    while True:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            try:
                data = json.loads(msg.value().decode('utf-8'))
                storage.appendleft(data)
            except Exception as e:
                print(f"Error: {e}")

# Start background consumers
threading.Thread(target=consume_topic, args=("price-aggregated", price_data, "dashboard-price"), daemon=True).start()
threading.Thread(target=consume_topic, args=("fraud-alerts", fraud_data, "dashboard-fraud"), daemon=True).start()
threading.Thread(target=consume_topic, args=("stock-prices", alert_data, "dashboard-alert"), daemon=True).start()

@app.get("/prices")
def get_prices():
    return list(price_data)

@app.get("/fraud")
def get_fraud():
    return list(fraud_data)

@app.get("/alerts")
def get_alerts():
    return list(alert_data)

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
