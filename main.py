
import os
import json
import threading
from collections import deque
from confluent_kafka import Consumer, KafkaException
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
    'enable.auto.commit': True,
}

price_data = deque(maxlen=50)
fraud_data = deque(maxlen=50)
alert_data = deque(maxlen=50)

def consume_topic(topic, storage, group_id):
    while True:
        try:
            config = {**kafka_config, 'group.id': group_id}
            consumer = Consumer(config)
            consumer.subscribe([topic])
            print(f"Subscribed to {topic}")
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Error on {topic}: {msg.error()}")
                    continue
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    storage.appendleft(data)
                    print(f"Received from {topic}: {data}")
                except Exception as e:
                    print(f"Parse error: {e}")
        except Exception as e:
            print(f"Consumer crashed for {topic}, restarting: {e}")
            import time
            time.sleep(5)

threading.Thread(target=consume_topic, args=("price-aggregated", price_data, "db-price-v1"), daemon=True).start()
threading.Thread(target=consume_topic, args=("fraud-alerts", fraud_data, "db-fraud-v1"), daemon=True).start()
threading.Thread(target=consume_topic, args=("stock-prices", alert_data, "db-alert-v1"), daemon=True).start()

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
