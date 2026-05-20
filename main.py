import os
import json
import time
import random
from confluent_kafka import Producer

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap.kafka.svc:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-prices")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "693bggvqxivs40d8pamjwr5bi")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "WC3mwbnBCst3iEu0t2UszAL6JchNhkNC")

producer_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
}

if KAFKA_USERNAME and KAFKA_PASSWORD:
    producer_config.update({
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": KAFKA_USERNAME,
        "sasl.password": KAFKA_PASSWORD,
    })

producer = Producer(producer_config)

TICKERS = ["AAPL", "GOOGL", "TSLA", "MSFT", "AMZN"]
prices = {t: round(random.uniform(100, 500), 2) for t in TICKERS}

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()}")

def simulate_stock():
    print(f"🚀 Publishing stock prices to topic: {KAFKA_TOPIC}")
    while True:
        for ticker in TICKERS:
            change = round(random.uniform(-5, 5), 2)
            prices[ticker] = round(prices[ticker] + change, 2)
            payload = {
                "ticker": ticker,
                "price": prices[ticker],
                "change_pct": round((change / prices[ticker]) * 100, 2),
                "volume": random.randint(1000, 50000),
                "timestamp": int(time.time())
            }
            producer.produce(
                KAFKA_TOPIC,
                key=ticker,
                value=json.dumps(payload).encode('utf-8'),
                callback=delivery_report
            )
            producer.poll(0)
        producer.flush()
        time.sleep(1)

if __name__ == "__main__":
    simulate_stock()