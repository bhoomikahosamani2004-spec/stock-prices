import os
import json
import time
from collections import defaultdict
from confluent_kafka import Consumer, Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap.kafka.svc:9092")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME", "693bggvqxivs40d8pamjwr5bi")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD", "WC3mwbnBCst3iEu0t2UszAL6JchNhkNC")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "stock-prices")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "price-aggregated")
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "30"))

kafka_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': KAFKA_USERNAME,
    'sasl.password': KAFKA_PASSWORD,
}

consumer = Consumer({
    **kafka_config,
    'group.id': 'price-aggregator-v1',
    'auto.offset.reset': 'latest',
})

producer = Producer(kafka_config)

# Store prices per ticker
price_window = defaultdict(list)
last_flush = time.time()

def aggregate_and_publish():
    for ticker, prices in price_window.items():
        if not prices:
            continue
        result = {
            "ticker": ticker,
            "avg_price": round(sum(prices) / len(prices), 2),
            "min_price": round(min(prices), 2),
            "max_price": round(max(prices), 2),
            "count": len(prices),
            "window_seconds": WINDOW_SECONDS,
            "timestamp": int(time.time())
        }
        producer.produce(OUTPUT_TOPIC, json.dumps(result).encode('utf-8'))
        print(f"Published aggregation: {result}")
    producer.flush()
    price_window.clear()

def main():
    consumer.subscribe([INPUT_TOPIC])
    print(f"Listening to {INPUT_TOPIC}, publishing aggregations to {OUTPUT_TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg and not msg.error():
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    ticker = data.get("ticker", "UNKNOWN")
                    price = float(data.get("price", 0))
                    price_window[ticker].append(price)
                    print(f"Received: {ticker} @ {price}")
                except Exception as e:
                    print(f"Error processing message: {e}")

            # Every WINDOW_SECONDS, publish aggregation
            if time.time() - last_flush >= WINDOW_SECONDS:
                aggregate_and_publish()
                globals()['last_flush'] = time.time()

    except KeyboardInterrupt:
        print("Shutting down.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()