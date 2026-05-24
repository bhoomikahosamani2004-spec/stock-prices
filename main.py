import os
import json
import time
import random
import uuid
from confluent_kafka import Producer, Consumer

# ── Kafka config (same as your stock-prices app) ──────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "my-cluster-kafka-bootstrap.kafka.svc:9092")
KAFKA_USERNAME          = os.getenv("KAFKA_USERNAME", "693bggvqxivs40d8pamjwr5bi")
KAFKA_PASSWORD          = os.getenv("KAFKA_PASSWORD", "WC3mwbnBCst3iEu0t2UszAL6JchNhkNC")
INPUT_TOPIC             = os.getenv("INPUT_TOPIC",  "order-book-events")
OUTPUT_TOPIC            = os.getenv("OUTPUT_TOPIC", "fraud-alerts")

kafka_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_PLAINTEXT",
    "sasl.mechanism":    "SCRAM-SHA-512",
    "sasl.username":     KAFKA_USERNAME,
    "sasl.password":     KAFKA_PASSWORD,
}

producer = Producer(kafka_config)

# ── BVRD Dominican Republic tickers ───────────────────────────────────────────
TICKERS  = ["APAP", "BPOP", "BHDLEON", "POPULAR", "TRICOM", "CERVECERIA", "VIAJAR"]
BROKERS  = ["BR-PARVAL", "BR-JMMB", "BR-BULLTICK", "BR-CITIFX", "BR-CARIBE"]
prices   = {t: round(random.uniform(50, 500), 2) for t in TICKERS}

# Track recent orders per ticker to detect wash trading
order_history = {t: [] for t in TICKERS}

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ [{msg.topic()}] {msg.key().decode()}")

def is_wash_trade(ticker, broker_id, side):
    """Flag if same broker traded both sides within last 10 orders."""
    history = order_history[ticker][-10:]
    opposite = "SELL" if side == "BUY" else "BUY"
    return any(o["broker_id"] == broker_id and o["side"] == opposite for o in history)

def is_spoofing(ticker, quantity, side):
    """Flag large orders that appear and disappear quickly (simulated)."""
    return quantity > 40000 and random.random() < 0.1  # 10% chance on large orders

def simulate_orders():
    print(f"🚀 BVRD Fraud Detection — publishing to: {INPUT_TOPIC}")
    while True:
        for ticker in TICKERS:
            # Simulate price movement
            change = round(random.uniform(-8, 8), 2)
            prices[ticker] = max(10, round(prices[ticker] + change, 2))

            broker_id = random.choice(BROKERS)
            side      = random.choice(["BUY", "SELL"])
            quantity  = random.randint(100, 50000)
            order_id  = str(uuid.uuid4())

            # Build order event
            order = {
                "order_id":   order_id,
                "ticker":     ticker,
                "broker_id":  broker_id,
                "side":       side,
                "price":      prices[ticker],
                "quantity":   quantity,
                "timestamp":  int(time.time()),
            }

            # Publish raw order to input topic
            producer.produce(
                INPUT_TOPIC,
                key=ticker,
                value=json.dumps(order).encode("utf-8"),
                callback=delivery_report,
            )

            # ── Fraud detection logic ──────────────────────────────────────
            fraud_flags = []

            if is_wash_trade(ticker, broker_id, side):
                fraud_flags.append("WASH_TRADE")

            if is_spoofing(ticker, quantity, side):
                fraud_flags.append("SPOOFING")

            if abs(change) > 6:
                fraud_flags.append("PRICE_MANIPULATION")

            if fraud_flags:
                alert = {
                    "alert_id":    str(uuid.uuid4()),
                    "order_id":    order_id,
                    "ticker":      ticker,
                    "broker_id":   broker_id,
                    "fraud_types": fraud_flags,
                    "severity":    "HIGH" if len(fraud_flags) > 1 else "MEDIUM",
                    "price":       prices[ticker],
                    "quantity":    quantity,
                    "timestamp":   int(time.time()),
                }
                producer.produce(
                    OUTPUT_TOPIC,
                    key=ticker,
                    value=json.dumps(alert).encode("utf-8"),
                    callback=delivery_report,
                )
                print(f"🚨 FRAUD ALERT [{ticker}] {fraud_flags}")

            # Store in history
            order_history[ticker].append({"broker_id": broker_id, "side": side})
            if len(order_history[ticker]) > 20:
                order_history[ticker].pop(0)

            producer.poll(0)

        producer.flush()
        time.sleep(1)

if __name__ == "__main__":
    simulate_orders()