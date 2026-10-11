import json
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

# 1. Connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

TICKERS = ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN"]
PRICES = {
    "AAPL": 182.50,
    "TSLA": 245.00,
    "NVDA": 880.00,
    "MSFT": 420.00,
    "AMZN": 178.00
}

trade_id = 1
print("Market Producer online. Emitting live trade stream with 10% chaos lag...")

try:
    while True:
        ticker = random.choice(TICKERS)
        
        # Fluctuate prices slightly (-0.3% to +0.3%)
        price_change_factor = 1 + random.uniform(-0.003, 0.003)
        PRICES[ticker] = round(PRICES[ticker] * price_change_factor, 2)
        price = PRICES[ticker]
        
        volume = random.choice([100, 200, 500, 1000, 2500])
        current_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        
        # 10% chance of an 8-second delayed trade (Watermark Chaos Injection)
        is_delayed = random.random() < 0.10
        event_time = current_ms - 8000 if is_delayed else current_ms

        payload = {
            "tradeId": trade_id,
            "ticker": ticker,
            "price": price,
            "volume": volume,
            "eventTime": event_time
        }

        producer.send('market-trades', key=ticker, value=payload)
        
        if is_delayed:
            print(f"⚠️ [CHAOS INJECTED] Late trade: {ticker} (Lag: 8,000ms | Trade ID: {trade_id})")
        else:
            print(f"Tick -> {ticker}: ${price:.2f} | Vol: {volume} | Trade ID: {trade_id}")

        trade_id += 1
        time.sleep(0.15)

except KeyboardInterrupt:
    print("\nStopping producer...")
    producer.flush()
    producer.close()
    print("Producer gracefully disconnected.")