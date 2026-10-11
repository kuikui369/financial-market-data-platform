import json
import psycopg2
from kafka import KafkaConsumer

# Rolling benchmark VWAP state (derived from streaming windows)
BENCHMARK_VWAP = {
    "AAPL": 181.80,
    "MSFT": 415.00,
    "NVDA": 875.00,
    "TSLA": 250.00,
    "AMZN": 176.00
}

DEVIATION_THRESHOLD_PCT = 0.30  # 0.30% divergence triggers execution

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="market_ledger",
        user="quant_admin",
        password="quant_password"
    )

def start_strategy_engine():
    consumer = KafkaConsumer(
        'market-trades',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='quant-strategy-engine',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    conn = get_db_connection()
    cursor = conn.cursor()

    print("[STRATEGY] Quantitative Signal & Execution Engine ACTIVE.")
    print(f"[STRATEGY] Monitoring 'market-trades' with ±{DEVIATION_THRESHOLD_PCT}% VWAP Mean-Reversion threshold...\n")

    try:
        for message in consumer:
            trade = message.value
            ticker = trade['ticker']
            price = trade['price']
            trade_id = trade['tradeId']

            if ticker not in BENCHMARK_VWAP:
                continue

            vwap = BENCHMARK_VWAP[ticker]
            deviation_pct = ((price - vwap) / vwap) * 100.0

            signal = None
            if deviation_pct <= -DEVIATION_THRESHOLD_PCT:
                signal = "BUY"
            elif deviation_pct >= DEVIATION_THRESHOLD_PCT:
                signal = "SELL"

            if signal:
                # Commit executed algorithmic signal to ledger
                cursor.execute("""
                    INSERT INTO trade_executions 
                    (ticker, signal_type, trade_price, benchmark_vwap, deviation_pct)
                    VALUES (%s, %s, %s, %s, %s);
                """, (ticker, signal, price, vwap, round(deviation_pct, 3)))
                conn.commit()

                indicator = "🟢" if signal == "BUY" else "🔴"
                print(f"{indicator} [SIGNAL {signal:<4}] {ticker} | Price: ${price:<7.2f} | VWAP: ${vwap:<7.2f} | Dev: {deviation_pct:+.2f}% | TradeID: {trade_id}")

    except KeyboardInterrupt:
        print("\n[STRATEGY] Shutting down execution engine...")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    start_strategy_engine()