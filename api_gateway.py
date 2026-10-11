import psycopg2
from fastapi import FastAPI, Query
from sentence_transformers import SentenceTransformer
from typing import Optional
from pnl_analytics import compute_portfolio_pnl

app = FastAPI(
    title="Institutional Market Data Platform API",
    description="Low-latency quantitative gateway serving VWAP metrics, ledger reconciliations, and semantic news retrieval.",
    version="1.0.0"
)

# Load lightweight 384-d sentence encoder
model = SentenceTransformer('all-MiniLM-L6-v2')

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="market_ledger",
        user="quant_admin",
        password="quant_password"
    )

@app.get("/health")
def health_check():
    """Confirms cluster gateway connectivity."""
    return {"status": "ONLINE", "layer": "Serving Gateway", "version": "1.0.0"}

@app.get("/v1/reconciliation")
def get_settlement_reconciliation(ticker: Optional[str] = None):
    """Retrieves exchange official vs pipeline volume discrepancies for trade audits."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        s.settlement_date,
        s.ticker,
        s.official_volume,
        p.ingested_volume,
        (s.official_volume - p.ingested_volume) AS discrepancy_volume,
        ROUND(((s.official_volume - p.ingested_volume)::numeric / s.official_volume) * 100, 4) AS discrepancy_pct
    FROM official_exchange_settlement s
    JOIN pipeline_ingested_volume p 
      ON s.settlement_date = p.settlement_date 
     AND s.ticker = p.ticker
    """
    params = []
    if ticker:
        query += " WHERE s.ticker = %s;"
        params.append(ticker.upper())
    else:
        query += ";"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    results = []
    for row in rows:
        results.append({
            "settlement_date": str(row[0]),
            "ticker": row[1],
            "official_volume": row[2],
            "ingested_volume": row[3],
            "discrepancy_volume": row[4],
            "discrepancy_pct": float(row[5])
        })
    return {"count": len(results), "records": results}

@app.get("/v1/news/semantic-search")
def search_market_news(query: str = Query(..., description="Natural language semantic search term"), limit: int = 3):
    """Encodes incoming query into vectors and executes HNSW cosine distance search on pgvector."""
    query_vector = model.encode(query).tolist()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query strictly the columns confirmed to exist in market_news_rag
    sql = """
    SELECT 
        ticker, 
        headline, 
        1 - (embedding <=> %s::vector) AS cosine_similarity
    FROM market_news_rag
    ORDER BY embedding <=> %s::vector
    LIMIT %s;
    """
    
    cursor.execute(sql, (str(query_vector), str(query_vector), limit))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    news_results = []
    for r in rows:
        news_results.append({
            "ticker": r[0],
            "headline": r[1],
            "cosine_similarity": round(float(r[2]), 4)
        })
    return {"query": query, "count": len(news_results), "matches": news_results}
@app.get("/v1/signals")
def get_recent_signals(limit: int = 10):
    """Retrieves the latest automated algorithmic trade signals executed by the strategy engine."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT execution_id, ticker, signal_type, trade_price, benchmark_vwap, deviation_pct, executed_at
        FROM trade_executions
        ORDER BY execution_id DESC
        LIMIT %s;
    """, (limit,))
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    signals = []
    for r in rows:
        signals.append({
            "execution_id": r[0],
            "ticker": r[1],
            "signal": r[2],
            "trade_price": float(r[3]),
            "benchmark_vwap": float(r[4]),
            "deviation_pct": float(r[5]),
            "executed_at": str(r[6])
        })
    return {"total_retrieved": len(signals), "signals": signals}
from pnl_analytics import compute_portfolio_pnl

@app.get("/v1/portfolio")
def get_portfolio_performance(lot_size: int = 100):
    """Calculates realized PnL, inventory positions, and win rate across trade executions."""
    return compute_portfolio_pnl(lot_size=lot_size)