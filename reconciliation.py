import psycopg2

def run_reconciliation():
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="market_ledger",
        user="quant_admin",
        password="quant_password"
    )
    cursor = conn.cursor()

    # Query exchange settlement vs pipeline ingested volume
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
     AND s.ticker = p.ticker;
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    print("\n=================== DAILY SETTLEMENT RECONCILIATION REPORT ===================")
    print(f"{'Date':<12} | {'Ticker':<6} | {'Official Vol':<14} | {'Ingested Vol':<14} | {'Discrepancy':<12} | {'Error %':<8}")
    print("-" * 80)
    for row in rows:
        date, ticker, official, ingested, disc, pct = row
        print(f"{str(date):<12} | {ticker:<6} | {official:<14} | {ingested:<14} | {disc:<12} | {pct}%")
    print("===============================================================================\n")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_reconciliation()