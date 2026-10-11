import psycopg2
from collections import defaultdict, deque

def compute_portfolio_pnl(lot_size: int = 100):
    """
    Connects to PostgreSQL, loads historical executions in chronological order,
    and calculates realized PnL and net open inventory using FIFO accounting.
    """
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="market_ledger",
        user="quant_admin",
        password="quant_password"
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT execution_id, ticker, signal_type, trade_price, executed_at
        FROM trade_executions
        ORDER BY execution_id ASC;
    """)
    executions = cursor.fetchall()
    cursor.close()
    conn.close()

    # FIFO queues: ticker -> deque of [price, remaining_shares]
    buy_inventory = defaultdict(deque)
    realized_pnl_by_ticker = defaultdict(float)
    closed_trades = []
    current_inventory = defaultdict(int)

    for exec_id, ticker, signal, price, timestamp in executions:
        price = float(price)

        if signal == "BUY":
            buy_inventory[ticker].append({"price": price, "shares": lot_size, "time": timestamp})
            current_inventory[ticker] += lot_size

        elif signal == "SELL":
            shares_to_sell = lot_size
            current_inventory[ticker] -= lot_size

            # Match against existing long inventory using FIFO
            while shares_to_sell > 0 and buy_inventory[ticker]:
                oldest_buy = buy_inventory[ticker][0]
                matched_shares = min(shares_to_sell, oldest_buy["shares"])

                # Calculate realized profit/loss
                gain = (price - oldest_buy["price"]) * matched_shares
                realized_pnl_by_ticker[ticker] += gain

                closed_trades.append({
                    "ticker": ticker,
                    "buy_price": oldest_buy["price"],
                    "sell_price": price,
                    "shares": matched_shares,
                    "pnl": round(gain, 2),
                    "closed_at": str(timestamp)
                })

                oldest_buy["shares"] -= matched_shares
                shares_to_sell -= matched_shares

                if oldest_buy["shares"] == 0:
                    buy_inventory[ticker].popleft()

    total_realized = sum(realized_pnl_by_ticker.values())
    winning_trades = [t for t in closed_trades if t["pnl"] > 0]
    win_rate = (len(winning_trades) / len(closed_trades) * 100) if closed_trades else 0.0

    return {
        "total_executions_processed": len(executions),
        "total_closed_trades": len(closed_trades),
        "total_realized_pnl": round(total_realized, 2),
        "win_rate_pct": round(win_rate, 2),
        "pnl_by_ticker": {k: round(v, 2) for k, v in realized_pnl_by_ticker.items()},
        "net_open_inventory": dict(current_inventory),
        "recent_closed_trades": closed_trades[-5:] if closed_trades else []
    }

if __name__ == "__main__":
    report = compute_portfolio_pnl()
    print("=" * 65)
    print("      QUANTITATIVE PORTFOLIO PERFORMANCE & PnL SUMMARY")
    print("=" * 65)
    print(f"Total Executions Audited : {report['total_executions_processed']}")
    print(f"Total Round-Trip Trades  : {report['total_closed_trades']}")
    print(f"Cumulative Realized PnL  : ${report['total_realized_pnl']:,.2f}")
    print(f"Strategy Win Rate        : {report['win_rate_pct']}%")
    print("-" * 65)
    print("Realized PnL by Security:")
    for ticker, pnl in report['pnl_by_ticker'].items():
        print(f"  [{ticker}] ${pnl:,.2f}")
    print("-" * 65)
    print("Net Open Share Inventory:")
    for ticker, shares in report['net_open_inventory'].items():
        print(f"  [{ticker}] {shares} shares")
    print("=" * 65)