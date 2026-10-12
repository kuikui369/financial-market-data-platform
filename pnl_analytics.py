"""
FIFO Realized PnL & Position Analytics Engine
Decoupled functional core for deterministic testing and live PostgreSQL ledger processing.
"""
import psycopg2
from collections import defaultdict, deque


def calculate_fifo_pnl(executions, default_lot_size: int = 100):
    """
    Pure deterministic FIFO accounting engine.
    Accepts an iterable of executions (dicts or DB tuples) and calculates:
    - Realized Profit & Loss per ticker and portfolio
    - Strategy Win Rate %
    - Remaining open inventory positions
    """
    if not executions:
        return {
            "total_executions_processed": 0,
            "total_closed_trades": 0,
            "total_realized_pnl": 0.0,
            "win_rate_pct": 0.0,
            "pnl_by_ticker": {},
            "net_open_inventory": {},
            "recent_closed_trades": []
        }

    buy_inventory = defaultdict(deque)
    realized_pnl_by_ticker = defaultdict(float)
    closed_trades = []
    total_processed = 0

    for exec_item in executions:
        total_processed += 1
        if isinstance(exec_item, dict):
            ticker = exec_item.get("ticker")
            side = exec_item.get("side") or exec_item.get("signal_type")
            price = float(exec_item.get("price") or exec_item.get("trade_price"))
            shares = int(exec_item.get("shares", default_lot_size))
            timestamp = str(exec_item.get("timestamp") or exec_item.get("executed_at", ""))
        else:
            # Tuple from PostgreSQL: (execution_id, ticker, signal_type, trade_price, executed_at)
            _, ticker, side, price, timestamp = exec_item
            price = float(price)
            shares = default_lot_size
            timestamp = str(timestamp)

        if side == "BUY":
            buy_inventory[ticker].append([price, shares])
        elif side == "SELL":
            shares_to_sell = shares
            while shares_to_sell > 0 and buy_inventory[ticker]:
                earliest_buy = buy_inventory[ticker][0]
                buy_price, buy_qty = earliest_buy

                fill_shares = min(shares_to_sell, buy_qty)
                trade_pnl = round((price - buy_price) * fill_shares, 2)
                realized_pnl_by_ticker[ticker] += trade_pnl

                closed_trades.append({
                    "ticker": ticker,
                    "buy_price": buy_price,
                    "sell_price": price,
                    "shares": fill_shares,
                    "pnl": trade_pnl,
                    "closed_at": timestamp
                })

                shares_to_sell -= fill_shares
                if fill_shares == buy_qty:
                    buy_inventory[ticker].popleft()
                else:
                    earliest_buy[1] -= fill_shares

    total_closed = len(closed_trades)
    profitable_trades = sum(1 for t in closed_trades if t["pnl"] > 0)
    win_rate = round((profitable_trades / total_closed) * 100.0, 2) if total_closed > 0 else 0.0
    total_pnl = round(sum(realized_pnl_by_ticker.values()), 2)

    net_inventory = {}
    for ticker, dq in buy_inventory.items():
        total_open_shares = sum(lot[1] for lot in dq)
        if total_open_shares > 0 or ticker in realized_pnl_by_ticker:
            net_inventory[ticker] = total_open_shares

    return {
        "total_executions_processed": total_processed,
        "total_closed_trades": total_closed,
        "total_realized_pnl": total_pnl,
        "win_rate_pct": win_rate,
        "pnl_by_ticker": dict(realized_pnl_by_ticker),
        "net_open_inventory": net_inventory,
        "recent_closed_trades": closed_trades[-20:]
    }


def compute_portfolio_pnl(lot_size: int = 100):
    """
    Imperative Database Wrapper:
    Fetches raw execution rows from PostgreSQL and delegates calculation
    to calculate_fifo_pnl. Preserves exact compatibility with api_gateway.py.
    """
    try:
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
    except Exception as e:
        return {
            "error": f"Database query failed: {str(e)}",
            "total_executions_processed": 0,
            "total_closed_trades": 0,
            "total_realized_pnl": 0.0,
            "win_rate_pct": 0.0,
            "pnl_by_ticker": {},
            "net_open_inventory": {},
            "recent_closed_trades": []
        }

    return calculate_fifo_pnl(executions, default_lot_size=lot_size)