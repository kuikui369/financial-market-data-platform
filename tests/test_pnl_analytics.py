"""
Institutional Test Suite: FIFO Realized PnL & Position Analytics
Verifies accounting correctness, lot exhaustion, and inventory invariants.
"""
import pytest
from pnl_analytics import calculate_fifo_pnl


def test_empty_executions_returns_neutral_portfolio():
    """Validates boundary handling when no executions exist in the database."""
    metrics = calculate_fifo_pnl([])
    assert metrics["total_executions_processed"] == 0
    assert metrics["total_closed_trades"] == 0
    assert metrics["total_realized_pnl"] == 0.0
    assert metrics["win_rate_pct"] == 0.0
    assert metrics["pnl_by_ticker"] == {}
    assert metrics["net_open_inventory"] == {}
    assert metrics["recent_closed_trades"] == []


def test_single_round_trip_profitable_trade():
    """Tests a standard long trade: BUY 100 @ 150, SELL 100 @ 155 -> +$500 PnL."""
    executions = [
        {"ticker": "AAPL", "side": "BUY", "price": 150.0, "shares": 100, "timestamp": "2026-10-12 10:00:00"},
        {"ticker": "AAPL", "side": "SELL", "price": 155.0, "shares": 100, "timestamp": "2026-10-12 10:05:00"},
    ]
    metrics = calculate_fifo_pnl(executions)

    assert metrics["total_executions_processed"] == 2
    assert metrics["total_closed_trades"] == 1
    assert metrics["total_realized_pnl"] == 500.0
    assert metrics["win_rate_pct"] == 100.0
    assert metrics["pnl_by_ticker"]["AAPL"] == 500.0
    assert metrics["net_open_inventory"]["AAPL"] == 0


def test_multi_lot_fifo_partial_allocations():
    """
    Tests FIFO queue ordering with layered purchases:
    - Lot 1: BUY 100 @ 200
    - Lot 2: BUY 100 @ 220
    - Close: SELL 150 @ 210
      -> 100 shares matched against Lot 1: (210 - 200) * 100 = +$1000
      -> 50 shares matched against Lot 2: (210 - 220) * 50 = -$500
      -> Total Realized PnL: +$500
      -> Remaining Inventory: 50 shares
    """
    executions = [
        {"ticker": "NVDA", "side": "BUY", "price": 200.0, "shares": 100, "timestamp": "2026-10-12 10:00:00"},
        {"ticker": "NVDA", "side": "BUY", "price": 220.0, "shares": 100, "timestamp": "2026-10-12 10:01:00"},
        {"ticker": "NVDA", "side": "SELL", "price": 210.0, "shares": 150, "timestamp": "2026-10-12 10:05:00"},
    ]
    metrics = calculate_fifo_pnl(executions)

    assert metrics["total_closed_trades"] == 2
    assert metrics["total_realized_pnl"] == 500.0
    assert metrics["pnl_by_ticker"]["NVDA"] == 500.0
    assert metrics["net_open_inventory"]["NVDA"] == 50


def test_multi_ticker_isolation_and_win_rate():
    """Verifies that executions across distinct tickers never cross-contaminate queues."""
    executions = [
        # Win on MSFT: +$1,000
        {"ticker": "MSFT", "side": "BUY", "price": 300.0, "shares": 100, "timestamp": "2026-10-12 09:30:00"},
        {"ticker": "MSFT", "side": "SELL", "price": 310.0, "shares": 100, "timestamp": "2026-10-12 09:35:00"},
        # Loss on TSLA: -$600
        {"ticker": "TSLA", "side": "BUY", "price": 250.0, "shares": 100, "timestamp": "2026-10-12 09:40:00"},
        {"ticker": "TSLA", "side": "SELL", "price": 244.0, "shares": 100, "timestamp": "2026-10-12 09:45:00"},
    ]
    metrics = calculate_fifo_pnl(executions)

    assert metrics["total_closed_trades"] == 2
    assert metrics["total_realized_pnl"] == 400.0
    assert metrics["win_rate_pct"] == 50.0
    assert metrics["pnl_by_ticker"]["MSFT"] == 1000.0
    assert metrics["pnl_by_ticker"]["TSLA"] == -600.0
    assert metrics["net_open_inventory"]["MSFT"] == 0
    assert metrics["net_open_inventory"]["TSLA"] == 0