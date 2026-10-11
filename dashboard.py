import streamlit as st
import pandas as pd
import requests
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Institutional Market Data Cockpit",
    page_icon="📈",
    layout="wide"
)

API_BASE = "http://localhost:8000"

st.title("🏛️ Institutional Market Data & Quantitative Execution Cockpit")
st.markdown("Real-time telemetry from Apache Flink, KRaft Kafka, pgvector, and FastAPI.")

# 1. Cluster Status Badges
col_h1, col_h2, col_h3 = st.columns(3)

try:
    health_res = requests.get(f"{API_BASE}/health", timeout=2).json()
    col_h1.metric("Gateway Cluster Status", health_res.get("status", "OFFLINE"), "Active 8000")
except Exception:
    col_h1.metric("Gateway Cluster Status", "OFFLINE", "-Disconnected", delta_color="inverse")

# 2. Fetch Recent Algorithmic Signals
signals_data = []
try:
    sig_res = requests.get(f"{API_BASE}/v1/signals?limit=50", timeout=2).json()
    signals_data = sig_res.get("signals", [])
except Exception as e:
    st.error(f"Failed to fetch trading signals from API Gateway: {e}")

df_signals = pd.DataFrame(signals_data)

if not df_signals.empty:
    col_h2.metric("Total Signals Loaded", len(df_signals))
    buy_count = (df_signals["signal"] == "BUY").sum()
    sell_count = (df_signals["signal"] == "SELL").sum()
    col_h3.metric("Signal Distribution", f"{buy_count} BUY / {sell_count} SELL")

    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Live Execution Telemetry",
        "💰 Portfolio & PnL",
        "⚖️ Clearinghouse Reconciliation",
        "🔍 Semantic News Intelligence (RAG)"
    ])

    # ----------------------------------------------------
    # TAB 1: Live Execution Telemetry
    # ----------------------------------------------------
    with tab1:
        st.subheader("Price vs. Benchmark VWAP Deviation Spread")
        fig_scatter = px.scatter(
            df_signals,
            x="trade_price",
            y="deviation_pct",
            color="signal",
            color_discrete_map={"BUY": "#00CC96", "SELL": "#EF553B"},
            hover_data=["ticker", "benchmark_vwap", "executed_at"],
            title="Executed Mean-Reversion Divergence (%) across Market Ticks"
        )
        fig_scatter.add_hline(y=0.3, line_dash="dash", line_color="red", annotation_text="Upper SELL Threshold (+0.30%)")
        fig_scatter.add_hline(y=-0.3, line_dash="dash", line_color="green", annotation_text="Lower BUY Threshold (-0.30%)")
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.subheader("Recent Execution Ledger")
        st.dataframe(df_signals, use_container_width=True)

    # ----------------------------------------------------
    # TAB 2: Portfolio & PnL
    # ----------------------------------------------------
    with tab2:
        st.subheader("Quantitative Strategy Performance & Realized PnL")
        try:
            port_res = requests.get(f"{API_BASE}/v1/portfolio", timeout=5).json()
            
            p_col1, p_col2, p_col3, p_col4 = st.columns(4)
            p_col1.metric("Cumulative Realized PnL", f"${port_res.get('total_realized_pnl', 0):,.2f}")
            p_col2.metric("Strategy Win Rate", f"{port_res.get('win_rate_pct', 0)}%")
            p_col3.metric("Round-Trip Trades", port_res.get("total_closed_trades", 0))
            p_col4.metric("Audited Executions", port_res.get("total_executions_processed", 0))

            st.write("---")
            pnl_breakdown = port_res.get("pnl_by_ticker", {})
            if pnl_breakdown:
                df_pnl = pd.DataFrame(list(pnl_breakdown.items()), columns=["Ticker", "Realized_PnL"])
                fig_pnl = px.bar(
                    df_pnl,
                    x="Ticker",
                    y="Realized_PnL",
                    color="Realized_PnL",
                    color_continuous_scale=["#EF553B", "#00CC96"],
                    title="Realized PnL Attribution by Security ($)"
                )
                st.plotly_chart(fig_pnl, use_container_width=True)

            st.subheader("Recent Closed FIFO Trades")
            closed_trades = port_res.get("recent_closed_trades", [])
            if closed_trades:
                st.dataframe(pd.DataFrame(closed_trades), use_container_width=True)
            else:
                st.info("No round-trip closed trades yet (requires both BUY and SELL executions on the same security to close a FIFO cycle).")
        except Exception as e:
            st.error(f"Failed to fetch portfolio analytics from API Gateway: {e}")

    # ----------------------------------------------------
    # TAB 3: Clearinghouse Reconciliation
    # ----------------------------------------------------
    with tab3:
        st.subheader("Daily Exchange Settlement Audit")
        ticker_filter = st.selectbox("Filter by Security", ["AAPL", "All"])
        url = f"{API_BASE}/v1/reconciliation" if ticker_filter == "All" else f"{API_BASE}/v1/reconciliation?ticker={ticker_filter}"
        
        try:
            recon_res = requests.get(url, timeout=2).json()
            df_recon = pd.DataFrame(recon_res.get("records", []))
            if not df_recon.empty:
                st.dataframe(df_recon, use_container_width=True)
                
                fig_bar = px.bar(
                    df_recon,
                    x="ticker",
                    y=["official_volume", "ingested_volume"],
                    barmode="group",
                    title="Official Exchange Volume vs. Pipeline Ingested Volume"
                )
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("No reconciliation records found for specified security.")
        except Exception as e:
            st.error(f"Reconciliation error: {e}")

    # ----------------------------------------------------
    # TAB 4: Semantic News Intelligence (RAG)
    # ----------------------------------------------------
    with tab4:
        st.subheader("pgvector Semantic Similarity News Search")
        search_query = st.text_input("Enter natural language query:", "artificial intelligence hardware and chips")
        limit_val = st.slider("Result Count", 1, 5, 2)
        
        if st.button("Execute Vector Search"):
            try:
                news_res = requests.get(f"{API_BASE}/v1/news/semantic-search?query={search_query}&limit={limit_val}", timeout=5).json()
                matches = news_res.get("matches", [])
                for item in matches:
                    with st.expander(f"[{item['ticker']}] Cosine Similarity: {item['cosine_similarity']}"):
                        st.write(f"**Headline:** {item['headline']}")
            except Exception as e:
                st.error(f"Semantic search query failed: {e}")
else:
    st.warning("No signals found in trade_executions table. Ensure quant_strategy.py has emitted trades.")