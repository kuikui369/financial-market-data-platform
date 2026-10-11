# 🏛️ Institutional Market Data & Quantitative Execution Platform

An exchange-agnostic, distributed event-driven market data pipeline and quantitative execution platform built with Apache Flink, KRaft Kafka, PostgreSQL 16 with pgvector, FastAPI, and Streamlit.

---

## 📐 System Architecture

```text
[ Market Feed (Chaos Ticks) ] ──► [ KRaft Kafka (market-trades) ]
                                          │
            ┌─────────────────────────────┴─────────────────────────────┐
            ▼                                                           ▼
[ Apache Flink 1.18.1 (Java 17) ]                           [ quant_strategy.py (Python) ]
  - 1-Minute Sliding Event-Time VWAP                          - Mean-Reversion Threshold Engine
  - Late-Arriving Trade Side Output Audits                     - ±0.30% Divergence Band Execution
            │                                                           │
            └─────────────────────────────┬─────────────────────────────┘
                                          ▼
                      [ PostgreSQL 16 Enterprise Ledger ]
                        - trade_executions (Transactional Signals)
                        - market_news_rag (HNSW pgvector Embeddings)
                        - official_exchange_settlement (Audits)
                                          │
            ┌─────────────────────────────┴─────────────────────────────┐
            ▼                                                           ▼
[ api_gateway.py (FastAPI Serving) ]                        [ pnl_analytics.py (FIFO Engine) ]
  - Asynchronous High-Throughput REST                         - FIFO Cost-Basis Matching
  - Cosine Distance Semantic News RAG                         - Mark-to-Market Realized PnL
                                          │
                                          ▼
                  [ dashboard.py (Streamlit Visual Cockpit) ]
                    - Live Execution Telemetry & Deviation Bands
                    - Portfolio Realized PnL & Win Rate Tracking
                    - Clearinghouse Settlement Reconciliations
                    - Semantic News Intelligence Search
```

---

## ⚡ Core Engineering Highlights

1. **Stateful Stream Processing ($O(1)$ Sliding VWAP):**
   * Implemented in Apache Flink 1.18.1 using `AggregateFunction<TradeEvent, VwapAccumulator, VwapResult>` to maintain constant memory bounds per sliding window:
   $$\text{VWAP} = \frac{\sum (P_i \times V_i)}{\sum V_i}$$
   * Bounded out-of-order watermarking with side outputs capturing late-arriving ticks for clearinghouse audits.

2. **Autonomous Mean-Reversion Execution:**
   * Continuous pricing deviation evaluated against real-time VWAP:
   $$\text{Deviation (\%)} = \frac{\text{Price} - \text{VWAP}}{\text{VWAP}} \times 100$$
   * Automated order triggers: $\le -0.30\%$ generates 🟢 BUY signals, $\ge +0.30\%$ generates 🔴 SELL signals. Over 11,000 executions audited and committed to PostgreSQL.

3. **FIFO Portfolio Accounting Engine:**
   * Realized profit and loss calculated via First-In, First-Out (FIFO) share lot matching:
   $$\text{Trade PnL} = (\text{Price}_{\text{SELL}} - \text{Cost Basis}_{\text{BUY}}) \times \text{Shares}$$

4. **Vector Retrieval-Augmented Intelligence (RAG):**
   * Semantic embedding using `sentence-transformers/all-MiniLM-L6-v2` (384 dimensions) indexed with PostgreSQL `pgvector` Hierarchical Navigable Small World (HNSW) cosine distance.

---

## 🚀 Quickstart & Lifecycle Management

```bash
# Start all containers, background engines, API gateway, and dashboard:
./manage.sh start

# Verify automated platform health audit:
./manage.sh status

# Gracefully terminate daemons and hibernate Docker cluster:
./manage.sh stop
```

---

## 📡 REST API Specifications

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/health` | Cluster gateway operational status |
| `GET` | `/v1/signals?limit=50` | Recent algorithmic mean-reversion order executions |
| `GET` | `/v1/portfolio?lot_size=100` | Realized PnL, inventory positions, and strategy win rate |
| `GET` | `/v1/reconciliation?ticker=AAPL` | Clearinghouse volume reconciliation audits |
| `GET` | `/v1/news/semantic-search?query=...` | HNSW pgvector cosine distance news retrieval |