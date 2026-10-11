CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE securities_master (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(100) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    currency VARCHAR(5) DEFAULT 'USD'
);

INSERT INTO securities_master (ticker, company_name, exchange) VALUES
('AAPL', 'Apple Inc.', 'NASDAQ'),
('TSLA', 'Tesla Inc.', 'NASDAQ'),
('NVDA', 'NVIDIA Corporation', 'NASDAQ');

CREATE TABLE corporate_actions (
    action_id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) REFERENCES securities_master(ticker),
    split_factor NUMERIC(10, 4) NOT NULL,
    effective_date DATE NOT NULL
);

INSERT INTO corporate_actions (ticker, split_factor, effective_date)
VALUES ('AAPL', 0.5000, '2026-10-01');

CREATE TABLE official_exchange_settlement (
    settlement_date DATE,
    ticker VARCHAR(10),
    official_volume BIGINT
);

CREATE TABLE pipeline_ingested_volume (
    settlement_date DATE,
    ticker VARCHAR(10),
    ingested_volume BIGINT
);

INSERT INTO official_exchange_settlement VALUES ('2026-10-09', 'AAPL', 5000000);
INSERT INTO pipeline_ingested_volume VALUES ('2026-10-09', 'AAPL', 4998200);

CREATE TABLE market_news_rag (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    headline TEXT NOT NULL,
    embedding vector(384)
);

CREATE INDEX ON market_news_rag USING hnsw (embedding vector_cosine_ops);