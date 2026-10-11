#!/usr/bin/env bash

PID_FILE=".platform.pids"
PROJECT_DIR="$HOME/financial-market-data-platform"

activate_env() {
    cd "$PROJECT_DIR" || exit 1
    if [ -f "venv/bin/activate" ]; then
        source venv/bin/activate
    else
        echo "[ERROR] Python virtual environment (venv) not found!"
        exit 1
    fi
}

start_platform() {
    activate_env
    echo "============================================================"
    echo "  STARTING INSTITUTIONAL MARKET DATA PLATFORM               "
    echo "============================================================"

    # 1. Boot Infrastructure Containers
    echo "[1/5] Launching Kafka, PostgreSQL, and Apache Flink..."
    docker compose up -d
    sleep 2

    # Clean ports and allow OS socket cooldown
    echo "Releasing lingering network sockets..."
    fuser -k -9 8000/tcp 2>/dev/null
    fuser -k -9 8501/tcp 2>/dev/null
    sleep 2

    # 2. Launch FastAPI Gateway
    echo "[2/5] Starting Serving Gateway (Port 8000)..."
    nohup python3 -m uvicorn api_gateway:app --host 0.0.0.0 --port 8000 > uvicorn.log 2>&1 &
    UVICORN_PID=$!

    # 3. Launch Market Data Producer
    echo "[3/5] Starting Market Data Producer..."
    nohup python3 market_producer.py > producer.log 2>&1 &
    PRODUCER_PID=$!

    # 4. Launch Quantitative Strategy Engine
    echo "[4/5] Starting Quantitative Strategy Engine..."
    nohup python3 quant_strategy.py > strategy.log 2>&1 &
    STRATEGY_PID=$!

    # 5. Launch Streamlit Analytics Cockpit
    echo "[5/5] Starting Streamlit Analytics Cockpit (Port 8501)..."
    nohup streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0 > dashboard.log 2>&1 &
    STREAMLIT_PID=$!

    echo "$UVICORN_PID $PRODUCER_PID $STRATEGY_PID $STREAMLIT_PID" > "$PID_FILE"

    # Active Polling: Wait for API Gateway neural weights to load and listen (up to 25s)
    echo "Warming up Sentence Transformers & API Gateway..."
    GATEWAY_READY=false
    for i in {1..25}; do
        if curl -s "http://localhost:8000/health" | grep -q "ONLINE"; then
            GATEWAY_READY=true
            break
        fi
        sleep 1
    done

    echo "============================================================"
    if [ "$GATEWAY_READY" = true ]; then
        echo "  CLUSTER ONLINE & OPERATIONAL                              "
        echo "  - Serving API Gateway  : http://localhost:8000/docs       "
        echo "  - Streamlit Cockpit    : http://localhost:8501            "
    else
        echo "  [WARNING] Gateway took longer than expected to bind.     "
        echo "  Inspect uvicorn.log for diagnostic logs.                  "
    fi
    echo "============================================================"
}

stop_platform() {
    echo "============================================================"
    echo "  STOPPING INSTITUTIONAL MARKET DATA PLATFORM               "
    echo "============================================================"

    if [ -f "$PID_FILE" ]; then
        PIDS=$(cat "$PID_FILE")
        echo "Terminating platform daemons: $PIDS"
        kill -9 $PIDS 2>/dev/null
        rm -f "$PID_FILE"
    fi

    fuser -k -9 8000/tcp 2>/dev/null
    fuser -k -9 8501/tcp 2>/dev/null
    pkill -9 -f market_producer.py 2>/dev/null
    pkill -9 -f quant_strategy.py 2>/dev/null

    echo "Hibernating Docker containers..."
    cd "$PROJECT_DIR" && docker compose stop

    echo "[✓] All platform services safely stopped."
}

status_platform() {
    activate_env
    python3 test_platform.py
}

case "$1" in
    start)
        start_platform
        ;;
    stop)
        stop_platform
        ;;
    restart)
        stop_platform
        sleep 2
        start_platform
        ;;
    status)
        status_platform
        ;;
    *)
        echo "Usage: ./manage.sh {start|stop|restart|status}"
        exit 1
        ;;
esac