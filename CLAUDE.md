# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Matriz Twin is a Python application that connects to the Matriz/EcoBolsar trading platform (Argentine stock broker) via WebSocket to receive real-time market data and manage trading orders. It provides a FastAPI-based web monitor for observing connection status and market data.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the web monitor (starts on port 8000 by default)
python web/app.py

# Or with uvicorn directly
uvicorn web.app:app --host 0.0.0.0 --port 8000
```

## Environment Configuration

Copy `.env.example` to `.env` and configure:
- `MATRIZ_USERNAME` / `MATRIZ_PASSWORD`: Broker credentials (required)
- `MATRIZ_HOST`: Broker host (default: matriz.eco.xoms.com.ar)
- `MONITOR_PORT`: Web monitor port (default: 8000)
- `HEALTH_CHECK_INTERVAL`: Health check frequency in seconds (default: 5)

## Architecture

### Core Components

**Session & HTTP Layer** (`service/`)
- `MatrizSession`: Handles authentication with the broker, manages session_id and conn_id
- `MatrizRequester`: HTTP client with CSRF token management and cookie handling

**WebSocket Listeners** (`ws_listener/`)
- `MatrizWebsocketListener`: Abstract base class with connection management and auto-reconnection
- `MatrizPriceListener`: Subscribes to market data (book + md topics) for configured tickers
- `MatrizOrderListener`: Subscribes to order events for the account

**Message Processing** (`ws_listener/message_processor/`)
- `BaseMessageProcessor`: FIFO queue with single consumer. All messages processed in order, none lost
- `MonitorMessageProcessor`: For price data. Logs to `logs/ws/price-*.txt`, notifies UI via callbacks
- `OrderMessageProcessor`: For order events. Logs to `logs/ws/order-*.txt`, notifies UI via callbacks

**Order Management** (`service/order_service.py`, `model/order_model.py`, `repository/order_repository.py`)
- Supports market, limit, and caution orders
- Dual-mode operation: `prod` (sends to broker) or `paper` (local only)
- SQLite persistence in `data/orders.db`

**Monitoring** (`monitoring/`)
- `HealthChecker`: Periodic health checks for network (Google, Cloudflare, GitHub) and Matriz host
- `DisconnectionLogger`: Tracks and logs connection state changes

**Web Interface** (`web/app.py`)
- FastAPI with WebSocket support for real-time UI updates
- Three modes: `prod`, `paper` (live data, paper orders), `test` (replay from log files)
- Replay system reads from `logs/ws/*.txt` files with timing preservation

### Message Flow

1. `MatrizSession.login()` authenticates and obtains session_id/conn_id
2. Listener connects to `wss://matriz.eco.xoms.com.ar/ws` with session params
3. Subscribes to book/md topics for configured tickers (default: AL30 CI/24hs)
4. Messages are queued in `BaseMessageProcessor` FIFO queue
5. Single consumer processes all messages in order of arrival (no messages lost)

### Key Patterns

- Message types: `M:` (market data), `B:` (order book), `O:/E:` (order events)
- Instrument format: `bm_MERV_{ticker}_{term}` (e.g., `bm_MERV_AL30_CI`)
- Order status flow: placed → confirmed → partial → filled (or canceled → confirmed_canceled)
