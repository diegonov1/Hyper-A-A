# Binance Trading Bot

Binance Trading Bot is an AI-driven crypto trading platform focused on Binance USD-M Futures automation, with support for AI Trader and Program Trader workflows.

This repository is a fork and evolution of Hyper Alpha Arena, adapted to support real Binance trading flows while keeping compatibility with existing Hyperliquid integrations.

## Highlights

- Real trading adapter for Binance USD-M Futures:
  - Orders
  - Positions
  - Balance
  - Order cancellation
- AI Trader flow (LLM decisions)
- Program Trader flow (rule-based Python execution)
- Testnet-first execution workflow
- Multi-account architecture
- Docker-based local deployment

## Quick Start

### Prerequisites

- Docker Desktop (or Docker Engine + Compose)

### Run

```bash
git clone <your-fork-url> binance-trading-bot
cd binance-trading-bot
docker compose up -d --build
```

App URL:

```text
http://localhost:8802
```

### Common Commands

```bash
docker compose logs -f
docker compose restart
docker compose down
```

## Binance Demo Endpoints

The project includes Binance demo endpoints using environment variables:

- `DEMO_BINANCE_API_KEY`
- `DEMO_BINANCE_SECRET_KEY`

Main endpoints:

- `GET /api/binance/demo/config`
- `GET /api/binance/demo/test-connection`
- `GET /api/binance/demo/balance`
- `GET /api/binance/demo/positions`
- `GET /api/binance/demo/orders/open`
- `POST /api/binance/demo/orders/manual`
- `DELETE /api/binance/demo/orders/{order_id}`

## Notes

- Testnet is the default and recommended environment for validation.
- Mainnet should only be enabled after full end-to-end testing.

## License

Apache License 2.0. See `LICENSE`.

## Acknowledgments

- Forked from hyper-alpha-arena (Hyper Alpha Arena)
- Based on open-alpha-arena by etrobot: https://github.com/etrobot/open-alpha-arena
- Inspiration from nof1 Alpha Arena: https://nof1.ai
- Hyperliquid ecosystem and contributors
