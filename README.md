# carry-bot

Funding rate arbitrage bot for Bybit: delta-neutral position (spot long + perp short), earning the periodic funding payments.

Built as a set of Go microservices over NATS — partly a trading experiment, partly a playground for microservices, messaging and observability (OpenTelemetry, Prometheus, Grafana).

```
market-data ──► strategy ──► order ──► portfolio ──► notification
                        (NATS / JetStream)
```

> Not financial advice. Paper-trade first (Bybit testnet), API keys with trade-only permissions, never commit secrets.
