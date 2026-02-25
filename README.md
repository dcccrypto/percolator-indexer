# @percolator/indexer

Percolator Indexer — market discovery, stats collection, trade indexing, and insurance LP tracking for Percolator perpetual futures on Solana.

## Services

- **MarketDiscovery** — Discovers all on-chain Percolator markets across program IDs
- **StatsCollector** — Collects and stores market statistics (OI, volume, funding rates)
- **TradeIndexer** — Indexes trade events from on-chain transactions
- **InsuranceLPService** — Tracks insurance LP deposits and redemptions
- **HeliusWebhookManager** — Manages Helius webhooks for real-time event streaming

## Quick Start

```bash
pnpm install
cp .env.example .env
# Edit .env
pnpm build
pnpm start
```

## Testing

```bash
pnpm test
```

## Deployment

### Railway
```bash
railway link
railway up
```

### Docker
```bash
docker build -t percolator-indexer .
docker run --env-file .env percolator-indexer
```

## License

Apache-2.0
