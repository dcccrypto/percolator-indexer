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

## v2.2 support (wrapper VERSION 19)

- Account geometry is **VERSION-keyed**, from the SDK layout tables (`src/layout/resolve.ts`, `src/layout/portfolio.ts`): v2.1
  is VERSION 18 (header 758 B, slot 2,325 B, portfolio 9,563 B, leg 152 B), v2.2 variant B is VERSION 19 (806 / 2,661 /
  10,603 / 217). No offset is a literal in the indexer. An unknown VERSION is a loud, per-market skip (error log, counter
  `indexer_unknown_layout_total`, Sentry once) and never stops the other markets.
- New activity goes to the `v22_events` table (`supabase/migrations/20261007120000_v22_events.sql`, **applied by hand**, RLS on, no
  anon grants). Until it is applied the writer logs one error per 10 minutes and drops the events; nothing else is affected.
- `@percolatorct/sdk` is currently the vendored `vendor/percolatorct-sdk-9.0.0-candidate.tgz` (built from
  `dcccrypto/percolator-sdk#406`). Replace it with the published 9.x when it exists.

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
