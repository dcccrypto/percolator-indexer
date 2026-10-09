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
- **Wrapper log events** (fills, reductions, value moves; wrapper `docs/v22-fill-events.md`, release/v22-wrapper-rem c6ee0b6e) are decoded from
  `meta.logMessages` by `src/parsers/v22FillEvents.ts` and written to the same table with kinds `fill_event` / `reduce_event` / `move_event`
  (MOVE subs 1..4 incl. `g9_restore_pnl`) by `supabase/migrations/20261009120000_v22_log_events.sql` (**applied by hand, after the first
  migration**; until then the log events are dropped with one error per 10 minutes and the instruction events are unaffected). Rules:
  successful transactions only; each log element is one atomic line; a frame is pushed only on `Program <canonical key> invoke [depth = stack + 1]`
  and popped only on the top-of-stack id, and a `Program data:` line counts only inside a pinned wrapper frame (`config.allProgramIds`), so a
  matcher cannot forge an event; any inconsistency, `Log truncated` or null logs make the transaction's events **unknown**, recorded as an
  `events_unknown` marker (only for a market known to be VERSION 19) so its positions can be reconciled from account state, never as "no fill";
  unknown kinds / versions / subs / reasons are skipped and counted. Volume is `executed_q` at `price_e6` only (`notional_e6`);
  `requested_q` and the matcher's quote are stored as `*_untrusted`; `fee_atoms_total` is both portfolios' fee, not revenue. Events are written
  by the Atlas stream and the poll path (jsonParsed meta carries the logs) and by the webhook only when a delivery carries `logMessages`
  (the enhanced payload normally does not). Log-event rows use `inner_index = 1,000,000 + ordinal`, so the unique key is unchanged.
  Not recorded: tags 74 / 117 / 120 / 121 / 122 and the G9 allowlist account (governance, not activity).
- Discovery and the light pass: the light registration slice is derived from the SDK table as the largest registration prefix over every VERSION
  (v2.1 1,862 B, v2.2 1,910 B = 592 + 806 + 512), so a v2.2 market of any capacity (4,059 / 6,720 / 9,381 / 12,042 / 14,703 B) registers
  with the same fields from the slice as from the whole account.
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
