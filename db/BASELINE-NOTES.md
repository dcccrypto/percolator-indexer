# Percolator v17 — DB baseline notes

`db/v17_baseline.sql` is ONE clean schema to apply to a **fresh Supabase project**,
replacing the 63 hand-applied migrations (several of which silently rolled back).
Apply it once in the new project's SQL editor, then point the indexer + frontend at it.

## Philosophy: index only on-chain EVENT HISTORY

All live state is on-chain and read directly via RPC (`parseMarketGroupV17OI`,
`parsePortfolioV17`, `parseWrapperConfigV17`). The indexer persists ONLY what a single
RPC read can't reconstruct — event history — and everything derived (candles, 24h
volume, leaderboard) is computed from it. This mirrors dYdX v4 / Drift / Hyperliquid:
live state from the chain, fills/events saved, aggregates derived.

## The 4 tables

| Table | Purpose |
|---|---|
| **`trades`** | one row per fill — the core. Drives candles, 24h volume, leaderboard, trade + portfolio history. |
| **`markets`** | registry + OFF-CHAIN metadata (symbol/name/logo/mainnet_ca) not in the slab. |
| **`market_stats`** | thin cache: 24h volume rollup derived from trades (so the market LIST shows volume without re-aggregating). |
| **`oracle_markets`** | per-market oracle configuration. |

Plus the `markets_with_stats` view (registry + volume cache).

**Dropped** (0 frontend readers / read live / chart history not needed): oracle_prices,
oi_history, insurance_history, funding_history, insurance_snapshots, insurance_lp_events,
adl_events, position_nft_events. The `AdlIndexer`, `NftIndexer`, `InsuranceLPService`
services were deleted, and `StatsCollector` was gutted to registry + volume only.

## The "0 trades" root cause (fixed by construction)

The old `network`-column migrations rolled back (each referenced a missing column later
in the same transaction), so `trades.network` never existed — and `insertTrade()` wrote
`network` with NO fallback, so every trade insert threw and was swallowed. The baseline
defines `network` **inline** on every table, so this can't recur. (Debug check on the OLD
project: `SELECT column_name FROM information_schema.columns WHERE table_name='trades'` —
if `network` is absent, that was the bug.)

## Fixes baked in

- **H2/H3** — `trades.asset_index` + `leg_index` (both `NOT NULL DEFAULT 0`) +
  `UNIQUE(tx_signature, asset_index, leg_index)` (partial, `WHERE tx_signature IS NOT NULL`).
  Multi-fill batch legs no longer collapse to one row.
- **Liquidations** — `is_liquidation` flag. v17 liquidations run through the crank
  (PermissionlessCrank tag 5, action 1) and expose **no size/price/side**, so markers are
  captured with those columns `NULL` (that's why `side`/`size`/`price` are nullable) and
  **excluded from volume/candles** (`WHERE is_liquidation = false`). The marker's `trader`
  is the liquidated portfolio address (wallet would need an extra account read).
- Dropped the broken `idx_trades_market_time` (on a nonexistent `timestamp` column).
- Added `idx_trades_trader_created (trader, created_at DESC)` + a BRIN on `created_at`.
- `market_stats` UNIQUE on `slab_address` alone (matches `upsertMarketStats` onConflict).

## Validation

Validated on PG16 (pglite): the DDL applies clean; a 3-leg batch persists as 3 rows;
a true duplicate leg is rejected; NULL-signature rows are exempt; a liquidation marker
(null side/size/price, `is_liquidation=true`) inserts and is excluded from the volume
query; the trader index + BRIN exist and the broken index does not.

To validate locally: `psql -v ON_ERROR_STOP=1 -f db/v17_baseline.sql` (create the Supabase
roles `anon`/`authenticated`/`service_role` first when testing outside Supabase).
