# Percolator v17 — DB baseline notes

`db/v17_baseline.sql` is ONE clean, consolidated schema to apply to a **fresh Supabase project**,
replacing the 63 hand-applied migrations (several of which silently rolled back). Apply it once in the
new project's SQL editor, then point the indexer + frontend at the new project.

## The likely root cause of "trades aren't indexed"

The old `network`-column migrations (`20260329170000_..._PERC8192.sql`, `20260329180000_add_network_column.sql`)
each reference a **nonexistent column later in the same transaction** — so Supabase (which runs each file as
one transaction) **rolled the whole file back**:

- `170000` rebuilds `markets_with_stats` selecting `s.price_change_24h`, a column migration `005` dropped and
  never re-added → error → rollback.
- `180000` runs `CREATE INDEX ... insurance_snapshots(network, slab_address, timestamp DESC)`, but that table
  has `slab`/`created_at`, not `slab_address`/`timestamp` → error → the `ADD COLUMN network` in the same file
  rolls back too.

This is corroborated by defensive "network column missing — falling back to unfiltered query" branches the
engineers already shipped on the **read** side (`percolator-shared` `PERC-8215`, `app/api/stake/pools`
`PERC-8256` which literally says *"Apply 20260329180000 to fix"*).

**But `insertTrade()` writes `network: getNetwork()` unconditionally with NO fallback.** If `trades.network`
doesn't actually exist, **every trade INSERT throws** (and is swallowed/logged), so no trade is ever stored —
matching the observed "parsing is correct but 0 trades / volume 0 / candles 404".

➡️ **First debugging step on the OLD project:** `SELECT column_name FROM information_schema.columns WHERE
table_name='trades';` — if `network` is absent, that's the bug. **The new baseline fixes it by construction**
(`network` is defined inline on every table, never referenced across a table it hasn't created yet).

## Fixes baked into the baseline

| Fix | What |
|---|---|
| **H2/H3** | `trades` gains `asset_index` + `leg_index` (both `NOT NULL DEFAULT 0`) and `UNIQUE(tx_signature, asset_index, leg_index)` (partial, `WHERE tx_signature IS NOT NULL`). Multi-fill batch legs no longer collapse to one row. |
| **#3** | `network` present on every table, defined **inline** (no cross-table forward reference). |
| **L6/L15** | Dropped the broken `idx_trades_market_time` on `trades(timestamp)` — that column never existed. |
| **M5** | Added `idx_trades_trader_created (trader, created_at DESC)` for leaderboard/trader-history. |
| **L16** | Added a BRIN index on `trades(created_at)` for cheap time-range scans. |
| **M11** | `market_stats` gains `total_open_interest_usd`, `volume_24h_usd`, `active_positions`; all exposed through `markets_with_stats`. |
| **#1** | `funding_history`: ONE coherent definition (the live `009` shape) **plus** `created_at` (the TS type already assumed it). |
| **#2** | `insurance_snapshots` / `insurance_lp_events`: the `002` shape (`slab`/`user_wallet`), the one all live code speaks. `022`'s shape is dead — not resurrected. |
| **#5** | `adl_events` / `position_nft_events`: formalized from the code-comment schemas (`AdlIndexer.ts` / `NftIndexer.ts`) — they had NO migration file at all. |
| **units** | `market_stats.total_open_interest`/`volume_24h` remain raw base-asset Q for audit; the `*_usd` columns are the display values. |

`market_stats` UNIQUE stays on `slab_address` alone (matches `upsertMarketStats` `onConflict:"slab_address"`).

## What's included vs excluded

**Included** (trading / indexer data plane): `markets`, `market_stats`, `trades`, `oracle_prices`,
`oracle_markets`, `funding_history`, `insurance_history`, `oi_history`, `insurance_snapshots`,
`insurance_lp_events`, `adl_events`, `position_nft_events`, the views `markets_with_stats` /
`insurance_fund_health` / `oi_imbalance`, trigger/util functions, and RLS + column grants
(incl. the `trades` anon column restriction to `trader, size, created_at`).

**Excluded** (pure marketing/HR/utility — port from the old migrations if the new project also serves the
marketing site): `bug_reports`, `ideas`, `job_applications`, `admin_users`, `devnet_mints`,
`devnet_price_overrides`, `market_challenges`, `auto_fund_log`, `airdrop_claims`, `faucet_claims`,
`devnet_airdrop_claims`.

## Code changes that must land alongside the schema

- **H2/H3 write path** — `@percolatorct/shared` `insertTrade()` / `TradeRow` must accept + populate
  `asset_index` and `leg_index`; `TradeIndexer.ts` (the TODO at ~414) and `percolatorTxParser.ts` /
  `EventStreamService.ts` must pass a per-tx `leg_index`. Until then the columns exist but stay `0`.
- **H1 OI / active_positions** — decode real open interest. The on-chain `oi_eff_long_q/short_q` counter
  (`parseMarketGroupV17OI`) is **stale** (not decremented on close), so writing it reproduces the phantom-OI
  problem. Correct source = a `KIND_PORTFOLIO` scan (`parsePortfolioV17`) summing live position sizes →
  real `total_open_interest_usd` + `active_positions`; fall back to `0` (not the stale counter) when
  `getProgramAccounts` is unavailable.

## Validation

Apply to an ephemeral Postgres with `psql -v ON_ERROR_STOP=1 -f db/v17_baseline.sql` (Supabase provides the
`anon`/`authenticated`/`service_role` roles; create them first when testing locally). Then confirm:

```sql
SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='trades'::regclass;
-- expect uq_trades_sig_asset_leg UNIQUE (tx_signature, asset_index, leg_index) WHERE tx_signature IS NOT NULL
```
