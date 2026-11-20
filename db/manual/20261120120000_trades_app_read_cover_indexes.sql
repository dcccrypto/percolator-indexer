-- ============================================================================================
-- NOT APPLIED: review and run by hand against a non-production database first, like
-- 20261115120000_trades_covering_read_indexes.sql. Nothing in the indexer's deploy applies this file,
-- and the indexer and app work without it (same results, slower reads).
-- Lives in db/manual/, not supabase/migrations/: CREATE INDEX CONCURRENTLY cannot run in the per-file
-- transaction the Supabase CLI uses. supabase/migrations/20261120120000_trades_app_read_cover_indexes.sql
-- is the transaction-safe stand-in (builds these indexes only on an empty trades table, else warns to
-- run this file).
--
-- Three covering indexes for the app's trade reads (percolator-launch app/lib/indexer-db.ts), which
-- today bitmap-scan or seq-scan trades. Measured on scratch Postgres 16 with 250k trades: see
-- bench/app-read-covers/README.md.
-- 1. idx_trades_net_created_cover: time-window reads over one network (queryLeaderboard 24h / 7d,
--    the 24h part of queryStatsAggregate). Non-partial on purpose: those reads filter
--    `is_liquidation IS NOT TRUE`, which cannot use the partial idx_trades_volume_24h
--    (WHERE is_liquidation = false).
-- 2. idx_trades_slab_net_created_cover: per-market chart reads (CANDLE_AGG_SQL,
--    queryTradesForCandles, queryLastTradePriceBefore).
-- 3. idx_trades_trader_net_created_cover: per-trader stats and history (queryTraderStatsAggregate,
--    queryTraderStatsRows).
-- No existing index is dropped here (several reads still pick idx_trades_network /
-- idx_trades_trader_created; dropping them needs its own re-measure).
--
-- Write cost (bench README, 250k trades, vs the 20261115 state): +64 MB of index (+256 B per trade);
-- per inserted trade +53 us backend CPU (+44%), +7 buffer touches, +533 B WAL in steady state (+61%) and
-- +2.8 KB right after a checkpoint (full-page images, +53%). Small next to the reads it saves at the current
-- ingest rate; re-measure before applying on a write-heavy, read-light database.
--
-- RUN WITH ON_ERROR_STOP, as a script in autocommit:
--   psql -v ON_ERROR_STOP=1 -f db/manual/20261120120000_trades_app_read_cover_indexes.sql
-- CONCURRENTLY cannot run inside a transaction block: do not use a transactional runner.
-- `IF NOT EXISTS` does not detect an INVALID leftover from a failed build: check pg_index.indisvalid,
-- DROP INDEX CONCURRENTLY the invalid one and rerun.
--
-- Rollback: DROP INDEX CONCURRENTLY IF EXISTS idx_trades_net_created_cover,
-- idx_trades_slab_net_created_cover, idx_trades_trader_net_created_cover (one statement each).
-- ============================================================================================
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_net_created_cover
  ON trades (network, created_at) INCLUDE (trader, slab_address, size, price, is_liquidation);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_slab_net_created_cover
  ON trades (network, slab_address, created_at DESC) INCLUDE (price, size, id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_trader_net_created_cover
  ON trades (trader, network, created_at DESC) INCLUDE (slab_address, side, size, price, fee);
