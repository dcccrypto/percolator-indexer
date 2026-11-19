-- ============================================================================================
-- NOT APPLIED: review and run by hand against a non-production database first, like
-- 20261114130000_trades_position_lookup_index.sql. Nothing in the indexer's deploy applies this file,
-- and the indexer works without it (same results, slower reads).
-- Lives in db/manual/, not supabase/migrations/: CREATE INDEX CONCURRENTLY cannot run in the per-file
-- transaction the Supabase CLI uses. supabase/migrations/20261115120000_trades_covering_read_indexes.sql is the
-- transaction-safe stand-in (builds these indexes only on an empty trades table, else warns to run this file).
--
-- Two covering indexes so the indexer's recurring trade reads become index-only scans (no heap
-- visits). Measured on a local throwaway Postgres 18 with 500k trades: see bench/slow-queries/README.md.
--
-- 1. volume_24h_by_slab (StatsCollector.syncVolumeForAllDBMarkets, every stats cycle). It scans the last
--    24h of one network and reads slab_address + size from the heap. The partial predicate is the
--    function's own filter (liquidation markers and NULL sizes are excluded), so it must stay in
--    sync with 20260726180000_volume_rollup_fn.sql.
-- 2. fetchTraderPositionEvidence (src/db/traderNetPosition.ts, one paged read per close). Same key as
--    idx_trades_position_lookup plus the five columns the read returns, so it never touches the heap.
--    It supersedes idx_trades_position_lookup (identical key columns in the same order; no SQL, function or
--    route names the old index), which this file drops at the end so the table does not carry both.
--
-- Write cost: the cover index REPLACES idx_trades_position_lookup (dropped below), so every trades insert
-- maintains idx_trades_position_lookup_cover plus the partial volume index (which skips liquidation markers).
-- Keeping the old index too measured +69% (+77 MB) on the bench; with the drop it is ~+26% (+35 MB): see
-- bench/slow-queries/README.md. Index-only scans need an up-to-date visibility map: autovacuum handles it on
-- the busy table; Heap Fetches in EXPLAIN shows if it lags.
--
-- RUN WITH ON_ERROR_STOP, one statement at a time or as a script:
--   psql -v ON_ERROR_STOP=1 -f db/manual/20261115120000_trades_covering_read_indexes.sql
-- so a failed or INVALID cover build stops the script BEFORE the DROP (`IF NOT EXISTS` does not detect an
-- INVALID leftover). If the cover build fails: check pg_index.indisvalid, DROP INDEX CONCURRENTLY the invalid
-- one and retry; the old index is still in place until the DROP runs.
-- CONCURRENTLY cannot run inside a transaction block: do not use a transactional runner.
--
-- Rollback: DROP INDEX CONCURRENTLY IF EXISTS the two new names, and re-create idx_trades_position_lookup from db/manual/20261114130000.
-- ============================================================================================
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_volume_24h
  ON trades (network, created_at) INCLUDE (slab_address, size)
  WHERE is_liquidation = false AND size IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_position_lookup_cover
  ON trades (trader, slab_address, asset_index, network, id)
  INCLUDE (side, size, is_liquidation, created_at, tx_signature);

-- Only reached if both builds above succeeded (ON_ERROR_STOP). Optional sanity check before this runs:
--   SELECT indisvalid FROM pg_index WHERE indexrelid = 'idx_trades_position_lookup_cover'::regclass;  -- t
DROP INDEX CONCURRENTLY IF EXISTS idx_trades_position_lookup;
