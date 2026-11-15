-- ============================================================================================
-- NOT APPLIED: review and run by hand against a non-production database first, like
-- 20261114130000_trades_position_lookup_index.sql. Nothing in the indexer's deploy applies this file,
-- and the indexer works without it (same results, slower reads).
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
--    It supersedes idx_trades_position_lookup (same leading columns), which can be dropped afterwards (manual step at the
--    end) so the table does not carry both.
--
-- Write cost: every trades insert now maintains these two indexes instead of idx_trades_position_lookup
-- alone (the volume index is partial and skips liquidation markers). Index-only scans need an up-to-date
-- visibility map: autovacuum handles it on the busy table; Heap Fetches in EXPLAIN shows if it lags.
--
-- CONCURRENTLY cannot run inside a transaction block: apply each statement by hand on its own, not
-- through a transactional runner. A failed build leaves an INVALID index: check pg_index.indisvalid and
-- DROP INDEX CONCURRENTLY <name> before retrying. Roll back with DROP INDEX CONCURRENTLY of the two new
-- names (and re-create idx_trades_position_lookup from 20261114130000 if it was dropped).
-- ============================================================================================
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_volume_24h
  ON trades (network, created_at) INCLUDE (slab_address, size)
  WHERE is_liquidation = false AND size IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_position_lookup_cover
  ON trades (trader, slab_address, asset_index, network, id)
  INCLUDE (side, size, is_liquidation, created_at, tx_signature);

-- SEPARATE, MANUAL STEP (deliberately commented out: `IF NOT EXISTS` above does NOT detect an INVALID leftover,
-- and a script run without ON_ERROR_STOP would carry on to the DROP). Only after this returns `t`:
--   SELECT indisvalid FROM pg_index WHERE indexrelid = 'idx_trades_position_lookup_cover'::regclass;
-- run, as its own statement:
--   DROP INDEX CONCURRENTLY IF EXISTS idx_trades_position_lookup;
