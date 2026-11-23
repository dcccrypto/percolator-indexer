-- Transaction-safe stand-in for db/manual/20261115120000_trades_covering_read_indexes.sql.
--
-- That file builds idx_trades_volume_24h and idx_trades_position_lookup_cover with CREATE INDEX
-- CONCURRENTLY and then drops the superseded idx_trades_position_lookup with DROP INDEX CONCURRENTLY.
-- Neither can run inside a transaction block (25001), and the Supabase CLI (db push / db reset) and
-- psql --single-transaction run each migration file in one transaction. So here:
--   * trades is empty (a fresh or reset database): build both indexes plainly and drop the superseded
--     one; there are no rows to lock out, and a re-apply is a no-op (IF NOT EXISTS / IF EXISTS);
--   * trades has rows (a hosted database): build and drop NOTHING (a non-concurrent build blocks trades
--     writes for the whole build, and the drop must wait until the cover index is valid). If either
--     new index is missing, raise a WARNING: build them by hand with the db/manual file (psql
--     autocommit, ON_ERROR_STOP=1). The indexer works without them (same results, slower reads).
-- Index definitions must match the db/manual file (tests/db/coveringReadIndexes.test.ts pins both); the
-- volume index's partial predicate must stay in sync with 20260726180000_volume_rollup_fn.sql.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.trades LIMIT 1) THEN
    IF to_regclass('public.idx_trades_volume_24h') IS NULL
       OR to_regclass('public.idx_trades_position_lookup_cover') IS NULL THEN
      RAISE WARNING 'idx_trades_volume_24h / idx_trades_position_lookup_cover not built: trades has rows. Build them by hand with db/manual/20261115120000_trades_covering_read_indexes.sql (CONCURRENTLY, outside a transaction).';
    END IF;
  ELSE
    CREATE INDEX IF NOT EXISTS idx_trades_volume_24h
      ON public.trades (network, created_at) INCLUDE (slab_address, size)
      WHERE is_liquidation = false AND size IS NOT NULL;

    CREATE INDEX IF NOT EXISTS idx_trades_position_lookup_cover
      ON public.trades (trader, slab_address, asset_index, network, id)
      INCLUDE (side, size, is_liquidation, created_at, tx_signature);

    DROP INDEX IF EXISTS public.idx_trades_position_lookup;
  END IF;
END $$;
