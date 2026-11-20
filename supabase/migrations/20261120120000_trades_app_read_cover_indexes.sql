-- Transaction-safe stand-in for db/manual/20261120120000_trades_app_read_cover_indexes.sql.
--
-- That file builds three covering indexes for the app's trade reads with CREATE INDEX CONCURRENTLY,
-- which cannot run inside a transaction block (25001); the Supabase CLI (db push / db reset) and
-- psql --single-transaction run each migration file in one transaction. So here, like
-- 20261115120000_trades_covering_read_indexes.sql:
--   * trades is empty (a fresh or reset database): build the three indexes plainly; there are no rows
--     to lock out, and a re-apply is a no-op (IF NOT EXISTS);
--   * trades has rows (a hosted database): build NOTHING (a non-concurrent build blocks trades writes
--     for the whole build). If any of the three is missing, raise a WARNING: build them by hand with
--     the db/manual file (psql autocommit, ON_ERROR_STOP=1). Reads work without them, only slower.
-- Index definitions must match the db/manual file (tests/db/appReadCoverIndexes.test.ts pins both).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.trades LIMIT 1) THEN
    IF to_regclass('public.idx_trades_net_created_cover') IS NULL
       OR to_regclass('public.idx_trades_slab_net_created_cover') IS NULL
       OR to_regclass('public.idx_trades_trader_net_created_cover') IS NULL THEN
      RAISE WARNING 'idx_trades_net_created_cover / idx_trades_slab_net_created_cover / idx_trades_trader_net_created_cover not built: trades has rows. Build them by hand with db/manual/20261120120000_trades_app_read_cover_indexes.sql (CONCURRENTLY, outside a transaction).';
    END IF;
  ELSE
    CREATE INDEX IF NOT EXISTS idx_trades_net_created_cover
      ON public.trades (network, created_at) INCLUDE (trader, slab_address, size, price, is_liquidation);

    CREATE INDEX IF NOT EXISTS idx_trades_slab_net_created_cover
      ON public.trades (network, slab_address, created_at DESC) INCLUDE (price, size, id);

    CREATE INDEX IF NOT EXISTS idx_trades_trader_net_created_cover
      ON public.trades (trader, network, created_at DESC) INCLUDE (slab_address, side, size, price, fee);
  END IF;
END $$;
