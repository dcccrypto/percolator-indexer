-- Transaction-safe stand-in for db/manual/20261114130000_trades_position_lookup_index.sql.
--
-- That file builds idx_trades_position_lookup with CREATE INDEX CONCURRENTLY, which fails inside a
-- transaction block (25001). The Supabase CLI (db push / db reset) and psql --single-transaction run
-- each migration file in one transaction, so the CONCURRENTLY build cannot live in this directory.
-- This file keeps the version in the migration history and does only what is safe in a transaction:
--   * the index, or the cover index from 20261115120000 that supersedes it, already exists: nothing
--     (a re-apply does not rebuild an index the next file drops);
--   * trades is empty (a fresh or reset database): build it plainly; there are no rows to lock out;
--   * trades has rows (a hosted database): do NOT build it here, since a non-concurrent build blocks
--     trades writes for the whole build. Raise a WARNING instead; build it by hand with the db/manual
--     file (psql autocommit, CONCURRENTLY). The indexer works without it (same results, slower reads).
-- Key: see the db/manual file (fetchTraderPositionEvidence in src/db/traderNetPosition.ts).
DO $$
BEGIN
  IF to_regclass('public.idx_trades_position_lookup') IS NOT NULL
     OR to_regclass('public.idx_trades_position_lookup_cover') IS NOT NULL THEN
    RETURN;
  ELSIF EXISTS (SELECT 1 FROM public.trades LIMIT 1) THEN
    RAISE WARNING 'idx_trades_position_lookup not built: trades has rows. Build it by hand with db/manual/20261114130000_trades_position_lookup_index.sql (CONCURRENTLY, outside a transaction).';
  ELSE
    CREATE INDEX IF NOT EXISTS idx_trades_position_lookup
      ON public.trades (trader, slab_address, asset_index, network, id);
  END IF;
END $$;
