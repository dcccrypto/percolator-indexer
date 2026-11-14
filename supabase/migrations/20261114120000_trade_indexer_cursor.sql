-- ============================================================================================
-- MUST BE APPLIED BY HAND, like supabase/migrations/20261015120000_lp_vault_sync_cursor.sql.
-- Nothing in the indexer's deploy applies this file. If the table is absent the indexer does NOT
-- fail: src/db/tradeCursor.ts logs once at error level and the trade poller keeps its
-- in-memory cursor (the behaviour before this file existed).
--
-- Where the trade poller has got to, per market. `lastSignature` in TradeIndexerPolling was a
-- process-local Map, so every restart re-paged and re-fetched (getParsedTransactions) the
-- newest window of every market, and re-attempted every write (deduped by the DB, but paid for).
--
-- `signature` is the newest signature of a window the poller fully processed. It is written only
-- after the window's batches all committed; a failed or held window leaves the row untouched.
-- NOTE: a restart no longer re-scans the newest window, so to re-index a market after a parser fix (or to retry
-- skipped/null transactions) delete its row here: DELETE FROM trade_indexer_cursor WHERE slab_address = '<slab>';
-- Access: service_role only, like every other indexer-owned table here.
-- ============================================================================================
CREATE TABLE IF NOT EXISTS trade_indexer_cursor (
  network      text        NOT NULL,
  slab_address text        NOT NULL,
  signature    text        NOT NULL,
  updated_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (network, slab_address)
);

ALTER TABLE trade_indexer_cursor ENABLE ROW LEVEL SECURITY;  -- no policies: anon/authenticated get nothing
REVOKE ALL ON TABLE trade_indexer_cursor FROM anon, authenticated;
