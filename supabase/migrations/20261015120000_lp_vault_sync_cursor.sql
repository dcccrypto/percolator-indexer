-- ============================================================================================
-- MUST BE APPLIED BY HAND, like supabase/migrations/20261005120000_skipped_signatures.sql.
-- Nothing in the indexer's deploy applies this file. If the table is absent the indexer does NOT
-- fail: SupabaseLpVaultStore.loadCursor/saveCursor log once per vault at error level and the
-- indexer falls back to its in-memory cursor (i.e. to the pre-#231 behaviour, minus the wedge,
-- because the sync now drains a backlog in chunks instead of refusing it).
--
-- GH#231: where one LP vault's sync has got to.
--
-- `cursors` in LpVaultIndexer was a process-local Map, so every restart or deploy re-paged the
-- vault's entire signature history. Any vault past the per-sync signature cap then refused to
-- sync at all, permanently, and the backfill script -- which constructs the same indexer with the
-- same default cap -- refused the same vaults it exists to repair.
--
-- The fold itself needs no checkpoint: lp_vault_positions is always RECOMPUTED from
-- lp_vault_events, which is keyed (signature, ix_index) and idempotent. The cursor is the only
-- thing a restart loses, so it is the only thing this table stores.
--
-- `signature` is the newest transaction PROCESSED, which is deliberately not the same as the
-- newest row in lp_vault_events: a transaction that decodes to no vault events writes no event
-- row, so an events-derived watermark would never advance past unrelated traffic that merely
-- names the registry -- which is exactly the traffic that used to wedge the sync.
--
-- Access: service_role only, like every other indexer-owned table here.
-- ============================================================================================
CREATE TABLE IF NOT EXISTS lp_vault_sync_cursor (
  network    text        NOT NULL,
  registry   text        NOT NULL,
  -- Newest signature whose transaction has been folded into lp_vault_events for this vault.
  signature  text        NOT NULL,
  -- That transaction's slot. Not read by the indexer; it makes "how far behind is this vault"
  -- answerable from the table alone.
  slot       bigint      NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (network, registry)
);

-- Server-side only: the indexer writes with the service role. No client role may read or write.
ALTER TABLE lp_vault_sync_cursor ENABLE ROW LEVEL SECURITY;  -- no policies: anon/authenticated get nothing
REVOKE ALL ON TABLE lp_vault_sync_cursor FROM anon, authenticated;
