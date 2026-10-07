-- ============================================================================================
-- MUST BE APPLIED BY HAND on the MAINNET database (and on any other environment lacking it).
-- Nothing in the indexer's deploy applies this file. If the table is absent the indexer does NOT
-- fail: it logs at error level and reports to Sentry instead (and appends to the JSONL file when
-- SKIPPED_SIGNATURES_FILE is set), so skips stay visible but are not durable in the database.
--
-- Mirrors the table as it exists on devnet (columns read from the live REST schema 2026-10-07):
-- id bigserial PK, signature text NOT NULL, source text NOT NULL, slab text, network text,
-- error text, skipped_at timestamptz NOT NULL DEFAULT now(), reindexed_at timestamptz,
-- UNIQUE (signature, source). RLS on, no policies, no anon/authenticated grants.
-- Lives here, with the repo's other applied migrations (supabase/migrations/); the earlier copy under
-- migrations/ (added by #212) was removed so there is exactly one.
-- ============================================================================================
-- NOT APPLIED by code. Durable record of transaction signatures the indexer skipped because the RPC client could not
-- return them (X-1). Lets an operator re-index them once the reader is fixed, and alert on a non-zero count.
-- Apply by hand against the indexer database; the indexer writes here best-effort and falls back to a JSONL file.
CREATE TABLE IF NOT EXISTS skipped_signatures (
  id          bigserial PRIMARY KEY,
  signature   text        NOT NULL,
  source      text        NOT NULL,          -- 'trade-indexer' | 'lp-vault' | 'creator-lookup'
  slab        text,                          -- slab (or registry) whose window contained it
  network     text,
  error       text,
  skipped_at  timestamptz NOT NULL DEFAULT now(),
  reindexed_at timestamptz,
  UNIQUE (signature, source)
);
CREATE INDEX IF NOT EXISTS skipped_signatures_pending ON skipped_signatures (skipped_at) WHERE reindexed_at IS NULL;

-- Server-side only: the indexer writes with the service role. No client role may read or write this table.
ALTER TABLE skipped_signatures ENABLE ROW LEVEL SECURITY;  -- no policies: anon/authenticated get nothing
REVOKE ALL ON TABLE skipped_signatures FROM anon, authenticated;
REVOKE ALL ON SEQUENCE skipped_signatures_id_seq FROM anon, authenticated;
