-- =============================================================================
-- markets.metadata_source — protect human-set metadata from the indexer.
--
-- Market identity (symbol, name, logo_url) has two possible origins:
--
--   'auto'   — the indexer resolved it from chain (DAS lookup on the collateral
--              mint, or on the base asset behind a market's DEX pool). Safe for
--              the indexer to refresh as better data becomes available.
--
--   'manual' — a human set it via PATCH /api/markets/[slab]. The indexer must
--              NEVER overwrite these rows: they exist precisely because on-chain
--              resolution was wrong or impossible.
--
-- Why this is needed: v17 admin-oracle markets carry no on-chain pointer to a
-- base asset (no indexFeedId, no dexPool, no mainnet CA). The indexer cannot
-- name them, so it writes a neutral placeholder and a human corrects it. Without
-- this column the next registration/refresh pass would clobber that correction.
-- =============================================================================

ALTER TABLE markets
  ADD COLUMN IF NOT EXISTS metadata_source TEXT NOT NULL DEFAULT 'auto'
    CHECK (metadata_source IN ('auto', 'manual'));

COMMENT ON COLUMN markets.metadata_source IS
  'Origin of symbol/name/logo_url. ''manual'' rows are human-authored and must never be overwritten by the indexer.';

-- Drives the indexer's metadata refresh pass, which looks for auto rows still
-- missing a logo. Partial so it stays small as markets get resolved.
CREATE INDEX IF NOT EXISTS idx_markets_metadata_refresh
  ON markets (updated_at)
  WHERE metadata_source = 'auto' AND logo_url IS NULL;
