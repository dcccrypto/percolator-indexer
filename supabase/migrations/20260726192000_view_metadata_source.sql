-- =============================================================================
-- Rebuild markets_with_stats so it exposes markets.metadata_source.
--
-- The view selects `m.*`, but `*` is expanded to a fixed column list when the
-- view is CREATEd — it does not track later ALTER TABLE ADD COLUMN. The view was
-- created before metadata_source existed, so it still projects the old list and
-- the frontend cannot see the column.
--
-- CREATE OR REPLACE VIEW cannot fix this: it may only APPEND columns, and
-- re-expanding `m.*` inserts metadata_source in the middle (before the joined
-- stats columns), which fails with "cannot change name of view column". So the
-- view is dropped and recreated, inside a transaction so readers never observe
-- a missing view.
-- =============================================================================

BEGIN;

DROP VIEW IF EXISTS markets_with_stats;

CREATE VIEW markets_with_stats AS
SELECT
  m.*,
  s.volume_24h, s.volume_24h_usd, s.trade_count_24h, s.last_price,
  s.updated_at AS stats_updated_at
FROM markets m
LEFT JOIN market_stats s ON m.slab_address = s.slab_address
WHERE COALESCE(m.indexer_excluded, false) = false
  AND m.status <> 'closed';

COMMENT ON VIEW markets_with_stats IS
  'Market list source: registry joined to the 24h volume cache. Excludes indexer_excluded and closed markets.';

COMMIT;

-- DROP VIEW discards the view''s grants, so restore them.
GRANT SELECT ON markets_with_stats TO anon, authenticated, service_role;
