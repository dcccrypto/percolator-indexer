-- NOT APPLIED: review and run by hand against a non-production database first.
-- fetchTraderPositionEvidence (src/db/traderNetPosition.ts) reads every fill of one
-- (trader, slab, asset, network), ordered by id, a page of 1000 at a time.
-- The existing indexes lead with (trader, created_at), so Postgres reads all of the
-- trader's rows across every market and filters them. This index matches the equality
-- filters and ends in id, so the lookup reads only that position's rows (the planner may
-- still sort the small result, or walk the index in id order for a large one).
--
-- CONCURRENTLY cannot run inside a transaction block; apply it as a single statement.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_position_lookup
  ON trades (trader, slab_address, asset_index, network, id);
