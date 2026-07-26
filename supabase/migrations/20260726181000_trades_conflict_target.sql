-- =============================================================================
-- trades: make the (tx_signature, asset_index, leg_index) unique index usable as
-- an ON CONFLICT target.
--
-- The baseline created this as a PARTIAL index (WHERE tx_signature IS NOT NULL).
-- Postgres cannot infer a partial unique index from `ON CONFLICT (cols)` — it
-- requires the predicate be restated, which PostgREST's `on_conflict` parameter
-- cannot express (it takes column names only). Verified against this database:
--   ERROR: there is no unique or exclusion constraint matching the ON CONFLICT
--          specification
-- That blocks batching webhook trade inserts into a single upsert.
--
-- Dropping the predicate does NOT weaken or widen the constraint:
--   * asset_index and leg_index are NOT NULL, so tx_signature is the only column
--     that can be NULL.
--   * The index is NULLS DISTINCT (the default), so a row with NULL tx_signature
--     never conflicts with anything — exactly what the WHERE clause achieved by
--     excluding those rows outright.
-- Enforcement is therefore identical for every row; only NULL-signature rows are
-- now physically present in the index (they are rare — real fills always carry a
-- signature), and in exchange the index becomes a valid conflict target.
-- =============================================================================

DROP INDEX IF EXISTS uq_trades_sig_asset_leg;

CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_sig_asset_leg
  ON trades (tx_signature, asset_index, leg_index);

COMMENT ON INDEX uq_trades_sig_asset_leg IS
  'H2/H3 dedupe key for multi-fill batch legs sharing a tx_signature. Non-partial so it can serve as an ON CONFLICT target; NULLS DISTINCT makes NULL-signature rows non-conflicting.';
