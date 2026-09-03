-- =============================================================================
-- GH#195 part 2: close the NULLS DISTINCT dedup gap on `trades`, and de-duplicate
-- the rows the two dedup bugs already wrote.
--
-- THE GAP
--
-- `uq_trades_sig_asset_leg` is NULLS DISTINCT (the Postgres default), so any row
-- with `tx_signature = NULL` never conflicts with anything and can be inserted
-- unboundedly. The 20260726181000 migration made that explicit and accepted it on
-- the grounds that "real fills always carry a signature" — which is true of the
-- webhook path, because it validates signatures and rejects empty or malformed
-- ones. It is NOT a property of the table, and it is not enforced anywhere. Any
-- other writer — a backfill script, EventStreamService — can persist a NULL and
-- the dedup key silently stops applying to that row.
--
-- WHY `NULLS NOT DISTINCT` RATHER THAN `NOT NULL`
--
-- `tx_signature` is deliberately nullable: the baseline notes that
-- `is_liquidation` markers carry no amounts, and marker rows are not guaranteed to
-- come from a signed user tx. A blanket NOT NULL would reject legitimate marker
-- rows and turn a data-integrity fix into an ingestion outage.
--
-- A partial `NOT NULL` (non-marker rows only) would work but needs a CHECK
-- constraint plus a second index, and it still leaves marker rows undeduplicated.
-- `NULLS NOT DISTINCT` treats two NULL signatures as EQUAL for uniqueness, so
-- every row is covered by one key with no new constraint surface. Requires
-- Postgres 15+, which Supabase has been on since 2023.
--
-- The index stays non-partial, so it remains a valid `ON CONFLICT` target — the
-- whole point of the 20260726181000 migration, which this must not undo.
-- =============================================================================

-- ── 1. De-duplicate BEFORE tightening the index, or the CREATE fails ─────────
--
-- Ordering matters: a unique index cannot be built over existing duplicates. This
-- is also the "history becomes correct" half — inflated volume_24h_by_slab, trade
-- counts and candle volume all read from these rows, so leaving them would keep
-- the reported numbers wrong even after ingestion is fixed.
--
-- Keeps the LOWEST id per key, which is the first-written row: it is the one whose
-- side/size/price were recorded by the path that saw the fill first, and any
-- downstream row referencing a trade id will reference that one.
WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY tx_signature, asset_index, leg_index
      ORDER BY id
    ) AS rn
  FROM trades
)
DELETE FROM trades
WHERE id IN (SELECT id FROM ranked WHERE rn > 1);

-- ── 2. Rebuild the index with NULLS NOT DISTINCT ─────────────────────────────
DROP INDEX IF EXISTS uq_trades_sig_asset_leg;

CREATE UNIQUE INDEX uq_trades_sig_asset_leg
  ON trades (tx_signature, asset_index, leg_index)
  NULLS NOT DISTINCT;

COMMENT ON INDEX uq_trades_sig_asset_leg IS
  'H2/H3 dedupe key for multi-fill batch legs sharing a tx_signature. Non-partial '
  'so it can serve as an ON CONFLICT target (20260726181000). NULLS NOT DISTINCT '
  'since GH#195: NULL-signature rows are deduplicated too, instead of being '
  'exempt from the key entirely.';

-- ── 3. No rollup refresh is needed, and that is worth stating ───────────────
--
-- `volume_24h_by_slab` is a STABLE SQL FUNCTION that aggregates `public.trades`
-- live on every call — not a materialized view. Deleting the duplicate rows above
-- therefore corrects `volume_24h`, `trade_count_24h` and candle volume
-- immediately, with nothing to recompute.
--
-- An earlier draft of this migration called a `refresh_volume_24h_by_slab()`
-- inside an existence guard. No such function exists, so the guard would simply
-- never fire — a step that looks like it maintains the rollup while doing nothing,
-- which is worse than the honest absence of one.
