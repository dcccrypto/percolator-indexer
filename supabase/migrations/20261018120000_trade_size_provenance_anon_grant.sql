-- ============================================================================================
-- MUST BE APPLIED BY HAND, AFTER 20261012090000_trade_size_provenance.sql (and its validate
-- step). Nothing in the indexer's deploy applies this file. NOT applied to any Supabase project
-- at the time it was written (2026-10-18).
--
-- PR #228 (Bayyan16) added public.trades.size_provenance. anon is column-restricted on trades
-- (20260726120000_v17_baseline.sql:207, db/v17_baseline.sql: GRANT SELECT (trader, size,
-- created_at) ON trades TO anon), so a new column is invisible to anon until it is granted by
-- name. `authenticated` and `service_role` hold table-level SELECT, which already covers it.
--
-- size_provenance qualifies `size`, which anon can already read: a public consumer that sees a
-- size but not whether it is exact or an upper bound would treat every size as exact, which is
-- the mistake the column exists to prevent. It carries no PII (one of three fixed labels or NULL).
-- ============================================================================================
BEGIN;

GRANT SELECT (size_provenance) ON public.trades TO anon;

COMMIT;
