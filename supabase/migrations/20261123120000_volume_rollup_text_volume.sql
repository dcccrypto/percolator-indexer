-- =============================================================================
-- volume_24h_by_slab: return volume_24h as TEXT, so a 24h volume above 2^53 reaches
-- market_stats.volume_24h exactly.
--
-- NOT applied to any Supabase project when written (2026-11-23). Apply by hand, as the owner
-- of the function, after 20261119120000_rls_grants_hardening.sql.
--
-- Bug: PostgREST sends a NUMERIC function column as a bare JSON number. supabase-js
-- JSON.parses it into a JS double, so StatsCollector.syncVolumeForAllDBMarkets gets a value
-- that is already rounded above 2^53. Its "keep the string above MAX_SAFE_INTEGER" branch
-- then keeps the string of the rounded double. Real PostgREST 16.6 + PG16, two trades of size
-- 9007199254740000 and 993: the RPC computes 9007199254740993, market_stats stores
-- 9007199254740992 (and u64::MAX comes back as 18446744073709552000).
--
-- As TEXT, PostgREST sends a JSON string; StatsCollector already takes string | number
-- (String(raw), then Number() only when the value is within MAX_SAFE_INTEGER), so no code change
-- is needed and the old indexer keeps working against the new function. NaN / Infinity still
-- reach the existing skip ('NaN' -> Number NaN; 'Infinity' contains "inf").
--
-- The return type changes, so CREATE OR REPLACE is not allowed: DROP and CREATE in one
-- transaction, then put back the grants of 20261119120000 (a re-created function starts with
-- the default EXECUTE for PUBLIC and Supabase's default privileges). Only the indexer calls it
-- (service_role); nothing depends on it.
-- =============================================================================
BEGIN;

DROP FUNCTION IF EXISTS public.volume_24h_by_slab(text);

CREATE FUNCTION public.volume_24h_by_slab(p_network text)
RETURNS TABLE (
  slab_address    text,
  volume_24h      text,
  trade_count_24h bigint
)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = public
AS $$
  SELECT
    t.slab_address,
    COALESCE(SUM(ABS(t.size)), 0)::text AS volume_24h,
    COUNT(*)::bigint                    AS trade_count_24h
  FROM public.trades t
  WHERE t.network        = p_network
    AND t.is_liquidation = false      -- markers carry no amounts
    AND t.size          IS NOT NULL
    AND t.created_at    >= NOW() - INTERVAL '24 hours'
  GROUP BY t.slab_address
$$;

COMMENT ON FUNCTION public.volume_24h_by_slab(text) IS
  'Per-slab 24h volume (exact NUMERIC as text) + fill count for the given network. Excludes is_liquidation markers (NULL size). Backs market_stats.volume_24h / trade_count_24h.';

REVOKE ALL ON FUNCTION public.volume_24h_by_slab(text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.volume_24h_by_slab(text) TO service_role;

-- PostgREST must see the new return type (as 20261001020000_markets_keeper_status_pending.sql does).
NOTIFY pgrst, 'reload schema';

COMMIT;
