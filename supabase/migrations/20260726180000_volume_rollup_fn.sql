-- =============================================================================
-- volume_24h_by_slab — server-side 24h volume rollup
--
-- Replaces the client-side rollup in StatsCollector.syncVolumeForAllDBMarkets(),
-- which paginated up to 100k trade rows (20 HTTP round-trips x 5k rows) out of
-- PostgREST every 10 minutes and summed them in JS. That transferred the entire
-- 24h trade tape over the wire just to produce one number per market.
--
-- This does the same aggregation in Postgres and returns one row per market.
--
-- Correctness (see db/v17_baseline.sql, trades): is_liquidation rows are MARKERS.
-- v17 liquidations run through PermissionlessCrank (tag 5 / action 1), which
-- carries no size/price/side, so those rows have NULL size. The old JS path did
-- not filter them: BigInt(null) threw, the fallback Number(null) === 0 passed
-- the isFinite check, and each marker silently incremented trade_count_24h by 1
-- with 0 volume — inflating trade counts. Both filters are explicit here.
-- =============================================================================

CREATE OR REPLACE FUNCTION public.volume_24h_by_slab(p_network text)
RETURNS TABLE (
  slab_address    text,
  volume_24h      numeric,
  trade_count_24h bigint
)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = public
AS $$
  SELECT
    t.slab_address,
    COALESCE(SUM(ABS(t.size)), 0)::numeric AS volume_24h,
    COUNT(*)::bigint                       AS trade_count_24h
  FROM public.trades t
  WHERE t.network        = p_network
    AND t.is_liquidation = false      -- markers carry no amounts
    AND t.size          IS NOT NULL
    AND t.created_at    >= NOW() - INTERVAL '24 hours'
  GROUP BY t.slab_address
$$;

COMMENT ON FUNCTION public.volume_24h_by_slab(text) IS
  'Per-slab 24h volume + fill count for the given network. Excludes is_liquidation markers (NULL size). Backs market_stats.volume_24h / trade_count_24h.';

-- The indexer connects as service_role; the anon/authenticated roles have no
-- reason to run this (the frontend reads market_stats / markets_with_stats).
REVOKE ALL ON FUNCTION public.volume_24h_by_slab(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.volume_24h_by_slab(text) TO service_role;
