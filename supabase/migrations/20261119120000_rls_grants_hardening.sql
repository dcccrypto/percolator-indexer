-- ============================================================================================
-- RLS / grants hardening for schema public (audit J-20261119-IDX-REPORT-RLSGRANTSAUDIT, F1-F6).
--
-- NOT applied to any Supabase project when written (2026-11-19). Before applying by hand, check
-- the hosted default ACL (read only):
--   SELECT pg_get_userbyid(defaclrole), defaclobjtype, defaclacl
--   FROM pg_default_acl WHERE defaclnamespace = 'public'::regnamespace;
-- Every statement is safe whether or not those default privileges exist.
--
-- What the launch app reads with the anon key is unchanged:
--   * markets_with_stats (symbol, name, ...): anon keeps SELECT on the view and on both base
--     tables, so it still works with security_invoker on.
--   * trades: anon keeps exactly trader, size, created_at, size_provenance. (The app's leaderboard
--     fallback also reads price/network/slab_address with the anon key; that already fails today
--     and is fixed in the app, not here.)
-- The indexer uses the service role, which gets explicit grants below.
-- ============================================================================================
BEGIN;

-- F1. 20260729150000_restore_app_tables.sql enabled RLS but never revoked the default grants, so
-- on Supabase anon/authenticated kept ALL, including TRUNCATE, which RLS does not stop. The app
-- touches these tables only with the service-role key.
REVOKE ALL ON TABLE faucet_claims, devnet_mints, devnet_airdrop_claims, auto_fund_log, market_challenges
  FROM PUBLIC, anon, authenticated;

-- F2. REVOKE ... FROM PUBLIC (20260726180000_volume_rollup_fn.sql) does not remove the EXECUTE that
-- Supabase's default privileges grant anon/authenticated directly. Only the indexer calls it.
REVOKE ALL ON FUNCTION public.volume_24h_by_slab(text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.volume_24h_by_slab(text) TO service_role;

-- F3. Run the view with the caller's rights, so it can never read past the RLS and grants on
-- markets / market_stats. Any later CREATE VIEW of markets_with_stats must set this again.
ALTER VIEW markets_with_stats SET (security_invoker = on);

-- F4. cleanup_old_trades kept the default EXECUTE for PUBLIC, so anon could reach it via /rpc.
REVOKE ALL ON FUNCTION public.cleanup_old_trades(integer) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.cleanup_old_trades(integer) TO service_role;

-- F5. authenticated had full-row SELECT on trades; give it the same columns as anon. The table-level
-- REVOKE also drops any column-level grants, so re-grant the columns after it.
REVOKE ALL ON TABLE trades FROM authenticated;
GRANT SELECT (trader, size, created_at, size_provenance) ON trades TO authenticated;

-- F6. Indexer-only tables that worked only through Supabase's default privileges. Explicit grants,
-- as 20261007120000_v22_events.sql does, so a database without those defaults works too.
GRANT ALL ON TABLE lp_vault_sync_cursor, skipped_signatures, trade_indexer_cursor TO service_role;
GRANT ALL ON SEQUENCE skipped_signatures_id_seq TO service_role;

COMMIT;
