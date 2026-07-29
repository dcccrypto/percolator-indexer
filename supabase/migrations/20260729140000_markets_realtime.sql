-- =============================================================================
-- Publish `markets` to Supabase Realtime.
--
-- The oracle keeper is NAT'd and outbound-only, so the serverless frontend can
-- never reach it. Registration was therefore inverted into an outbound poll of
-- /api/playground/registered-markets. Polling is a latency floor: a market the
-- user just created is not priced until the next tick.
--
-- Supabase Realtime is the always-on push channel that removes it. The keeper
-- opens ONE outbound WebSocket and is told the instant a market row changes, so
-- there is no new relay service to run — the database is already the authority
-- for which markets exist (see the retirement filter on the registrations feed).
--
-- The keeper uses these events as a WAKE-UP, not as a data source: on any change
-- it immediately runs its normal registration poll. That keeps one code path for
-- actually admitting a market (on-chain owner filter, dexType, addMarket) and
-- leaves the periodic poll as the safety net if the socket drops.
--
-- SECURITY: markets already has RLS with a public_read SELECT policy and an anon
-- SELECT grant, so the keeper subscribes with the ANON key. Realtime enforces
-- RLS per subscriber, so publishing this table exposes nothing that GET
-- /api/markets does not already serve publicly. No service-role key is needed
-- off-box.
-- =============================================================================

-- REPLICA IDENTITY FULL so DELETE events carry the old row (slab_address in
-- particular). Without it Postgres emits only the primary key, and a retirement
-- would arrive as an unidentifiable delete. `markets` is small and low-write —
-- the extra WAL volume is immaterial here.
ALTER TABLE markets REPLICA IDENTITY FULL;

-- Idempotent: adding a table already in the publication is an error, so check.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_publication_tables
    WHERE pubname = 'supabase_realtime'
      AND schemaname = 'public'
      AND tablename = 'markets'
  ) THEN
    ALTER PUBLICATION supabase_realtime ADD TABLE public.markets;
  END IF;
END
$$;
