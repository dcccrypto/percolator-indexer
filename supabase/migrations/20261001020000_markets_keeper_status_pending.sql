-- keeper_status 'pending': discovered on chain, not yet registered.
--
-- The indexer auto-inserts a markets row for every slab discovery finds. Those rows
-- used to take the column default 'retired'. 'retired' is also the indexer's own
-- stop-ingesting lever (blocklist.ts setDbRetiredSlabs), so every brand-new market's
-- trades and events were dropped until the app's registration flipped the row to
-- 'active'. Real case: 9EPm8nB8… on 2026-10-01, inserted 01:21 and registered 01:55.
--
-- 'pending' keeps the 047 safety property: only 'active' enrolls a market for keeper
-- pricing (the oracle keeper's register-poll and price-ws filter keeper_status=eq.active),
-- and only the authenticated registration endpoint sets 'active'. The indexer now
-- writes 'pending' explicitly. The column DEFAULT stays 'retired' for any other writer.
ALTER TABLE markets DROP CONSTRAINT IF EXISTS markets_keeper_status_check;
ALTER TABLE markets ADD CONSTRAINT markets_keeper_status_check
  CHECK (keeper_status IN ('active', 'retired', 'pending'));

NOTIFY pgrst, 'reload schema';
