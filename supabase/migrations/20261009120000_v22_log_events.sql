-- NOT APPLIED. v2.2 wrapper LOG events (docs/v22-fill-events.md): executed fills, reductions and value moves decoded from the
-- transaction logs, plus an `events_unknown` marker for a transaction whose events could not be established (truncated / null
-- logs, inconsistent frames) so its positions can be reconciled from account state.
-- Apply by hand AFTER 20261007120000_v22_events.sql and BEFORE the v2.2 programs go live. The table, its unique key and its
-- grants are unchanged: log events take inner_index = 1,000,000 + their position among the transaction's log-event rows, so they
-- never collide with the instruction events (whose inner_index is -1 or a small index). Until this is applied the indexer drops
-- the log events with one loud error per 10 minutes (23514 check violation) and carries on; the instruction events are written
-- in a separate call and are unaffected.
ALTER TABLE v22_events DROP CONSTRAINT IF EXISTS v22_events_kind_check;
ALTER TABLE v22_events ADD CONSTRAINT v22_events_kind_check CHECK (kind IN (
  'bond_deposit', 'bond_withdraw_request', 'bond_withdraw_execute',
  'rescue_deposit', 'insurance_units_init',
  'backstop_propose', 'backstop_draw', 'backstop_restore',
  'holding_rent_settled', 'band_dust_swept', 'eviction',
  'fill_event', 'reduce_event', 'move_event', 'events_unknown'));
