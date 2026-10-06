-- NOT APPLIED. v2.2 activity events: bond deposits / withdrawals, rescue, insurance units, G9 backstop
-- (propose / draw / restore), holding-rent settlements, evictions and band dust sweeps.
-- Apply by hand against the indexer database BEFORE the v2.2 programs go live. Until it is applied the indexer logs
-- one loud error per 10 minutes and carries on (trades, stats and every other path are unaffected).
CREATE TABLE IF NOT EXISTS v22_events (
  id           bigserial PRIMARY KEY,
  signature    text        NOT NULL,
  ix_index     integer     NOT NULL,                 -- top-level instruction index
  inner_index  integer     NOT NULL DEFAULT -1,      -- -1 = top-level, else index inside the inner-instruction group
  kind         text        NOT NULL CHECK (kind IN (
                 'bond_deposit', 'bond_withdraw_request', 'bond_withdraw_execute',
                 'rescue_deposit', 'insurance_units_init',
                 'backstop_propose', 'backstop_draw', 'backstop_restore',
                 'holding_rent_settled', 'band_dust_swept', 'eviction')),
  slab_address text,                                 -- the market (no FK: an event must never fail on a missing markets row)
  asset_index  integer,
  actor        text,                                 -- signer / beneficiary named by the instruction
  subject      text,                                 -- portfolio / bond position / units account (the victim for an eviction)
  amount       numeric,                              -- exact atoms when the instruction carries one (deposits); else NULL
  detail       jsonb       NOT NULL DEFAULT '{}'::jsonb,  -- instruction args (min_shares, max_amount, ...): REQUESTED, not settled
  slot         bigint,
  block_time   timestamptz,
  network      text        NOT NULL,
  indexed_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (signature, ix_index, inner_index, network)
);
CREATE INDEX IF NOT EXISTS v22_events_slab_time ON v22_events (slab_address, block_time DESC);
CREATE INDEX IF NOT EXISTS v22_events_kind_time ON v22_events (kind, block_time DESC);
CREATE INDEX IF NOT EXISTS v22_events_actor ON v22_events (actor) WHERE actor IS NOT NULL;

-- Server-side only: the indexer writes with the service role. No client role may read or write this table.
ALTER TABLE v22_events ENABLE ROW LEVEL SECURITY;  -- no policies: anon/authenticated get nothing
REVOKE ALL ON TABLE v22_events FROM anon, authenticated;
REVOKE ALL ON SEQUENCE v22_events_id_seq FROM anon, authenticated;
