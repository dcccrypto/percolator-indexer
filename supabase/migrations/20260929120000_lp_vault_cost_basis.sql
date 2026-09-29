-- =============================================================================
-- GH#207: per-user cost basis for the Earn LP vault, so the Earn card can show
-- an exact "earned" figure instead of an estimate.
--
-- The Earn card is the WRAPPER LP vault (v18 wrapper, tags 75/76/77/81), not the
-- stake program. Nothing on-chain records what a depositor paid for their shares
-- (the registry holds only the vault-wide share count), so the indexer derives it
-- from the instructions themselves:
--
--   lp_vault_events     one row per decoded instruction, keyed (signature,
--                       ix_index) so re-delivery and re-polling are idempotent.
--   lp_vault_positions  the average-cost fold of a (registry, user)'s events.
--                       Always RECOMPUTED from lp_vault_events, never
--                       incrementally mutated, so it cannot double-count.
--
-- All amounts are integer atoms (collateral) or raw LP units, stored as
-- numeric(40,0): u128 on-chain, and bigint would overflow. realized_pnl_atoms is
-- signed (a redemption below cost is a loss).
--
-- basis_known is FALSE when the basis cannot be exact: an event whose amount
-- could not be decoded, or the chain showing an LP claim different from the
-- indexed shares (LP tokens are plain SPL and can be transferred without touching
-- the wrapper). Consumers must show an estimate — not an exact figure — then.
--
-- Access: service_role only, like every wallet-keyed table here. The launch app
-- reads it server-side (INDEXER_DATABASE_URL / service client).
-- =============================================================================

CREATE TABLE IF NOT EXISTS lp_vault_events (
  signature        text          NOT NULL,
  -- Top-level instruction i -> i; CPI at position j under top-level i -> 1000*(i+1)+j.
  ix_index         integer       NOT NULL,
  network          text          NOT NULL DEFAULT 'devnet',
  kind             text          NOT NULL
                   CHECK (kind IN ('deposit', 'request_redeem', 'cancel_redeem', 'execute_redeem')),
  program_id       text          NOT NULL,
  -- Market slab. NULL on request/cancel, where the market is not an account.
  market_slab      text,
  registry         text          NOT NULL,
  user_wallet      text          NOT NULL,
  lp_mint          text,
  collateral_atoms numeric(40,0) NOT NULL DEFAULT 0 CHECK (collateral_atoms >= 0),
  -- NULL when the share amount could not be read from the CPIs.
  lp_amount        numeric(40,0)          CHECK (lp_amount IS NULL OR lp_amount >= 0),
  domain           smallint,
  slot             bigint        NOT NULL,
  block_time       timestamptz,
  created_at       timestamptz   NOT NULL DEFAULT now(),
  PRIMARY KEY (signature, ix_index)
);

CREATE INDEX IF NOT EXISTS idx_lp_vault_events_position
  ON lp_vault_events (network, registry, user_wallet, slot);

CREATE TABLE IF NOT EXISTS lp_vault_positions (
  network                text          NOT NULL,
  registry               text          NOT NULL,
  user_wallet            text          NOT NULL,
  market_slab            text,
  lp_mint                text,
  -- Shares the user has a claim on: held + escrowed for a pending redemption.
  lp_shares              numeric(40,0) NOT NULL DEFAULT 0,
  pending_redeem_shares  numeric(40,0) NOT NULL DEFAULT 0,
  cost_basis_atoms       numeric(40,0) NOT NULL DEFAULT 0,
  realized_pnl_atoms     numeric(40,0) NOT NULL DEFAULT 0,
  total_deposited_atoms  numeric(40,0) NOT NULL DEFAULT 0,
  total_redeemed_atoms   numeric(40,0) NOT NULL DEFAULT 0,
  basis_known            boolean       NOT NULL DEFAULT false,
  -- Why the event stream alone cannot support an exact basis (NULL if it can).
  inconsistency          text,
  -- Sticky: context slot at which the chain last disagreed with lp_shares.
  transfer_detected_slot bigint,
  -- Last reconciliation against the chain (held LP + pending redemption shares).
  onchain_lp_shares      numeric(40,0),
  reconciled_slot        bigint,
  event_count            integer       NOT NULL DEFAULT 0,
  updated_slot           bigint        NOT NULL DEFAULT 0,
  updated_at             timestamptz   NOT NULL DEFAULT now(),
  PRIMARY KEY (network, registry, user_wallet)
);

CREATE INDEX IF NOT EXISTS idx_lp_vault_positions_market_user
  ON lp_vault_positions (network, market_slab, user_wallet);

ALTER TABLE lp_vault_events    ENABLE ROW LEVEL SECURITY;
ALTER TABLE lp_vault_positions ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON lp_vault_events    FROM anon, authenticated;
REVOKE ALL ON lp_vault_positions FROM anon, authenticated;
GRANT ALL ON lp_vault_events    TO service_role;
GRANT ALL ON lp_vault_positions TO service_role;
