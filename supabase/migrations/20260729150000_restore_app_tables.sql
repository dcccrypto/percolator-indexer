-- =============================================================================
-- Restore the five application tables the v17 baseline dropped.
--
-- The baseline was scoped to what the INDEXER needs (markets, market_stats,
-- trades, oracle_markets). That was the right call for the indexer and the wrong
-- call for the database as a whole: the frontend uses this same project, and
-- five of its tables went with them.
--
-- The damage was not gradual. `faucet_claims` backs tryFaucetGate
-- (app/lib/faucet-rate-gate.ts), whose pre-check does:
--
--     if (preCheckError) return { allowed: false, nextClaimAt: null };
--
-- A missing table is a query ERROR, not an empty result, so the gate failed
-- CLOSED on every call — and because it returns rather than throws, the caller's
-- catch (which would have engaged the durable Blob fallback) never ran. Every
-- /api/devnet-pre-fund request was answered "Already pre-funded recently",
-- including a wallet's first ever. That broke market creation at the deposit
-- step for everyone: the wizard could not fund the LP seed, so step 4 failed
-- with a rate-limit message on a wallet that had never been funded.
--
-- Fail-closed is correct for a transient DB blip — a faucet must not open
-- because the database hiccuped. It is wrong for "table does not exist", which
-- is a misconfiguration that silently breaks the product forever.
--
-- NOT included, deliberately: `logos` is a Supabase Storage bucket, not a table
-- (.from("logos").getPublicUrl), and `metadata` is a Metaplex PDA seed
-- (Buffer.from("metadata")). Neither belongs here.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- faucet_claims — 24h rate gate for the faucet, devnet pre-fund and devnet mint.
--
-- INSERT-as-gate: UNIQUE(wallet, fund_type) is what makes concurrent claims race
-- to a single winner (the loser gets 23505 and is told when it can retry), so
-- the constraint is load-bearing, not a nicety.
--
-- fund_type is free-form because callers namespace it per resource:
--   "sol" | "usdc" | "auto-fund" | "devnet-pre-fund:<mint>" | "devnet-mint:<ca>"
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS faucet_claims (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  wallet     TEXT        NOT NULL,
  fund_type  TEXT        NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (wallet, fund_type)
);
-- The gate's hot path is (wallet, fund_type, claimed_at >= window).
CREATE INDEX IF NOT EXISTS idx_faucet_claims_lookup
  ON faucet_claims (wallet, fund_type, claimed_at DESC);
-- Expired-claim sweep runs on claimed_at alone.
CREATE INDEX IF NOT EXISTS idx_faucet_claims_claimed_at ON faucet_claims (claimed_at);

-- -----------------------------------------------------------------------------
-- devnet_mints — mainnet CA -> devnet mirror mint, with the token's metadata.
--
-- UNIQUE(mainnet_ca) is also INSERT-as-gate: it is what stops two concurrent
-- launches minting two different devnet mirrors for the same mainnet token.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS devnet_mints (
  id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  mainnet_ca     TEXT        NOT NULL UNIQUE,
  devnet_mint    TEXT        NOT NULL,
  market_address TEXT,
  symbol         TEXT,
  name           TEXT,
  decimals       INTEGER     NOT NULL DEFAULT 6,
  logo_url       TEXT,
  creator_wallet TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- Looked up by devnet mint (the airdrop route resolves back to the mainnet CA).
CREATE INDEX IF NOT EXISTS idx_devnet_mints_devnet_mint ON devnet_mints (devnet_mint);

-- -----------------------------------------------------------------------------
-- devnet_airdrop_claims — per-(wallet, mint) airdrop rate gate.
-- Same INSERT-as-gate shape; claims are cleared once outside the window.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS devnet_airdrop_claims (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  wallet     TEXT        NOT NULL,
  mint       TEXT        NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (wallet, mint)
);
CREATE INDEX IF NOT EXISTS idx_devnet_airdrop_claims_lookup
  ON devnet_airdrop_claims (wallet, mint, claimed_at DESC);

-- -----------------------------------------------------------------------------
-- auto_fund_log — append-only record of what the auto-funder gave a wallet.
-- Diagnostics only; nothing gates on it.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS auto_fund_log (
  id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  wallet         TEXT        NOT NULL,
  sol_airdropped BOOLEAN     NOT NULL DEFAULT false,
  usdc_minted    BOOLEAN     NOT NULL DEFAULT false,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_auto_fund_log_wallet ON auto_fund_log (wallet, created_at DESC);

-- -----------------------------------------------------------------------------
-- market_challenges — nonce store for market-registration wallet-signature auth.
--
-- Largely superseded by lib/playground-nonce-store.ts (Blob-backed), but
-- POST /api/markets still attempts the DB claim first and falls through when it
-- matches nothing. Present so that attempt is a clean 0-row UPDATE rather than a
-- query error.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_challenges (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  nonce      TEXT        NOT NULL UNIQUE,
  deployer   TEXT        NOT NULL,
  client_ip  TEXT,
  used_at    TIMESTAMPTZ,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_market_challenges_claim
  ON market_challenges (nonce, deployer, client_ip) WHERE used_at IS NULL;

-- -----------------------------------------------------------------------------
-- RLS. Every one of these is written only by server-side routes holding the
-- service-role key, and none should be readable by the browser: they contain
-- wallet-level claim history. Enable RLS with NO policy, which denies anon and
-- authenticated outright while service_role continues to bypass it.
-- -----------------------------------------------------------------------------
ALTER TABLE faucet_claims          ENABLE ROW LEVEL SECURITY;
ALTER TABLE devnet_mints           ENABLE ROW LEVEL SECURITY;
ALTER TABLE devnet_airdrop_claims  ENABLE ROW LEVEL SECURITY;
ALTER TABLE auto_fund_log          ENABLE ROW LEVEL SECURITY;
ALTER TABLE market_challenges      ENABLE ROW LEVEL SECURITY;

GRANT SELECT, INSERT, UPDATE, DELETE
  ON faucet_claims, devnet_mints, devnet_airdrop_claims, auto_fund_log, market_challenges
  TO service_role;

COMMIT;
