-- =============================================================================
-- Percolator v17 — Consolidated Supabase/Postgres BASELINE schema (MINIMAL)
-- =============================================================================
-- ONE authoritative schema for a FRESH Supabase project. Apply once, then point
-- the indexer + frontend at it.
--
-- PHILOSOPHY: all live state is on-chain and read directly via RPC
-- (parseMarketGroupV17OI / parsePortfolioV17 / parseWrapperConfigV17). The indexer
-- only persists what a single RPC read CANNOT reconstruct: on-chain EVENT HISTORY.
-- Derived data (candles, 24h volume, leaderboard) is computed from that history.
--
-- This mirrors how dYdX v4 / Drift / Hyperliquid build their indexers: live state
-- from the chain; fills/events saved; aggregates derived (dYdX's "Roundtable").
--
-- TABLES:
--   trades         THE core event history -> candles, 24h volume, leaderboard,
--                  trade + portfolio history. (is_liquidation flags forced closes.)
--   markets        registry — holds OFF-CHAIN metadata (symbol/name/logo/mainnet_ca)
--                  that isn't in the slab, so the UI knows what markets exist.
--   market_stats   thin cache: the 24h volume rollup derived from trades (so the
--                  market LIST shows volume without re-aggregating per request).
--   oracle_markets per-market oracle configuration (read by the oracle UI).
--
-- DROPPED vs the old 63-migration set (0 frontend readers / read live / chart
-- history not needed): oracle_prices, oi_history, insurance_history,
-- funding_history, insurance_snapshots, insurance_lp_events, adl_events,
-- position_nft_events.
--
-- Fixes baked in: H2/H3 trades UNIQUE(tx_signature,asset_index,leg_index) +
-- asset_index/leg_index (multi-fill batch legs no longer collapse); network defined
-- INLINE on every table (the old cross-table network migrations rolled back ->
-- insertTrade threw with no fallback -> the "0 trades" bug); trades(trader) index
-- + BRIN; volume_24h_usd on the cache.
-- =============================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid() (core in PG13+; kept for parity)

-- -----------------------------------------------------------------------------
-- 1. markets — registry + off-chain metadata (natural key: slab_address)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS markets (
  id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  slab_address      TEXT        NOT NULL UNIQUE,
  mint_address      TEXT        NOT NULL,
  symbol            TEXT        NOT NULL DEFAULT 'UNKNOWN',
  name              TEXT        NOT NULL DEFAULT 'Unknown Token',
  decimals          INTEGER     NOT NULL DEFAULT 6,
  deployer          TEXT        NOT NULL,
  oracle_authority  TEXT,
  initial_price_e6  TEXT,
  max_leverage      INTEGER     DEFAULT 10,
  trading_fee_bps   INTEGER     DEFAULT 10,
  lp_collateral     TEXT,
  matcher_context   TEXT,
  logo_url          TEXT,
  mainnet_ca        TEXT,
  oracle_mode       TEXT        NOT NULL DEFAULT 'admin' CHECK (oracle_mode IN ('pyth','hyperp','admin')),
  dex_pool_address  TEXT,
  indexer_excluded  BOOLEAN     NOT NULL DEFAULT false,
  network           TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  status            TEXT        NOT NULL DEFAULT 'active' CHECK (status IN ('active','closed','paused')),
  created_at        TIMESTAMPTZ DEFAULT NOW(),
  updated_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_markets_mint           ON markets(mint_address);
CREATE INDEX IF NOT EXISTS idx_markets_deployer       ON markets(deployer);
CREATE INDEX IF NOT EXISTS idx_markets_oracle_mode    ON markets(oracle_mode);
CREATE INDEX IF NOT EXISTS idx_markets_mainnet_ca     ON markets(mainnet_ca) WHERE mainnet_ca IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_markets_network        ON markets(network);
CREATE INDEX IF NOT EXISTS idx_markets_network_status ON markets(network, status);
CREATE INDEX IF NOT EXISTS idx_markets_status         ON markets(status);

-- -----------------------------------------------------------------------------
-- 2. market_stats — thin volume cache (24h volume/count derived from trades).
--    Live state (price/OI/insurance) is NOT mirrored here — read it from chain.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_stats (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  slab_address     TEXT        NOT NULL UNIQUE REFERENCES markets(slab_address) ON DELETE CASCADE,
  volume_24h       NUMERIC     DEFAULT 0,        -- raw base-asset Q, from trades
  volume_24h_usd   NUMERIC,                      -- denormalized USD
  trade_count_24h  INTEGER     DEFAULT 0,
  last_price       NUMERIC,                      -- last trade price (cheap list convenience)
  network          TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  updated_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_market_stats_updated ON market_stats(updated_at DESC);

-- -----------------------------------------------------------------------------
-- 3. trades — one row per FILL. THE core indexer output.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trades (
  id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  slab_address   TEXT        NOT NULL REFERENCES markets(slab_address) ON DELETE CASCADE,
  trader         TEXT        NOT NULL,
  side           TEXT        NOT NULL CHECK (side IN ('long','short')),
  size           NUMERIC     NOT NULL,
  price          NUMERIC     NOT NULL,
  fee            NUMERIC     DEFAULT 0,
  tx_signature   TEXT,
  asset_index    SMALLINT    NOT NULL DEFAULT 0,      -- H2/H3
  leg_index      SMALLINT    NOT NULL DEFAULT 0,      -- H2/H3
  is_liquidation BOOLEAN     NOT NULL DEFAULT false,  -- forced close via crank (see note)
  network        TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  created_at     TIMESTAMPTZ DEFAULT NOW()
);
-- H2/H3: multi-fill batch legs share a tx_signature; dedupe per (sig, asset, leg).
CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_sig_asset_leg
  ON trades(tx_signature, asset_index, leg_index) WHERE tx_signature IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_trades_slab           ON trades(slab_address, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_trader_created ON trades(trader, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_network        ON trades(network, slab_address, created_at DESC);
CREATE INDEX IF NOT EXISTS brin_trades_created_at    ON trades USING BRIN (created_at);
-- NOTE (liquidations): v17 liquidations run through the crank (tag 5 / action 1)
-- and DON'T carry size/price in the instruction, so is_liquidation rows are markers
-- and MUST be excluded from volume/candle aggregation (WHERE is_liquidation = false).

-- -----------------------------------------------------------------------------
-- 4. oracle_markets — per-market oracle configuration (read by the oracle UI)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS oracle_markets (
  slab_address     TEXT        PRIMARY KEY REFERENCES markets(slab_address) ON DELETE CASCADE,
  oracle_type      TEXT        NOT NULL CHECK (oracle_type IN ('pyth','hyperp','admin')),
  dex_pool_address TEXT,
  pyth_feed_id     TEXT,
  enabled          BOOLEAN     NOT NULL DEFAULT true,
  notes            TEXT,
  created_at       TIMESTAMPTZ DEFAULT NOW(),
  updated_at       TIMESTAMPTZ DEFAULT NOW(),
  CONSTRAINT oracle_markets_hyperp_needs_pool
    CHECK (oracle_type <> 'hyperp' OR dex_pool_address IS NOT NULL)
);

-- -----------------------------------------------------------------------------
-- 5. View — markets_with_stats (the market LIST source: registry + volume cache)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW markets_with_stats AS
SELECT
  m.*,
  s.volume_24h, s.volume_24h_usd, s.trade_count_24h, s.last_price,
  s.updated_at AS stats_updated_at
FROM markets m
LEFT JOIN market_stats s ON m.slab_address = s.slab_address
WHERE COALESCE(m.indexer_excluded, false) = false
  AND m.status <> 'closed';

-- -----------------------------------------------------------------------------
-- 6. Functions & triggers
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at := NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS markets_set_updated_at ON markets;
CREATE TRIGGER markets_set_updated_at BEFORE UPDATE ON markets
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS oracle_markets_set_updated_at ON oracle_markets;
CREATE TRIGGER oracle_markets_set_updated_at BEFORE UPDATE ON oracle_markets
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Optional retention for the one unbounded table (trades). Invoke manually / via cron.
CREATE OR REPLACE FUNCTION cleanup_old_trades(days_to_keep INTEGER DEFAULT 365) RETURNS void AS $$
BEGIN
  DELETE FROM trades WHERE created_at < NOW() - (days_to_keep || ' days')::INTERVAL;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- 7. Row-Level Security & grants
-- -----------------------------------------------------------------------------
-- Supabase provides anon / authenticated / service_role. The indexer uses the
-- service_role key (BYPASSES RLS); the direct postgres pool is privileged. RLS
-- governs the public anon/auth surface; writes are service-role only.

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['markets','market_stats','trades','oracle_markets'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY;', t);
  END LOOP;
END $$;

DROP POLICY IF EXISTS public_read ON markets;        CREATE POLICY public_read ON markets        FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON market_stats;   CREATE POLICY public_read ON market_stats   FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON oracle_markets; CREATE POLICY public_read ON oracle_markets FOR SELECT USING (true);
-- trades: RLS SELECT allowed, but anon is column-restricted via GRANT below.
DROP POLICY IF EXISTS public_read ON trades;         CREATE POLICY public_read ON trades         FOR SELECT USING (true);

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['markets','market_stats','trades','oracle_markets'] LOOP
    EXECUTE format('REVOKE ALL ON %I FROM anon, authenticated;', t);
    EXECUTE format('GRANT ALL ON %I TO service_role;', t);
  END LOOP;
END $$;

GRANT SELECT ON markets, market_stats, oracle_markets TO anon, authenticated;
-- trades: expose only non-PII trade columns publicly (migration 030 parity).
GRANT SELECT (trader, size, created_at) ON trades TO anon;
GRANT SELECT ON trades TO authenticated;
GRANT SELECT ON markets_with_stats TO anon, authenticated, service_role;

COMMIT;

-- Verify:
--   SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY 1;
--   SELECT indexdef FROM pg_indexes WHERE indexname='uq_trades_sig_asset_leg';
