-- =============================================================================
-- Percolator v17 — Consolidated Supabase/Postgres BASELINE schema (REDUCED)
-- =============================================================================
-- ONE authoritative, de-conflicted, optimised schema for a FRESH Supabase project.
-- Apply once in the new project's SQL editor, then point the indexer + frontend at it.
--
-- REDUCED SCOPE (2026-07-26): the frontend now reads price/OI/positions/config
-- LIVE from chain (parseMarketGroupV17OI / parsePortfolioV17 / parseWrapperConfigV17),
-- so the indexer only persists what a single RPC read CANNOT give: trade history
-- and time-series/aggregates. Tables with ZERO frontend readers were dropped:
--   oracle_prices, oi_history, insurance_history, adl_events, position_nft_events
-- (and their unused views insurance_fund_health / oi_imbalance). The NftIndexer +
-- AdlIndexer services were removed from the indexer.
--
-- KEPT (real consumers):
--   trades              -> trade feed, candles, 24h volume, leaderboard, history
--   market_stats        -> fast bulk snapshot for the market LIST (markets_with_stats)
--   markets             -> registry
--   funding_history     -> /api/funding/[slab] + /api/funding/global
--   insurance_snapshots -> LP-vault APY time-series (/api/stake/pools)
--   insurance_lp_events -> deposit/withdraw log (written by the FE, not the indexer)
--   oracle_markets      -> oracle configuration
--
-- Fixes baked in: H2/H3 trades UNIQUE(tx_signature,asset_index,leg_index) +
-- asset_index/leg_index; market_stats *_usd + active_positions; coherent
-- funding_history + created_at; 002-shape insurance tables; network defined INLINE
-- on every table (the old cross-table network migrations rolled back -> insertTrade
-- threw with no fallback -> the "0 trades" bug); dropped the broken trades(timestamp)
-- index; added trades(trader) index + BRIN; markets_with_stats exposes USD/positions.
--
-- Designed to apply cleanly in a single pass on an EMPTY database.
-- =============================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid() (core in PG13+; kept for parity)

-- -----------------------------------------------------------------------------
-- 1. markets — root registry (natural key: slab_address)
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
-- 2. market_stats — one row per market (upserted on slab_address).
--    A light bulk snapshot so the market LIST avoids N per-market RPC reads.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_stats (
  id                        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  slab_address              TEXT        NOT NULL UNIQUE REFERENCES markets(slab_address) ON DELETE CASCADE,
  last_price                NUMERIC,
  mark_price                NUMERIC,
  index_price               NUMERIC,
  volume_24h                NUMERIC     DEFAULT 0,           -- raw base-asset Q
  volume_total              NUMERIC,
  open_interest_long        NUMERIC     DEFAULT 0,
  open_interest_short       NUMERIC     DEFAULT 0,
  total_open_interest       NUMERIC,                         -- raw base-asset Q
  insurance_fund            NUMERIC     DEFAULT 0,
  total_accounts            INTEGER     DEFAULT 0,           -- allocated slots (NOT open positions)
  funding_rate              NUMERIC     DEFAULT 0,
  warmup_period_slots       BIGINT,
  net_lp_pos                NUMERIC,
  lp_sum_abs                NUMERIC,
  lp_max_abs                NUMERIC,
  insurance_balance         NUMERIC,
  insurance_fee_revenue     NUMERIC,
  vault_balance             NUMERIC     DEFAULT 0,
  lifetime_liquidations     NUMERIC     DEFAULT 0,
  lifetime_force_closes     NUMERIC     DEFAULT 0,
  c_tot                     NUMERIC     DEFAULT 0,
  pnl_pos_tot               NUMERIC     DEFAULT 0,
  last_crank_slot           BIGINT      DEFAULT 0,
  max_crank_staleness_slots BIGINT      DEFAULT 0,
  maintenance_fee_per_slot  TEXT        DEFAULT '0',
  liquidation_fee_bps       BIGINT      DEFAULT 0,
  liquidation_fee_cap       TEXT        DEFAULT '0',
  liquidation_buffer_bps    BIGINT      DEFAULT 0,
  trade_count_24h           INTEGER     DEFAULT 0,
  network                   TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  total_open_interest_usd   NUMERIC,                         -- M11: denormalized USD
  volume_24h_usd            NUMERIC,
  active_positions          INTEGER     DEFAULT 0,           -- live open positions (engine stored_pos_count)
  updated_at                TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_market_stats_updated ON market_stats(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_stats_network ON market_stats(network);

-- -----------------------------------------------------------------------------
-- 3. trades — one row per FILL. THE core indexer output (candles/volume/leaderboard).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trades (
  id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  slab_address  TEXT        NOT NULL REFERENCES markets(slab_address) ON DELETE CASCADE,
  trader        TEXT        NOT NULL,
  side          TEXT        NOT NULL CHECK (side IN ('long','short')),
  size          NUMERIC     NOT NULL,
  price         NUMERIC     NOT NULL,
  fee           NUMERIC     DEFAULT 0,
  tx_signature  TEXT,
  asset_index   SMALLINT    NOT NULL DEFAULT 0,   -- H2/H3
  leg_index     SMALLINT    NOT NULL DEFAULT 0,   -- H2/H3
  network       TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  created_at    TIMESTAMPTZ DEFAULT NOW()
);
-- H2/H3: multi-fill batch legs share a tx_signature; dedupe per (sig, asset, leg).
CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_sig_asset_leg
  ON trades(tx_signature, asset_index, leg_index) WHERE tx_signature IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_trades_slab           ON trades(slab_address, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_trader_created ON trades(trader, created_at DESC);   -- M5
CREATE INDEX IF NOT EXISTS idx_trades_network        ON trades(network, slab_address, created_at DESC);
CREATE INDEX IF NOT EXISTS brin_trades_created_at    ON trades USING BRIN (created_at);      -- L16
-- (Dropped: idx_trades_market_time on trades(timestamp) — that column never existed.)

-- -----------------------------------------------------------------------------
-- 4. oracle_markets — per-market oracle configuration (not indexer time-series)
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
-- 5. funding_history — the one history time-series still consumed (/api/funding)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS funding_history (
  id                   BIGSERIAL   PRIMARY KEY,
  market_slab          TEXT        NOT NULL REFERENCES markets(slab_address) ON DELETE CASCADE,
  slot                 BIGINT      NOT NULL,
  "timestamp"          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  rate_bps_per_slot    NUMERIC     NOT NULL DEFAULT 0,
  net_lp_pos           NUMERIC     NOT NULL DEFAULT 0,
  price_e6             NUMERIC     NOT NULL DEFAULT 0,
  funding_index_qpb_e6 TEXT        NOT NULL DEFAULT '0',
  network              TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (market_slab, slot)
);
CREATE INDEX IF NOT EXISTS idx_funding_history_market_time ON funding_history(market_slab, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_funding_history_slot        ON funding_history(market_slab, slot DESC);
CREATE INDEX IF NOT EXISTS idx_funding_history_network     ON funding_history(network, market_slab, "timestamp" DESC);

-- -----------------------------------------------------------------------------
-- 6. Insurance / LP (002 shape — the one live code speaks; append-only)
--    insurance_snapshots: written by the indexer (APY time-series).
--    insurance_lp_events: written by the FE deposit/withdraw path (kept for completeness).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS insurance_snapshots (
  id                 BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  slab               TEXT        NOT NULL,
  insurance_balance  BIGINT      NOT NULL,
  lp_supply          BIGINT      NOT NULL,
  redemption_rate_e6 BIGINT      NOT NULL,
  snapshot_slot      BIGINT      NOT NULL,
  network            TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_insurance_snapshots_slab_created ON insurance_snapshots(slab, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_insurance_snapshots_network      ON insurance_snapshots(network, slab, created_at DESC);

CREATE TABLE IF NOT EXISTS insurance_lp_events (
  id                       BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  slab                     TEXT        NOT NULL,
  user_wallet              TEXT        NOT NULL,
  event_type               TEXT        NOT NULL CHECK (event_type IN ('deposit','withdraw')),
  collateral_amount        BIGINT      NOT NULL,
  lp_tokens                BIGINT      NOT NULL,
  insurance_balance_before BIGINT      NOT NULL,
  lp_supply_before         BIGINT      NOT NULL,
  tx_signature             TEXT        NOT NULL,
  network                  TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_insurance_lp_events_slab_created ON insurance_lp_events(slab, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_insurance_lp_events_network      ON insurance_lp_events(network, slab, created_at DESC);

-- -----------------------------------------------------------------------------
-- 7. View — markets_with_stats (the market LIST source; USD/positions surfaced)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW markets_with_stats AS
SELECT
  m.*,
  s.last_price, s.mark_price, s.index_price,
  s.volume_24h, s.volume_total,
  s.open_interest_long, s.open_interest_short,
  s.insurance_fund, s.total_accounts, s.funding_rate,
  s.total_open_interest, s.net_lp_pos, s.lp_sum_abs, s.lp_max_abs,
  s.insurance_balance, s.insurance_fee_revenue, s.warmup_period_slots,
  s.vault_balance, s.lifetime_liquidations, s.lifetime_force_closes,
  s.c_tot, s.pnl_pos_tot, s.last_crank_slot, s.max_crank_staleness_slots,
  s.maintenance_fee_per_slot, s.liquidation_fee_bps, s.liquidation_fee_cap,
  s.liquidation_buffer_bps, s.trade_count_24h,
  s.total_open_interest_usd, s.volume_24h_usd, s.active_positions,
  s.updated_at AS stats_updated_at
FROM markets m
LEFT JOIN market_stats s ON m.slab_address = s.slab_address
WHERE COALESCE(m.indexer_excluded, false) = false
  AND m.status <> 'closed';

-- -----------------------------------------------------------------------------
-- 8. Functions & triggers
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

-- Manual/cron pruning for the funding_history time-series.
CREATE OR REPLACE FUNCTION cleanup_old_history(days_to_keep INTEGER DEFAULT 90) RETURNS void AS $$
BEGIN
  DELETE FROM funding_history WHERE "timestamp" < NOW() - (days_to_keep || ' days')::INTERVAL;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- 9. Row-Level Security & grants
-- -----------------------------------------------------------------------------
-- Supabase provides anon / authenticated / service_role. The indexer uses the
-- service_role key (BYPASSES RLS); the direct postgres pool is privileged (also
-- bypasses RLS). RLS below governs the public anon/auth surface; writes are
-- service-role only (no anon/auth write policy).

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'markets','market_stats','trades','oracle_markets',
    'funding_history','insurance_snapshots','insurance_lp_events'
  ] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY;', t);
  END LOOP;
END $$;

DROP POLICY IF EXISTS public_read ON markets;             CREATE POLICY public_read ON markets             FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON market_stats;        CREATE POLICY public_read ON market_stats        FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON oracle_markets;      CREATE POLICY public_read ON oracle_markets      FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON funding_history;     CREATE POLICY public_read ON funding_history     FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON insurance_snapshots; CREATE POLICY public_read ON insurance_snapshots FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON insurance_lp_events; CREATE POLICY public_read ON insurance_lp_events FOR SELECT USING (true);
-- trades: RLS SELECT allowed, but anon is column-restricted via GRANT below.
DROP POLICY IF EXISTS public_read ON trades;              CREATE POLICY public_read ON trades              FOR SELECT USING (true);

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'markets','market_stats','trades','oracle_markets',
    'funding_history','insurance_snapshots','insurance_lp_events'
  ] LOOP
    EXECUTE format('REVOKE ALL ON %I FROM anon, authenticated;', t);
    EXECUTE format('GRANT ALL ON %I TO service_role;', t);
  END LOOP;
END $$;

GRANT SELECT ON markets, market_stats, oracle_markets,
  funding_history, insurance_snapshots, insurance_lp_events
  TO anon, authenticated;
-- trades: expose only non-PII trade columns publicly (migration 030 parity).
GRANT SELECT (trader, size, created_at) ON trades TO anon;
GRANT SELECT ON trades TO authenticated;
GRANT SELECT ON markets_with_stats TO anon, authenticated, service_role;

COMMIT;

-- Verify:
--   SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY 1;
--   SELECT indexdef FROM pg_indexes WHERE indexname='uq_trades_sig_asset_leg';
