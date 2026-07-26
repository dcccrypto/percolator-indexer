-- =============================================================================
-- Percolator v17 — Consolidated Supabase/Postgres BASELINE schema
-- =============================================================================
-- ONE authoritative, de-conflicted, optimised schema for a FRESH Supabase project.
-- Replaces the 63 hand-applied, partially-rolled-back migrations in
-- percolator-launch/supabase/migrations. Apply this once in the new project's
-- SQL editor (or `supabase db execute`), then point the indexer + frontend at it.
--
-- Scope: the trading / indexer DATA PLANE — every table the indexer writes and
-- everything the playground reads to display markets, trades, prices, funding,
-- insurance and events. Pure marketing/HR/utility tables (bug_reports, ideas,
-- job_applications, admin_users, devnet_mints, devnet_price_overrides,
-- market_challenges, auto_fund_log, airdrop_claims, faucet_claims,
-- devnet_airdrop_claims) are intentionally NOT included — port them from the
-- existing migrations if the new project also serves the marketing site.
--
-- Fixes baked in vs. the old migration set (see db/BASELINE-NOTES.md):
--   H2/H3  trades: + asset_index, + leg_index, UNIQUE(tx_signature,asset_index,leg_index)
--                  so multi-fill batch legs no longer collapse to one row.
--   #3     network present on every table (correctly, in-file) — the old
--                  20260329170000/180000 migrations referenced a missing column
--                  later in the same txn and rolled back, so `trades.network`
--                  never existed and insertTrade() (which writes it with NO
--                  fallback) threw on every insert. Prime suspect for "0 trades".
--   L6/L15 dropped the broken idx_trades_market_time (trades has no `timestamp`).
--   M5     + idx_trades_trader_created (trader, created_at DESC).
--   M11    market_stats + total_open_interest_usd, + volume_24h_usd, + active_positions,
--                  all exposed through markets_with_stats.
--   L16    BRIN index on trades(created_at) for cheap time-range scans.
--   #1     funding_history: ONE coherent definition (009 shape) + created_at.
--   #2     insurance_snapshots/insurance_lp_events: the 002 shape (live code speaks it).
--   #5     adl_events / position_nft_events: formalized from the code-comment schemas.
--
-- Idempotent-ish: uses IF NOT EXISTS so a partial re-run is safe. Designed to
-- apply cleanly in a single pass on an EMPTY database.
-- =============================================================================

BEGIN;

-- gen_random_uuid() is core in PG13+ (Supabase ships it); pgcrypto kept for parity.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

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
  initial_price_e6  TEXT,                                   -- raw u64 as text (u64-safe)
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
CREATE INDEX IF NOT EXISTS idx_markets_mint          ON markets(mint_address);
CREATE INDEX IF NOT EXISTS idx_markets_deployer      ON markets(deployer);
CREATE INDEX IF NOT EXISTS idx_markets_oracle_mode   ON markets(oracle_mode);
CREATE INDEX IF NOT EXISTS idx_markets_mainnet_ca    ON markets(mainnet_ca) WHERE mainnet_ca IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_markets_network       ON markets(network);
CREATE INDEX IF NOT EXISTS idx_markets_network_status ON markets(network, status);
CREATE INDEX IF NOT EXISTS idx_markets_status        ON markets(status);

-- -----------------------------------------------------------------------------
-- 2. market_stats — one row per market (upserted on slab_address)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_stats (
  id                        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  slab_address              TEXT        NOT NULL UNIQUE REFERENCES markets(slab_address) ON DELETE CASCADE,
  last_price                NUMERIC,
  mark_price                NUMERIC,
  index_price               NUMERIC,
  volume_24h                NUMERIC     DEFAULT 0,           -- raw base-asset Q (see *_usd below)
  volume_total              NUMERIC,
  open_interest_long        NUMERIC     DEFAULT 0,
  open_interest_short       NUMERIC     DEFAULT 0,
  total_open_interest       NUMERIC,                         -- raw base-asset Q
  insurance_fund            NUMERIC     DEFAULT 0,
  total_accounts            INTEGER     DEFAULT 0,           -- allocated portfolio slots (NOT open positions)
  funding_rate              NUMERIC     DEFAULT 0,
  warmup_period_slots       BIGINT,
  net_lp_pos                NUMERIC,
  lp_sum_abs                NUMERIC,
  lp_max_abs                NUMERIC,
  insurance_balance         NUMERIC,
  insurance_fee_revenue     NUMERIC,
  vault_balance             NUMERIC     DEFAULT 0,
  lifetime_liquidations     NUMERIC     DEFAULT 0,           -- NUMERIC: u64 can exceed PG bigint max
  lifetime_force_closes     NUMERIC     DEFAULT 0,
  c_tot                     NUMERIC     DEFAULT 0,
  pnl_pos_tot               NUMERIC     DEFAULT 0,
  last_crank_slot           BIGINT      DEFAULT 0,
  max_crank_staleness_slots BIGINT      DEFAULT 0,
  maintenance_fee_per_slot  TEXT        DEFAULT '0',         -- u128 as text
  liquidation_fee_bps       BIGINT      DEFAULT 0,
  liquidation_fee_cap       TEXT        DEFAULT '0',         -- u128 as text
  liquidation_buffer_bps    BIGINT      DEFAULT 0,
  trade_count_24h           INTEGER     DEFAULT 0,
  network                   TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  -- M11: denormalized USD, computed at write time (raw quantities kept above for audit)
  total_open_interest_usd   NUMERIC,
  volume_24h_usd            NUMERIC,
  -- L13: open positions, distinct from total_accounts (allocated-but-empty slots)
  active_positions          INTEGER     DEFAULT 0,
  updated_at                TIMESTAMPTZ DEFAULT NOW()
);
-- NOTE: UNIQUE is on slab_address ALONE — upsertMarketStats() uses onConflict:"slab_address".
CREATE INDEX IF NOT EXISTS idx_market_stats_updated ON market_stats(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_market_stats_network ON market_stats(network);

-- -----------------------------------------------------------------------------
-- 3. trades — one row per FILL (H2/H3: per-leg unique key)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trades (
  id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  slab_address  TEXT        NOT NULL REFERENCES markets(slab_address) ON DELETE CASCADE,
  trader        TEXT        NOT NULL,
  side          TEXT        NOT NULL CHECK (side IN ('long','short')),
  size          NUMERIC     NOT NULL,                        -- i128 abs magnitude as numeric
  price         NUMERIC     NOT NULL,
  fee           NUMERIC     DEFAULT 0,
  tx_signature  TEXT,
  -- H2/H3: asset_index + leg_index disambiguate legs of a multi-fill batch tx.
  -- NOT NULL DEFAULT 0 so the composite UNIQUE actually dedupes (NULLs would be
  -- treated as distinct and defeat it). Legacy single fills = (asset_index 0..N, leg 0).
  asset_index   SMALLINT    NOT NULL DEFAULT 0,
  leg_index     SMALLINT    NOT NULL DEFAULT 0,
  network       TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  created_at    TIMESTAMPTZ DEFAULT NOW()
);
-- H2/H3: dedupe per (signature, asset_index, leg_index), only when we have a signature.
CREATE UNIQUE INDEX IF NOT EXISTS uq_trades_sig_asset_leg
  ON trades(tx_signature, asset_index, leg_index) WHERE tx_signature IS NOT NULL;
-- Read paths:
CREATE INDEX IF NOT EXISTS idx_trades_slab          ON trades(slab_address, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_trades_trader_created ON trades(trader, created_at DESC);   -- M5
CREATE INDEX IF NOT EXISTS idx_trades_network       ON trades(network, slab_address, created_at DESC);
CREATE INDEX IF NOT EXISTS brin_trades_created_at   ON trades USING BRIN (created_at);      -- L16
-- (Dropped: idx_trades_market_time on trades(timestamp) — column never existed. L6/L15.)

-- -----------------------------------------------------------------------------
-- 4. oracle_prices — append-only price history (no unique key by design)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS oracle_prices (
  id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  slab_address  TEXT        NOT NULL REFERENCES markets(slab_address) ON DELETE CASCADE,
  price_e6      TEXT        NOT NULL,                        -- raw u64 as text
  source        TEXT        DEFAULT 'admin',
  "timestamp"   BIGINT      NOT NULL,                        -- epoch seconds (this table really has it)
  tx_signature  TEXT,
  network       TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_oracle_prices_slab_time ON oracle_prices(slab_address, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_oracle_prices_network   ON oracle_prices(network, slab_address, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_oracle_prices_tx_sig    ON oracle_prices(tx_signature) WHERE tx_signature IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 5. oracle_markets — per-market oracle configuration
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
-- 6. History tables (append-mostly time series, keyed by (market_slab, slot))
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
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),   -- #1: exists for real (TS type assumed it)
  UNIQUE (market_slab, slot)
);
CREATE INDEX IF NOT EXISTS idx_funding_history_market_time ON funding_history(market_slab, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_funding_history_slot        ON funding_history(market_slab, slot DESC);
CREATE INDEX IF NOT EXISTS idx_funding_history_network     ON funding_history(network, market_slab, "timestamp" DESC);

CREATE TABLE IF NOT EXISTS insurance_history (
  id          BIGSERIAL   PRIMARY KEY,
  market_slab TEXT        NOT NULL REFERENCES markets(slab_address) ON DELETE CASCADE,
  slot        BIGINT      NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  balance     NUMERIC     NOT NULL,
  fee_revenue NUMERIC     NOT NULL,
  network     TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  UNIQUE (market_slab, slot)
);
CREATE INDEX IF NOT EXISTS idx_insurance_history_slab_time ON insurance_history(market_slab, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_insurance_history_slab_slot ON insurance_history(market_slab, slot DESC);
CREATE INDEX IF NOT EXISTS idx_insurance_history_network   ON insurance_history(network, market_slab, "timestamp" DESC);

CREATE TABLE IF NOT EXISTS oi_history (
  id          BIGSERIAL   PRIMARY KEY,
  market_slab TEXT        NOT NULL REFERENCES markets(slab_address) ON DELETE CASCADE,
  slot        BIGINT      NOT NULL,
  "timestamp" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  total_oi    NUMERIC     NOT NULL,
  net_lp_pos  NUMERIC     NOT NULL,
  lp_sum_abs  NUMERIC     NOT NULL,
  lp_max_abs  NUMERIC     NOT NULL,
  network     TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  UNIQUE (market_slab, slot)
);
CREATE INDEX IF NOT EXISTS idx_oi_history_slab_time ON oi_history(market_slab, "timestamp" DESC);
CREATE INDEX IF NOT EXISTS idx_oi_history_slab_slot ON oi_history(market_slab, slot DESC);
CREATE INDEX IF NOT EXISTS idx_oi_history_network   ON oi_history(network, market_slab, "timestamp" DESC);

-- -----------------------------------------------------------------------------
-- 7. Insurance / LP (002 shape — the one live code speaks; append-only)
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
-- 8. Event tables (formalized from AdlIndexer/NftIndexer code-comment schemas)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS adl_events (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  signature  TEXT        NOT NULL UNIQUE,
  slab       TEXT        NOT NULL,
  target_idx INTEGER     NOT NULL,
  slot       BIGINT      NOT NULL,
  "timestamp" BIGINT     NOT NULL,
  network    TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_adl_events_slab   ON adl_events(slab);
CREATE INDEX IF NOT EXISTS idx_adl_events_slot   ON adl_events(slot);
CREATE INDEX IF NOT EXISTS idx_adl_events_target ON adl_events(target_idx);

CREATE TABLE IF NOT EXISTS position_nft_events (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  signature  TEXT        NOT NULL UNIQUE,                    -- upsertNftEvent onConflict:"signature"
  event_type TEXT        NOT NULL CHECK (event_type IN ('mint','burn','transfer')),
  slab       TEXT        NOT NULL,
  user_idx   INTEGER     NOT NULL,
  owner      TEXT        NOT NULL,
  nft_mint   TEXT,
  slot       BIGINT      NOT NULL,
  "timestamp" BIGINT     NOT NULL,
  network    TEXT        NOT NULL DEFAULT 'devnet' CHECK (network IN ('devnet','mainnet')),
  created_at TIMESTAMPTZ DEFAULT NOW()
);
-- FUTURE (#168): to index multiple TransferPortfolioOwnership per tx, add
--   instruction_index SMALLINT NOT NULL DEFAULT 0
-- and swap UNIQUE(signature) -> UNIQUE(signature, instruction_index) IN LOCKSTEP
-- with upsertNftEvent()'s onConflict. Left as single-key here to match current code.
CREATE INDEX IF NOT EXISTS idx_position_nft_events_slab  ON position_nft_events(slab);
CREATE INDEX IF NOT EXISTS idx_position_nft_events_owner ON position_nft_events(owner);
CREATE INDEX IF NOT EXISTS idx_position_nft_events_slot  ON position_nft_events(slot);

-- -----------------------------------------------------------------------------
-- 9. Views
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
  -- M11: USD + open-position columns surfaced to the frontend
  s.total_open_interest_usd, s.volume_24h_usd, s.active_positions,
  s.updated_at AS stats_updated_at
FROM markets m
LEFT JOIN market_stats s ON m.slab_address = s.slab_address
WHERE COALESCE(m.indexer_excluded, false) = false
  AND m.status <> 'closed';

CREATE OR REPLACE VIEW insurance_fund_health AS
SELECT
  m.slab_address,
  m.insurance_balance,
  m.insurance_fee_revenue,
  m.total_open_interest,
  CASE WHEN m.total_open_interest > 0
       THEN m.insurance_balance / m.total_open_interest ELSE NULL END AS health_ratio,
  COALESCE(
    m.insurance_fee_revenue - LAG(m.insurance_fee_revenue)
      OVER (PARTITION BY m.slab_address ORDER BY m.updated_at), 0) AS fee_growth_24h
FROM market_stats m
ORDER BY m.slab_address;

CREATE OR REPLACE VIEW oi_imbalance AS
SELECT
  m.slab_address, m.total_open_interest, m.net_lp_pos, m.lp_sum_abs, m.lp_max_abs,
  (m.total_open_interest - m.net_lp_pos) / 2 AS long_oi,
  (m.total_open_interest + m.net_lp_pos) / 2 AS short_oi,
  CASE WHEN m.total_open_interest > 0
       THEN (m.net_lp_pos * 100.0 / m.total_open_interest) ELSE 0 END AS imbalance_percent
FROM market_stats m
ORDER BY m.slab_address;

-- -----------------------------------------------------------------------------
-- 10. Functions & triggers
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

-- Manual/cron pruning helper for the append-only history tables.
CREATE OR REPLACE FUNCTION cleanup_old_history(days_to_keep INTEGER DEFAULT 90) RETURNS void AS $$
BEGIN
  DELETE FROM insurance_history WHERE "timestamp" < NOW() - (days_to_keep || ' days')::INTERVAL;
  DELETE FROM oi_history        WHERE "timestamp" < NOW() - (days_to_keep || ' days')::INTERVAL;
  DELETE FROM funding_history   WHERE "timestamp" < NOW() - (days_to_keep || ' days')::INTERVAL;
END;
$$ LANGUAGE plpgsql;

-- -----------------------------------------------------------------------------
-- 11. Row-Level Security & grants
-- -----------------------------------------------------------------------------
-- Supabase provides roles: anon, authenticated, service_role. The indexer uses
-- the service_role key (BYPASSES RLS); the direct postgres pool connects as a
-- privileged role (also bypasses RLS). RLS below governs the public anon/auth
-- API surface only. Writes are service-role only (no anon/auth write policy).

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'markets','market_stats','trades','oracle_prices','oracle_markets',
    'funding_history','insurance_history','oi_history',
    'insurance_snapshots','insurance_lp_events','adl_events','position_nft_events'
  ] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY;', t);
  END LOOP;
END $$;

-- Public read policies (SELECT) for the display tables.
DROP POLICY IF EXISTS public_read ON markets;             CREATE POLICY public_read ON markets             FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON market_stats;        CREATE POLICY public_read ON market_stats        FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON oracle_prices;       CREATE POLICY public_read ON oracle_prices       FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON oracle_markets;      CREATE POLICY public_read ON oracle_markets      FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON funding_history;     CREATE POLICY public_read ON funding_history     FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON insurance_history;   CREATE POLICY public_read ON insurance_history   FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON oi_history;          CREATE POLICY public_read ON oi_history          FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON insurance_snapshots; CREATE POLICY public_read ON insurance_snapshots FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON insurance_lp_events; CREATE POLICY public_read ON insurance_lp_events FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON adl_events;          CREATE POLICY public_read ON adl_events          FOR SELECT USING (true);
DROP POLICY IF EXISTS public_read ON position_nft_events; CREATE POLICY public_read ON position_nft_events FOR SELECT USING (true);
-- trades: RLS SELECT allowed, but anon is column-restricted via GRANT below
-- (public leaderboard exposes only trader/size/created_at — migration 030 parity).
DROP POLICY IF EXISTS public_read ON trades;              CREATE POLICY public_read ON trades              FOR SELECT USING (true);

-- Column-level grants. Revoke broad table grants first, then re-grant precisely.
DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'markets','market_stats','trades','oracle_prices','oracle_markets',
    'funding_history','insurance_history','oi_history',
    'insurance_snapshots','insurance_lp_events','adl_events','position_nft_events'
  ] LOOP
    EXECUTE format('REVOKE ALL ON %I FROM anon, authenticated;', t);
    EXECUTE format('GRANT ALL ON %I TO service_role;', t);
  END LOOP;
END $$;

-- Read grants for anon/authenticated (RLS still applies on top).
GRANT SELECT ON markets, market_stats, oracle_prices, oracle_markets,
  funding_history, insurance_history, oi_history,
  insurance_snapshots, insurance_lp_events, adl_events, position_nft_events
  TO anon, authenticated;
-- trades: expose only non-PII trade columns publicly (migration 030 parity).
GRANT SELECT (trader, size, created_at) ON trades TO anon;
GRANT SELECT ON trades TO authenticated;
-- Views inherit RLS from their base tables; grant SELECT so anon/auth can read them.
GRANT SELECT ON markets_with_stats, insurance_fund_health, oi_imbalance TO anon, authenticated, service_role;

COMMIT;

-- =============================================================================
-- End of baseline. After apply, verify with:
--   SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY 1;
--   SELECT conname, pg_get_constraintdef(oid) FROM pg_constraint
--     WHERE conrelid='trades'::regclass;   -- expect uq_trades_sig_asset_leg
-- =============================================================================
