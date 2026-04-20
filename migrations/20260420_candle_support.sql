-- Phase 3: candle support for Percolator internal-trade chart.
--
-- Apply manually in Supabase SQL editor. Two variants:
--   Variant A (default): plain Postgres — adds an index supporting on-the-fly
--     OHLCV aggregation via date_trunc in the /candles endpoint.
--   Variant B (optional): TimescaleDB — materialized continuous aggregates
--     for faster queries under high trade volume. Requires the timescaledb
--     extension to be enabled on the Supabase project.
--
-- The /candles/:market endpoint in percolator-api is written to work with
-- Variant A out-of-the-box. Variant B is a performance upgrade layered on
-- top; the endpoint can switch to querying candle_1m/5m/... views instead
-- of aggregating trades directly once the views exist.

-- -----------------------------------------------------------------------------
-- Variant A — plain Postgres (always apply first)
-- -----------------------------------------------------------------------------

-- Speed up chart queries that range-scan trades by slab in time order.
-- The existing trades table (defined elsewhere in Supabase) has columns:
--   slab_address text, trader text, side text, size text/numeric,
--   price numeric, fee numeric, tx_signature text, network text,
--   created_at timestamptz.
CREATE INDEX IF NOT EXISTS trades_slab_created_at_idx
  ON trades (slab_address, created_at DESC);

-- -----------------------------------------------------------------------------
-- Variant B — TimescaleDB continuous aggregates (optional performance upgrade)
-- -----------------------------------------------------------------------------
-- Prerequisites (run manually):
--   CREATE EXTENSION IF NOT EXISTS timescaledb;
--   -- Convert trades to a hypertable. Must be empty or use migrate_data=>true:
--   SELECT create_hypertable('trades', 'created_at', if_not_exists => TRUE, migrate_data => TRUE);

-- 1-minute bars.
-- CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1m
-- WITH (timescaledb.continuous) AS
-- SELECT
--   slab_address,
--   time_bucket('1 minute', created_at) AS bucket,
--   first(price, created_at) AS open,
--   max(price)               AS high,
--   min(price)               AS low,
--   last(price, created_at)  AS close,
--   sum(size::numeric)       AS volume
-- FROM trades
-- GROUP BY slab_address, bucket
-- WITH NO DATA;
-- SELECT add_continuous_aggregate_policy('candles_1m',
--   start_offset => INTERVAL '2 hours',
--   end_offset   => INTERVAL '1 minute',
--   schedule_interval => INTERVAL '1 minute');

-- Repeat the block above for 5m ('5 minutes'), 15m ('15 minutes'),
-- 1h ('1 hour'), 4h ('4 hours'), 1d ('1 day') — adjusting start_offset
-- and schedule_interval accordingly (typical: start_offset = 2 * bucket,
-- schedule_interval = bucket).

-- Compression + retention policies (optional):
--   ALTER TABLE trades SET (
--     timescaledb.compress,
--     timescaledb.compress_segmentby = 'slab_address'
--   );
--   SELECT add_compression_policy('trades', INTERVAL '7 days', if_not_exists => TRUE);
--   SELECT add_retention_policy('trades',   INTERVAL '90 days', if_not_exists => TRUE);
