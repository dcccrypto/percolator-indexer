-- J-20261120 price_lot_exp: the unit each trade's price is quoted in (per 10^price_lot_exp base tokens; 0 = per
-- token). NULL = unknown (rows written before the indexer knew lot_exp hold per-LOT prices). Additive, nullable,
-- no default: a metadata-only change. Applied to the live indexer DB (Supabase PercolatorIndexer) on 2026-11-20
-- 21:2x GMT with Khubair's approval; IF NOT EXISTS keeps a replay idempotent. No backfill.
ALTER TABLE trades ADD COLUMN IF NOT EXISTS price_lot_exp SMALLINT;
