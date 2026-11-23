-- trades.asset_index was SMALLINT (max 32767) in the baseline, but the trade writer
-- (src/db/insertTradeRow.ts) sends a u16 (0..65535) and v22_events.asset_index is already
-- INTEGER. A fill with asset_index 32768..65535 failed with 22003 "out of range for type smallint".
-- Widen to INTEGER. No view or function references the column; the three indexes that include it
-- (unique dedupe key, position lookup, covering read) are rebuilt by the rewrite and stay valid.
-- Column grants, the DEFAULT 0 and NOT NULL are preserved by ALTER TYPE.
-- Note: this rewrites the table under ACCESS EXCLUSIVE; run in a quiet window on a large trades table.
BEGIN;
ALTER TABLE public.trades ALTER COLUMN asset_index TYPE integer;
COMMIT;
