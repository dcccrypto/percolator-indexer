-- lp_vault_events.domain was SMALLINT (max 32767), but the LP-vault decoder (src/lpVault/decoder.ts) reads it as a
-- u16 (0..65535) from DepositToLpVault / ExecuteRedemption data. A domain is 2*asset (+1 for the odd side), so any
-- asset >= 16384 gives a domain >= 32768: the row failed with 22003 "out of range for type smallint", the whole
-- upsertEvents batch was rejected, and the vault's cursor stayed on that transaction forever.
-- Widen to INTEGER, the same fix as trades.asset_index (20261120120000). No view, function or index references the
-- column (indexes: the (signature, ix_index) primary key and idx_lp_vault_events_position, neither on domain).
-- Note: this rewrites the table under ACCESS EXCLUSIVE; lp_vault_events is small (one row per LP-vault instruction).
BEGIN;
ALTER TABLE public.lp_vault_events ALTER COLUMN domain TYPE integer;
COMMIT;
