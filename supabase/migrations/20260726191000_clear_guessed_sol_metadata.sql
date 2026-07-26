-- =============================================================================
-- Reset market identities the indexer GUESSED as SOL.
--
-- Before the metadata fix, StatsCollector's hyperp branch defaulted to
-- baseSymbol="SOL" / "SOL/USDC Perpetual" whenever base-asset resolution failed.
-- v17 admin-oracle markets have no dexPool, so resolution ALWAYS failed for them
-- and that default was the only branch that ever ran: six unrelated devnet
-- markets all shipped as "SOL/USDC Perpetual".
--
-- This resets those rows to the neutral placeholder the indexer now writes, so
-- the UI shows an honest unnamed market instead of six identical wrong ones.
--
-- The predicate is deliberately narrow — it must not rename a market that
-- genuinely IS SOL:
--   metadata_source = 'auto'      never touch a human's edit
--   dex_pool_address IS NULL      no pool ⇒ base asset was never resolvable
--   mainnet_ca       IS NULL      no CA   ⇒ no other identity hint either
--   symbol           = 'SOL'      only the guessed value
--   logo_url         IS NULL      a resolved SOL market would have gotten a logo
-- A market meeting all five could not have been resolved from chain, so its
-- "SOL" can only have come from the hardcoded default.
-- =============================================================================

UPDATE markets
SET symbol     = 'UNKNOWN',
    name       = 'Market ' || LEFT(slab_address, 8),
    updated_at = NOW()
WHERE metadata_source  = 'auto'
  AND dex_pool_address IS NULL
  AND mainnet_ca       IS NULL
  AND symbol           = 'SOL'
  AND logo_url         IS NULL;
