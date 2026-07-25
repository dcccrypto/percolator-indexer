-- Add event-level identity for position NFT events.
--
-- A single Solana transaction can contain multiple TransferPortfolioOwnership
-- instructions. The old signature-only uniqueness model collapses all events in
-- the same transaction into one row.
--
-- Deployment requirement:
--   Pause the NFT indexer before applying this migration, deploy the updated
--   indexer only after the migration succeeds, then resume indexing.
--
-- This migration intentionally avoids CREATE/DROP INDEX CONCURRENTLY so it
-- remains compatible with migration runners that wrap migrations in a
-- transaction.

ALTER TABLE position_nft_events
  ADD COLUMN IF NOT EXISTS instruction_index INT NOT NULL DEFAULT 0;

-- v17 writes TransferPortfolioOwnership events as event_type = 'transfer'.
-- Recreate the existing named constraint so both legacy and v17 event types
-- remain valid.
ALTER TABLE position_nft_events
  DROP CONSTRAINT IF EXISTS position_nft_events_event_type_check;

ALTER TABLE position_nft_events
  ADD CONSTRAINT position_nft_events_event_type_check
  CHECK (event_type IN ('mint', 'burn', 'transfer'));

-- Remove the old signature-only uniqueness model. ALTER TABLE scopes the
-- constraint lookup to position_nft_events and avoids matching a same-named
-- constraint on another relation.
ALTER TABLE position_nft_events
  DROP CONSTRAINT IF EXISTS position_nft_events_signature_key;

-- Some installations may have created signature uniqueness as a standalone
-- index rather than a table constraint.
DROP INDEX IF EXISTS position_nft_events_signature_key;

CREATE UNIQUE INDEX IF NOT EXISTS position_nft_events_signature_instruction_index_key
  ON position_nft_events (signature, instruction_index);
