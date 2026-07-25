-- Add event-level identity for position NFT events.
--
-- A single Solana transaction can contain multiple TransferPortfolioOwnership
-- instructions. The old signature-only uniqueness model collapses all events in
-- the same transaction into one row.
--
-- Deployment requirement:
--   Pause the NFT indexer, apply this migration, then apply
--   20260725_position_nft_events_validate_event_type_check.sql.
--   Deploy the updated indexer only after both migrations succeed, then resume.
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

-- Add the replacement without scanning all existing rows under the initial
-- ALTER TABLE lock. Existing rows are checked by the follow-up validation
-- migration after this transaction completes.
ALTER TABLE position_nft_events
  ADD CONSTRAINT position_nft_events_event_type_check
  CHECK (event_type IN ('mint', 'burn', 'transfer')) NOT VALID;

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
