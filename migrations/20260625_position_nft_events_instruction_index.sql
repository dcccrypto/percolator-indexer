-- Add event-level identity for position NFT events.
--
-- A single Solana transaction can contain multiple TransferPortfolioOwnership
-- instructions. The old signature-only uniqueness model collapses all events in
-- the same transaction into one row.
--
-- This migration adds instruction_index and changes uniqueness to:
--   (signature, instruction_index)

ALTER TABLE position_nft_events
  ADD COLUMN IF NOT EXISTS instruction_index INT NOT NULL DEFAULT 0;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'position_nft_events_signature_key'
  ) THEN
    ALTER TABLE position_nft_events
      DROP CONSTRAINT position_nft_events_signature_key;
  END IF;
END $$;

DROP INDEX IF EXISTS position_nft_events_signature_key;

CREATE UNIQUE INDEX IF NOT EXISTS position_nft_events_signature_instruction_index_key
  ON position_nft_events (signature, instruction_index);
