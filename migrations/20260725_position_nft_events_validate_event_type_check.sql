-- Validate the expanded position NFT event-type constraint.
--
-- Apply this file after:
--   20260625_position_nft_events_instruction_index.sql
--
-- This validation is intentionally separated from the constraint creation so
-- it can run in a later transaction with PostgreSQL's lighter validation lock.

ALTER TABLE position_nft_events
  VALIDATE CONSTRAINT position_nft_events_event_type_check;
