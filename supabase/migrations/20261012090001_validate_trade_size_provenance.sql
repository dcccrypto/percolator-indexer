-- Validate provenance after the schema migration commits.
-- This avoids holding the ADD CONSTRAINT lock during the scan.

BEGIN;

ALTER TABLE public.trades
  VALIDATE CONSTRAINT trades_size_provenance_check;

COMMIT;
