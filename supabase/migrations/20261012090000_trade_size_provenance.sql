-- Additive historical trade provenance migration.
-- NULL means unknown or not applicable.
-- Existing trade values are not modified.
-- Idempotent: a second apply (or an apply on a DB where it was run by hand) is a no-op.

BEGIN;

ALTER TABLE public.trades
  ADD COLUMN IF NOT EXISTS size_provenance TEXT;

-- Postgres has no ADD CONSTRAINT IF NOT EXISTS; add it only when the name is not taken yet
-- (an existing one, validated by 20261012090001 or not, is left as it is).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.trades'::regclass AND conname = 'trades_size_provenance_check'
  ) THEN
    ALTER TABLE public.trades
      ADD CONSTRAINT trades_size_provenance_check
      CHECK (
        size_provenance IS NULL OR size_provenance IN (
          'matcher_exact',
          'matcher_request_upper_bound',
          'legacy_unverified'
        )
      ) NOT VALID;
  END IF;
END $$;

COMMENT ON COLUMN public.trades.size_provenance IS
  'Evidence quality of indexed trade size; NULL means unknown or not applicable.';

COMMIT;
