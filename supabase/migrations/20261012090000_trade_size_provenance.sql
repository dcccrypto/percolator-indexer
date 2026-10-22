-- Additive historical trade provenance migration.
-- NULL means unknown or not applicable.
-- Existing trade values are not modified.

BEGIN;

ALTER TABLE public.trades
  ADD COLUMN size_provenance TEXT;

ALTER TABLE public.trades
  ADD CONSTRAINT trades_size_provenance_check
  CHECK (
    size_provenance IS NULL OR size_provenance IN (
      'matcher_exact',
      'matcher_request_upper_bound',
      'legacy_unverified'
    )
  ) NOT VALID;

COMMENT ON COLUMN public.trades.size_provenance IS
  'Evidence quality of indexed trade size; NULL means unknown or not applicable.';

COMMIT;
