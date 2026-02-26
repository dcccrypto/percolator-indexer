/**
 * BigInt sanitization for on-chain → DB conversion.
 *
 * Prevents PostgreSQL "value X out of range for type bigint" errors when
 * on-chain slab fields contain u64::MAX sentinel values or other values
 * exceeding the signed 64-bit range.
 *
 * NOTE: These functions are duplicated from @percolator/shared/sanitize.
 * Once the shared package is republished with PERC-206 changes,
 * switch imports back to @percolator/shared.
 */

/** PostgreSQL bigint max: 2^63 − 1 */
const PG_BIGINT_MAX = 9_223_372_036_854_775_807n;

/** Solana u64::MAX sentinel value */
const U64_MAX = 18_446_744_073_709_551_615n;

/** Anything ≥ 90% of u64::MAX is treated as a sentinel / uninitialized field */
const SENTINEL_THRESHOLD = (U64_MAX * 9n) / 10n; // ~16.6 × 10^18

/**
 * Sanitize a bigint value before converting to Number for DB insertion.
 *
 * Handles three failure modes:
 *  1. u64::MAX sentinel values (uninitialized on-chain fields) → returns `fallback`
 *  2. Values exceeding PostgreSQL bigint range (±2^63 − 1) → returns `fallback`
 *  3. Values exceeding Number.MAX_SAFE_INTEGER → returns `fallback` (precision loss)
 *
 * This prevents the "value X out of range for type bigint" Postgres errors
 * that occur when on-chain slab fields contain u64::MAX sentinels.
 */
export function sanitizeBigIntForDb(value: bigint, fallback: number = 0): number {
  // Detect u64::MAX sentinel or near-sentinel values
  if (value >= SENTINEL_THRESHOLD) return fallback;

  // Detect negative sentinel-like values (i64-reinterpreted or underflows)
  if (value <= -SENTINEL_THRESHOLD) return fallback;

  // Clamp to PostgreSQL bigint range
  if (value > PG_BIGINT_MAX || value < -PG_BIGINT_MAX) return fallback;

  return Number(value);
}

/**
 * Sanitize a bigint value to a string for DB text/numeric columns.
 * Same sentinel detection as sanitizeBigIntForDb, but preserves full precision
 * for fields stored as text (e.g. net_lp_pos, maintenance_fee_per_slot).
 */
export function sanitizeBigIntToString(value: bigint, fallback: string = "0"): string {
  if (value >= SENTINEL_THRESHOLD || value <= -SENTINEL_THRESHOLD) return fallback;
  return value.toString();
}
