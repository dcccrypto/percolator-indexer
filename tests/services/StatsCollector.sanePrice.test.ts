/**
 * M1: sanePriceE6 must reject out-of-range sentinel prices. An unset u64 (e.g.
 * u64::MAX) otherwise divides to ~$1.8e13 and poisons oracle_prices + funding_history.
 */
import { describe, it, expect, vi } from 'vitest';

// StatsCollector pulls in @percolatorct/shared/@percolatorct/sdk at import time;
// stub the surfaces its module scope touches so we can import the pure helper.
vi.mock('@percolatorct/shared', () => ({
  config: { allProgramIds: [] },
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  getConnection: vi.fn(),
  getSupabase: vi.fn(),
  getMarkets: vi.fn(),
  getNetwork: vi.fn(() => 'devnet'),
  withRetry: vi.fn(async (fn: any) => fn()),
  captureException: vi.fn(),
  insertOraclePrice: vi.fn(),
  insertMarket: vi.fn(),
  upsertMarketStats: vi.fn(),
  decodeBase58: vi.fn(),
  parseTradeSize: vi.fn(),
}));
vi.mock('@percolatorct/sdk', () => ({
  parseEngine: vi.fn(), parseConfig: vi.fn(), parseParams: vi.fn(), parseAllAccounts: vi.fn(),
  isV17Account: vi.fn(), parseWrapperConfigV17: vi.fn(), parseAssetOracleProfileV17: vi.fn(),
  detectSlabLayout: vi.fn(),
  V17_HEADER_LEN: 16, V17_MARKET_GROUP_OFF: 592, V17_ASSET_ORACLE_PROFILE_LEN: 400,
  IX_TAG: {},
}));

import { sanePriceE6, MAX_SANE_PRICE_E6 } from '../../src/services/StatsCollector.js';

describe('sanePriceE6 (M1)', () => {
  it('passes through an ordinary price', () => {
    expect(sanePriceE6(13_641n)).toBe(13_641n);        // $0.013641
    expect(sanePriceE6(84_000_000n)).toBe(84_000_000n); // $84
  });

  it('drops zero and negatives to 0n', () => {
    expect(sanePriceE6(0n)).toBe(0n);
    expect(sanePriceE6(-5n)).toBe(0n);
  });

  it('drops an unset u64 sentinel (u64::MAX) to 0n', () => {
    expect(sanePriceE6(18_446_744_073_709_551_615n)).toBe(0n); // would be ~$1.8e13
  });

  it('is exclusive at the MAX boundary', () => {
    expect(sanePriceE6(MAX_SANE_PRICE_E6)).toBe(0n);        // 1e12 rejected (== boundary)
    expect(sanePriceE6(MAX_SANE_PRICE_E6 - 1n)).toBe(MAX_SANE_PRICE_E6 - 1n); // just under passes
  });
});
