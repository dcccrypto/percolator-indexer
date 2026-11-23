// MARKETS_FILTER: empty string vs unset vs all-invalid in MarketDiscovery (kills M24 M25 M26 M28).
// Mocks copied from tests/services/MarketDiscovery.test.ts.
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

vi.mock('../../src/v17/discovery.js', () => ({
  discoverV17Markets: vi.fn(async () => []),
}));
vi.mock('@percolatorct/sdk', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@percolatorct/sdk')>()),
  discoverMarkets: vi.fn(async () => []),
  getMarketsByAddress: vi.fn(async () => []),
  isV17Account: vi.fn(() => false),
}));
const mockPrimaryConnection = { getProgramAccounts: vi.fn(async () => []) };
const mockFallbackConnection = { getProgramAccounts: vi.fn(async () => []) };
vi.mock('@percolatorct/shared', () => ({
  config: { allProgramIds: ['11111111111111111111111111111111', 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'] },
  createLogger: vi.fn(() => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() })),
  captureException: vi.fn(),
  getConnection: vi.fn(() => mockPrimaryConnection),
  getPrimaryConnection: vi.fn(() => mockPrimaryConnection),
  getFallbackConnection: vi.fn(() => mockFallbackConnection),
}));

import { MarketDiscovery } from '../../src/services/MarketDiscovery.js';
import * as core from '@percolatorct/sdk';
import * as v17disc from '../../src/v17/discovery.js';

async function drive<T>(p: Promise<T>): Promise<T> {
  let done = false;
  const w = p.finally(() => { done = true; });
  w.catch(() => {});
  for (let i = 0; !done; i++) {
    if (i > 600) throw new Error('drive(): still pending after 600 s of fake time');
    await vi.advanceTimersByTimeAsync(1_000);
  }
  return w;
}

describe('MARKETS_FILTER empty vs unset vs all-invalid (liveness)', () => {
  let orig: string | undefined;
  let d: MarketDiscovery;
  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
    orig = process.env.MARKETS_FILTER;
    d = new MarketDiscovery();
  });
  afterEach(() => {
    d.stop();
    vi.useRealTimers();
    if (orig === undefined) delete process.env.MARKETS_FILTER; else process.env.MARKETS_FILTER = orig;
  });

  for (const [name, v] of [['empty string', ''], ['whitespace only', '  \n']] as const) {
    it(`K-M25/M26: full pass with MARKETS_FILTER=${name} scans every program (same as unset), never the filter path`, async () => {
      process.env.MARKETS_FILTER = v;
      await drive(d.discover());
      const calls = vi.mocked(v17disc.discoverV17Markets).mock.calls;
      expect(calls).toHaveLength(2); // two mocked program ids
      for (const c of calls) expect(c[2]).toBeUndefined();
      expect(core.getMarketsByAddress).not.toHaveBeenCalled();
    });
    it(`K-M24: light pass with MARKETS_FILTER=${name} scans every program with no address list`, async () => {
      process.env.MARKETS_FILTER = v;
      await d.discoverLight();
      const calls = vi.mocked(v17disc.discoverV17Markets).mock.calls;
      expect(calls).toHaveLength(2);
      for (const c of calls) { expect(c[2]).toBeUndefined(); expect(c[3]).toEqual({ light: true }); }
    });
  }

  it('K-M28: light pass with an all-invalid MARKETS_FILTER makes no scan (an empty list would full-scan)', async () => {
    process.env.MARKETS_FILTER = 'oops, nope';
    expect(await d.discoverLight()).toEqual([]);
    expect(v17disc.discoverV17Markets).not.toHaveBeenCalled();
  });
});
