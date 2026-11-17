/** Env interval parsing (leads: hex/exp/fraction/overflow) + inPlaceholderGrace skew. */
import { describe, it, expect, vi, afterEach } from 'vitest';


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
vi.mock('@percolatorct/sdk', async (importOriginal) => ({
  // v2.2: the indexer's layout code uses the REAL SDK layout tables; tests override only what they stub.
  ...(await importOriginal<typeof import('@percolatorct/sdk')>()),
  parseEngine: vi.fn(), parseConfig: vi.fn(), parseParams: vi.fn(), parseAllAccounts: vi.fn(),
  isV17Account: vi.fn(), parseWrapperConfigV17: vi.fn(), parseAssetOracleProfileV17: vi.fn(),
  detectSlabLayout: vi.fn(),
  V17_HEADER_LEN: 16, V17_MARKET_GROUP_OFF: 592, V17_ASSET_ORACLE_PROFILE_LEN: 400,
  IX_TAG: {},
}));

async function load(name: string, v: string) {
  vi.resetModules();
  vi.stubEnv(name, v);
  return import('../../src/services/StatsCollector.js');
}
afterEach(() => { vi.unstubAllEnvs(); vi.useRealTimers(); });

describe('interval env parsing', () => {
  it.each(['0x10', '1e3', '1.5', '2147483648', '99999999999', '-5', '0', 'abc', ' '])('rejects %j', async (v) => {
    const m = await load('STATS_COLLECT_INTERVAL_MS', v);
    expect(m.COLLECT_INTERVAL_MS).toBe(60_000);
    const a = await load('STATS_AUTOCLOSE_INTERVAL_MS', v);
    expect(a.AUTOCLOSE_INTERVAL_MS).toBe(600_000);
  });
  it.each([['5000', 5000], [' 30000 ', 30000], ['86400000', 86_400_000]])('accepts %j', async (v, n) => {
    expect((await load('STATS_COLLECT_INTERVAL_MS', v)).COLLECT_INTERVAL_MS).toBe(n);
    expect((await load('STATS_AUTOCLOSE_INTERVAL_MS', v)).AUTOCLOSE_INTERVAL_MS).toBe(n);
  });
  it('unset keeps defaults', async () => {
    vi.resetModules();
    const m = await import('../../src/services/StatsCollector.js');
    expect(m.COLLECT_INTERVAL_MS).toBe(60_000);
    expect(m.AUTOCLOSE_INTERVAL_MS).toBe(600_000);
  });
});

describe('inPlaceholderGrace clock skew (lead 3: bounded, not forever)', () => {
  it('a blockTime slightly in the future leaves grace once the clock passes it + grace', async () => {
    vi.resetModules();
    const m = await import('../../src/services/StatsCollector.js');
    const f = (m.StatsCollector.prototype as any).inPlaceholderGrace;
    vi.useFakeTimers(); vi.setSystemTime(1_000_000_000_000);
    const c = { creator: 'x', blockTime: 1_000_000_000 + 5 };
    expect(f.call({}, c)).toBe(true);
    vi.setSystemTime(1_000_000_000_000 + 5_000 + m.PLACEHOLDER_REGISTRATION_GRACE_MS + 1);
    expect(f.call({}, c)).toBe(false);
  });
});
