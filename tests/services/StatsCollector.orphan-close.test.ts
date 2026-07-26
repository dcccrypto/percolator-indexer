import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock external dependencies (mirrors StatsCollector.test.ts)
const mockGetAccountInfo = vi.fn();
const mockGetMultipleAccountsInfo = vi.fn();

const mockUpdate = vi.fn();
const mockFrom = vi.fn();

vi.mock('@percolatorct/sdk', () => ({
  parseEngine: vi.fn(),
  parseConfig: vi.fn(),
  parseParams: vi.fn(),
  parseAllAccounts: vi.fn(() => []),
  isV17Account: vi.fn(() => false),
  parseWrapperConfigV17: vi.fn(),
  parseAssetOracleProfileV17: vi.fn(),
  V17_HEADER_LEN: 16,
  V17_MARKET_GROUP_OFF: 448,
  V17_ASSET_ORACLE_PROFILE_LEN: 400,
  detectDexType: vi.fn(() => null),
  parseDexPool: vi.fn(),
}));

vi.mock('@percolatorct/shared', () => ({
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
  getConnection: vi.fn(() => ({
    getAccountInfo: mockGetAccountInfo,
    getMultipleAccountsInfo: mockGetMultipleAccountsInfo,
    getParsedAccountInfo: vi.fn().mockResolvedValue({ value: null }),
    rpcEndpoint: 'https://api.devnet.solana.com',
  })),
  upsertMarketStats: vi.fn(),
  insertOraclePrice: vi.fn(),
  get24hVolume: vi.fn(async () => ({ volume: '0', tradeCount: 0 })),
  getMarkets: vi.fn(async () => []),
  insertMarket: vi.fn(),
  getSupabase: vi.fn(() => ({ from: mockFrom })),
  getNetwork: vi.fn(() => 'devnet'),
  withRetry: vi.fn(async (fn: any) => fn()),
  addBreadcrumb: vi.fn(),
  captureException: vi.fn(),
}));

import { StatsCollector } from '../../src/services/StatsCollector.js';
import type { MarketProvider } from '../../src/services/StatsCollector.js';
import * as shared from '@percolatorct/shared';

const SLAB1 = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';

/** Records every `.update({...})` payload written to the `markets` table. */
function installSupabaseSpy(): { updates: Record<string, unknown>[] } {
  const updates: Record<string, unknown>[] = [];
  mockFrom.mockImplementation((table: string) => {
    if (table !== 'markets') {
      return { insert: vi.fn().mockResolvedValue({ error: null }) };
    }
    return {
      update: (payload: Record<string, unknown>) => {
        updates.push(payload);
        mockUpdate(payload);
        // .eq(...).eq(...) chain, awaited by the caller
        const chain: any = { eq: () => chain, then: (r: any) => r({ error: null }) };
        return chain;
      },
      insert: vi.fn().mockResolvedValue({ error: null }),
    };
  });
  return { updates };
}

function makeDbMarket(slab: string) {
  return {
    slab_address: slab,
    network: 'devnet',
    indexer_excluded: false,
    status: 'active',
  };
}

/** Only the writes that close + exclude a market. */
const closures = (updates: Record<string, unknown>[]) =>
  updates.filter((u) => u.status === 'closed' && u.indexer_excluded === true);

describe('StatsCollector — orphan market auto-close (GH#158)', () => {
  let statsCollector: StatsCollector;
  let mockMarketProvider: MarketProvider;

  beforeEach(() => {
    vi.clearAllMocks();
    // Orphan = present in DB, absent from live discovery.
    mockMarketProvider = { getMarkets: vi.fn(() => new Map()) };
    statsCollector = new StatsCollector(mockMarketProvider);
    (statsCollector as any)._running = true;
    vi.mocked(shared.getMarkets).mockResolvedValue([makeDbMarket(SLAB1)] as any);
  });

  afterEach(() => {
    statsCollector.stop();
  });

  const runSync = () => (statsCollector as any).syncStatsForOrphanDBMarkets();

  it('does not close a market after a single null account response', async () => {
    const { updates } = installSupabaseSpy();
    mockGetMultipleAccountsInfo.mockResolvedValue([null]);

    await runSync();

    expect(closures(updates)).toHaveLength(0);
  });

  it('does not close a market after two consecutive null responses', async () => {
    const { updates } = installSupabaseSpy();
    mockGetMultipleAccountsInfo.mockResolvedValue([null]);

    await runSync();
    await runSync();

    expect(closures(updates)).toHaveLength(0);
  });

  it('closes the market once the miss threshold is reached', async () => {
    const { updates } = installSupabaseSpy();
    mockGetMultipleAccountsInfo.mockResolvedValue([null]);

    await runSync();
    await runSync();
    await runSync();

    expect(closures(updates)).toHaveLength(1);
  });

  it('resets the miss counter when the account reappears', async () => {
    const { updates } = installSupabaseSpy();

    // Two transient misses...
    mockGetMultipleAccountsInfo.mockResolvedValue([null]);
    await runSync();
    await runSync();

    // ...then the RPC returns the account again. Layout is unparseable here,
    // which makes the sync skip the market — but the account plainly exists,
    // so the miss streak must be cleared.
    mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
    await runSync();

    // A single later miss must not tip it over the threshold.
    mockGetMultipleAccountsInfo.mockResolvedValue([null]);
    await runSync();

    expect(closures(updates)).toHaveLength(0);
  });

  it('does not grow the miss map without bound', async () => {
    const { updates } = installSupabaseSpy();
    void updates;
    mockGetMultipleAccountsInfo.mockImplementation(async (keys: unknown[]) =>
      keys.map(() => null),
    );

    // Feed far more distinct orphans than the cache is allowed to hold.
    const many = Array.from({ length: 600 }, (_, i) =>
      makeDbMarket(`${SLAB1.slice(0, 32)}${String(i).padStart(12, '0')}`),
    );
    vi.mocked(shared.getMarkets).mockResolvedValue(many as any);

    await runSync();

    const missMap = (statsCollector as any).orphanMissCounts as Map<string, number>;
    expect(missMap.size).toBeLessThanOrEqual(
      (StatsCollector as any).MAX_ORPHAN_MISS_ENTRIES,
    );
  });
});
