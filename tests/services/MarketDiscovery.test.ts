import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock the v17 discovery helper so tests don't need a real RPC connection.
// Default: return empty array (no v17 markets) so tests fall through to discoverMarkets().
vi.mock('../../src/v17/discovery.js', () => ({
  discoverV17Markets: vi.fn(async () => []),
}));

// Mock external dependencies — include all SDK exports used by MarketDiscovery and discoverV17Markets.
vi.mock('@percolatorct/sdk', () => ({
  discoverMarkets: vi.fn(),
  getMarketsByAddress: vi.fn(),
  isV17Account: vi.fn(() => false),
}));

// Stable mock connection objects so tests can script and assert connection switching.
const mockPrimaryConnection = {
  getProgramAccounts: vi.fn(),
};

const mockFallbackConnection = {
  getProgramAccounts: vi.fn(),
};

vi.mock('@percolatorct/shared', () => ({
  config: {
    allProgramIds: ['11111111111111111111111111111111', 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'],
  },
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
  captureException: vi.fn(),
  getConnection: vi.fn(() => mockPrimaryConnection),
  getPrimaryConnection: vi.fn(() => mockPrimaryConnection),
  getFallbackConnection: vi.fn(() => mockFallbackConnection),
}));

import { MarketDiscovery } from '../../src/services/MarketDiscovery.js';
import * as core from '@percolatorct/sdk';
import * as v17disc from '../../src/v17/discovery.js';

describe('MarketDiscovery', () => {
  let marketDiscovery: MarketDiscovery;

  beforeEach(() => {
    vi.clearAllMocks();
    marketDiscovery = new MarketDiscovery();
  });

  afterEach(() => {
    marketDiscovery.stop();
  });

  describe('discover', () => {
    it('should call discoverMarkets for each program ID', async () => {
      const mockMarkets = [
        {
          slabAddress: { toBase58: () => 'Market111111111111111111111111111111111' },
          programId: { toBase58: () => '11111111111111111111111111111111' },
          config: {
            collateralMint: { toBase58: () => 'Mint1111111111111111111111111111111111' },
            oracleAuthority: { toBase58: () => 'Oracle11111111111111111111111111111111' },
          },
          params: {},
          header: {},
        },
      ];

      vi.mocked(core.discoverMarkets).mockResolvedValue(mockMarkets as any);

      const result = await marketDiscovery.discover();

      expect(core.discoverMarkets).toHaveBeenCalledTimes(2); // Two program IDs
      expect(result).toHaveLength(2); // One market from each program
    });

    it('should store discovered markets in map', async () => {
      const mockMarket1 = {
        slabAddress: { toBase58: () => 'Market211111111111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {
          collateralMint: { toBase58: () => 'Mint2111111111111111111111111111111111' },
        },
        params: {},
        header: {},
      };

      const mockMarket2 = {
        slabAddress: { toBase58: () => 'Market311111111111111111111111111111111' },
        programId: { toBase58: () => 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' },
        config: {
          collateralMint: { toBase58: () => 'Mint3111111111111111111111111111111111' },
        },
        params: {},
        header: {},
      };

      vi.mocked(core.discoverMarkets)
        .mockResolvedValueOnce([mockMarket1] as any)
        .mockResolvedValueOnce([mockMarket2] as any);

      await marketDiscovery.discover();

      const markets = marketDiscovery.getMarkets();
      expect(markets.size).toBe(2);
      expect(markets.has('Market211111111111111111111111111111111')).toBe(true);
      expect(markets.has('Market311111111111111111111111111111111')).toBe(true);
    });

    it('should handle errors per program without crashing all', async () => {
      const mockMarket = {
        slabAddress: { toBase58: () => 'Market411111111111111111111111111111111' },
        programId: { toBase58: () => 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' },
        config: {},
        params: {},
        header: {},
      };

      vi.mocked(core.discoverMarkets)
        .mockRejectedValueOnce(new Error('Program 1 failed'))
        .mockResolvedValueOnce([mockMarket] as any);

      const result = await marketDiscovery.discover();

      // Should still discover from the second program
      expect(result).toHaveLength(1);
      expect(marketDiscovery.getMarkets().size).toBe(1);
    });

    it('should preserve stale markets when all programs fail', async () => {
      const mockMarket = {
        slabAddress: { toBase58: () => 'MarketStale1111111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      };

      // First call succeeds
      vi.mocked(core.discoverMarkets)
        .mockResolvedValueOnce([mockMarket] as any)
        .mockResolvedValueOnce([]);

      await marketDiscovery.discover();
      expect(marketDiscovery.getMarkets().size).toBe(1);

      // Second call: all programs fail (RPC down)
      vi.mocked(core.discoverMarkets)
        .mockRejectedValueOnce(new Error('502 Bad Gateway'))
        .mockRejectedValueOnce(new Error('502 Bad Gateway'));

      const result = await marketDiscovery.discover();

      // Should return empty but preserve stale markets in map
      expect(result).toHaveLength(0);
      expect(marketDiscovery.getMarkets().size).toBe(1); // stale preserved
    }, 15000);

    it('should add delay between program discoveries', async () => {
      vi.mocked(core.discoverMarkets).mockResolvedValue([]);

      const startTime = Date.now();
      await marketDiscovery.discover();
      const endTime = Date.now();

      // Should take at least 2 seconds (delay between 2 programs)
      expect(endTime - startTime).toBeGreaterThanOrEqual(2000);
    });

    it('should return empty array when no markets found', async () => {
      vi.mocked(core.discoverMarkets).mockResolvedValue([]);

      const result = await marketDiscovery.discover();

      expect(result).toHaveLength(0);
      expect(marketDiscovery.getMarkets().size).toBe(0);
    });

    it('should fall back to fallbackConn after exhausting all primary 429 retries', async () => {
      const { getPrimaryConnection, getFallbackConnection } = await import('@percolatorct/shared');
      const primaryConn = getPrimaryConnection();
      const fallbackConn = getFallbackConnection();

      const mockMarket = {
        slabAddress: { toBase58: () => 'MarketFallback11111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      };

      // Program 1: exhaust all primary retries (4) with 429, then succeed on fallback.
      // Program 2: succeed immediately on primary.
      let program1Calls = 0;
      vi.mocked(core.discoverMarkets).mockImplementation(async (conn, _programId) => {
        if (conn === primaryConn) {
          program1Calls++;
          // Fail with 429 until all retries are exhausted (HELIUS_429_BACKOFF_MS has 4 entries)
          if (program1Calls <= 4) {
            throw new Error('429 Too Many Requests');
          }
          // Should not reach here — fallback takes over
          return [];
        }
        // Called with fallbackConn — succeed
        return [mockMarket] as any;
      });

      const result = await marketDiscovery.discover();

      // Fallback connection must have been used
      expect(getPrimaryConnection).toHaveBeenCalled();
      expect(getFallbackConnection).toHaveBeenCalled();
      // Market discovered via fallback should be in the results
      expect(result.some(m => m.slabAddress.toBase58() === 'MarketFallback11111111111111111111111111')).toBe(true);
    }, 120_000); // generous timeout — 4 backoff delays sum to ~52 s with max jitter

    it('should update existing markets on rediscovery', async () => {
      const marketAddress = 'Market511111111111111111111111111111111';
      
      const mockMarketV1 = {
        slabAddress: { toBase58: () => marketAddress },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: { version: 1 },
        params: {},
        header: {},
      };

      const mockMarketV2 = {
        slabAddress: { toBase58: () => marketAddress },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: { version: 2 },
        params: {},
        header: {},
      };

      // First discover(): program1 returns v1, program2 returns nothing
      // Second discover(): program1 returns v2, program2 returns nothing
      vi.mocked(core.discoverMarkets)
        .mockResolvedValueOnce([mockMarketV1] as any) // discover #1, program 1
        .mockResolvedValueOnce([]) // discover #1, program 2
        .mockResolvedValueOnce([mockMarketV2] as any) // discover #2, program 1
        .mockResolvedValueOnce([]); // discover #2, program 2

      await marketDiscovery.discover();
      expect(marketDiscovery.getMarkets().size).toBe(1);

      // Rediscover with updated market
      await marketDiscovery.discover();
      const markets = marketDiscovery.getMarkets();
      expect(markets.size).toBe(1);
      // The market object should be the latest one stored
      const stored = markets.get(marketAddress);
      expect(stored).toBeDefined();
    }, 15000);
  });

  describe('getMarkets', () => {
    it('should return current markets map', async () => {
      const mockMarket = {
        slabAddress: { toBase58: () => 'Market611111111111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      };

      vi.mocked(core.discoverMarkets).mockResolvedValue([mockMarket] as any);

      await marketDiscovery.discover();

      const markets = marketDiscovery.getMarkets();
      expect(markets).toBeInstanceOf(Map);
      expect(markets.size).toBe(1);
    });

    it('should return empty map initially', () => {
      const markets = marketDiscovery.getMarkets();
      expect(markets.size).toBe(0);
    });
  });

  describe('start and stop', () => {
    it('should start timer and perform initial discovery', async () => {
      const mockMarket = {
        slabAddress: { toBase58: () => 'MarketStart11111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      };
      vi.mocked(core.discoverMarkets).mockResolvedValue([mockMarket] as any);

      await marketDiscovery.start(60_000);

      expect(core.discoverMarkets).toHaveBeenCalled();
      expect(marketDiscovery.getMarkets().size).toBe(1);
    });

    it('should set up periodic timer after initial discovery', async () => {
      const mockMarket = {
        slabAddress: { toBase58: () => 'MarketPeriodic111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      };
      vi.mocked(core.discoverMarkets).mockResolvedValue([mockMarket] as any);

      await marketDiscovery.start(3000);

      const callsAfterStart = vi.mocked(core.discoverMarkets).mock.calls.length;
      expect(callsAfterStart).toBeGreaterThanOrEqual(2); // 2 programs

      // Wait for one periodic cycle
      await new Promise(resolve => setTimeout(resolve, 8000));

      expect(vi.mocked(core.discoverMarkets).mock.calls.length).toBeGreaterThan(callsAfterStart);
    }, 20000);

    it('should stop timer', async () => {
      const mockMarket = {
        slabAddress: { toBase58: () => 'MarketStop1111111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      };
      vi.mocked(core.discoverMarkets).mockResolvedValue([mockMarket] as any);

      await marketDiscovery.start(60_000);

      const callsBefore = vi.mocked(core.discoverMarkets).mock.calls.length;

      marketDiscovery.stop();

      // Wait a bit and verify no more calls
      await new Promise(resolve => setTimeout(resolve, 200));
      const callsAfter = vi.mocked(core.discoverMarkets).mock.calls.length;

      expect(callsAfter).toBe(callsBefore);
    });

    it('should retry on initial failure and eventually start periodic timer', async () => {
      const mockMarket = {
        slabAddress: { toBase58: () => 'MarketRetry1111111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      };

      let callCount = 0;
      vi.mocked(core.discoverMarkets).mockImplementation(async () => {
        callCount++;
        // First 2 calls (attempt 1, both programs) fail, next 2 succeed
        if (callCount <= 2) {
          throw new Error('RPC down');
        }
        return [mockMarket] as any;
      });

      await marketDiscovery.start(60_000);

      // Should have retried and eventually succeeded
      expect(callCount).toBeGreaterThanOrEqual(4);
      expect(marketDiscovery.getMarkets().size).toBe(1);
    }, 30000);

    it('should not crash if stop is called before start', () => {
      expect(() => marketDiscovery.stop()).not.toThrow();
    });

    it('should not crash if stop is called multiple times', async () => {
      const mockMarket = {
        slabAddress: { toBase58: () => 'MarketMultiStop111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      };
      vi.mocked(core.discoverMarkets).mockResolvedValue([mockMarket] as any);

      await marketDiscovery.start(60_000);
      marketDiscovery.stop();
      
      expect(() => marketDiscovery.stop()).not.toThrow();
    });
  });

  describe('edge cases', () => {
    it('should handle markets with duplicate addresses', async () => {
      const mockMarket = {
        slabAddress: { toBase58: () => 'MarketDup111111111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: { version: 1 },
        params: {},
        header: {},
      };

      // Return same market from both programs
      vi.mocked(core.discoverMarkets).mockResolvedValue([mockMarket] as any);

      await marketDiscovery.discover();

      const markets = marketDiscovery.getMarkets();
      // Should only store once (last one wins)
      expect(markets.size).toBe(1);
    });

    it('should handle very large number of markets', async () => {
      const manyMarkets = Array.from({ length: 1000 }, (_, i) => ({
        slabAddress: { toBase58: () => `Market${i}11111111111111111111111111111` },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      }));

      vi.mocked(core.discoverMarkets)
        .mockResolvedValueOnce(manyMarkets as any)
        .mockResolvedValueOnce([]);

      await marketDiscovery.discover();

      const markets = marketDiscovery.getMarkets();
      expect(markets.size).toBe(1000);
    });
  });
});

// ---------------------------------------------------------------------------
// #145 focused regression tests — both scanners always run and results merge
// ---------------------------------------------------------------------------

describe('#145 — MarketDiscovery: both v17 and v12 scanners always run per program', () => {
  let marketDiscovery: MarketDiscovery;

  beforeEach(() => {
    vi.clearAllMocks();
    marketDiscovery = new MarketDiscovery();
  });

  afterEach(() => {
    marketDiscovery.stop();
  });

  it('normal path: v12 discoverMarkets still runs after v17 finds markets', async () => {
    // #145 regression: the original code broke out of the retry loop after the first
    // v17 hit, permanently skipping discoverMarkets (v12) for that program.
    const v17Market = {
      slabAddress: { toBase58: () => 'V17MarketAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' },
      programId: { toBase58: () => '11111111111111111111111111111111' },
      config: {}, params: {}, header: {},
    };
    const v12Market = {
      slabAddress: { toBase58: () => 'V12MarketBBBBBBBBBBBBBBBBBBBBBBBBBBBB' },
      programId: { toBase58: () => '11111111111111111111111111111111' },
      config: {}, params: {}, header: {},
    };

    // Program 1: v17 returns a market AND v12 also returns a market.
    // Program 2: both return nothing.
    vi.mocked(v17disc.discoverV17Markets).mockResolvedValue([v17Market] as any);
    vi.mocked(core.discoverMarkets)
      .mockResolvedValueOnce([v12Market] as any) // program 1
      .mockResolvedValueOnce([]);                 // program 2

    const result = await marketDiscovery.discover();

    // Both scanners must have produced results that were merged.
    expect(core.discoverMarkets).toHaveBeenCalled();
    expect(result.some(m => m.slabAddress.toBase58() === 'V17MarketAAAAAAAAAAAAAAAAAAAAAAAAAAAAA')).toBe(true);
    expect(result.some(m => m.slabAddress.toBase58() === 'V12MarketBBBBBBBBBBBBBBBBBBBBBBBBBBBB')).toBe(true);
    expect(marketDiscovery.getMarkets().size).toBe(4); // 2 unique slabs × 2 programs each
  }, 15_000);

  it('MARKETS_FILTER path: v12 getMarketsByAddress still runs after v17 finds markets', async () => {
    // #145 regression: the original MARKETS_FILTER code `continue`d to the next program
    // after finding v17 markets, permanently skipping getMarketsByAddress (v12).
    const slabV17 = 'V17FilterAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA';
    const slabV12 = 'V12FilterBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB';

    const v17Market = {
      slabAddress: { toBase58: () => slabV17 },
      programId: { toBase58: () => '11111111111111111111111111111111' },
      config: {}, params: {}, header: {},
    };
    const v12Market = {
      slabAddress: { toBase58: () => slabV12 },
      programId: { toBase58: () => '11111111111111111111111111111111' },
      config: {}, params: {}, header: {},
    };

    vi.mocked(v17disc.discoverV17Markets).mockResolvedValue([v17Market] as any);
    vi.mocked(core.getMarketsByAddress).mockResolvedValue([v12Market] as any);

    // Provide two slab addresses in MARKETS_FILTER env so the filter branch is taken.
    const origFilter = process.env.MARKETS_FILTER;
    process.env.MARKETS_FILTER = `${slabV17},${slabV12}`;
    try {
      const result = await marketDiscovery.discover();
      // Both scanners must have run and merged results.
      expect(core.getMarketsByAddress).toHaveBeenCalled();
      const addrs = result.map(m => m.slabAddress.toBase58());
      expect(addrs).toContain(slabV17);
      expect(addrs).toContain(slabV12);
    } finally {
      process.env.MARKETS_FILTER = origFilter;
    }
  });
});
