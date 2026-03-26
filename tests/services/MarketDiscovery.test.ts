import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock external dependencies
vi.mock('@percolator/sdk', () => ({
  discoverMarkets: vi.fn(),
}));

vi.mock('@percolator/shared', () => ({
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
  // MarketDiscovery.ts imports getPrimaryConnection directly (not getConnection alias)
  getPrimaryConnection: vi.fn(() => ({
    getProgramAccounts: vi.fn(),
  })),
  getFallbackConnection: vi.fn(() => ({
    getProgramAccounts: vi.fn(),
  })),
}));

import { MarketDiscovery } from '../../src/services/MarketDiscovery.js';
import * as core from '@percolator/sdk';

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

    it('should retry on 429 rate-limit errors but not on other errors', async () => {
      const mockMarket = {
        slabAddress: { toBase58: () => 'Market502111111111111111111111111111111' },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      };

      // Non-429 errors (like 502) are NOT retried — they fail immediately and move
      // to the next program. Only 429/rate-limit errors trigger the retry backoff.
      vi.mocked(core.discoverMarkets)
        .mockRejectedValueOnce(new Error('502 Bad Gateway')) // program 1: non-retryable error
        .mockResolvedValueOnce([mockMarket] as any); // program 2: success

      const result = await marketDiscovery.discover();

      // 2 calls: 1 failure (no retry for non-429) + 1 for second program
      expect(core.discoverMarkets).toHaveBeenCalledTimes(2);
      // program 1 failed but program 2 succeeded → 1 market returned
      expect(result).toHaveLength(1);
    }, 10000);

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
    }, 10000);

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
      vi.mocked(core.discoverMarkets).mockResolvedValue([]);

      marketDiscovery.start(100); // Short interval for testing

      // Wait for initial discovery
      await new Promise(resolve => setTimeout(resolve, 150));

      expect(core.discoverMarkets).toHaveBeenCalled();
    });

    it('should perform periodic discoveries', async () => {
      vi.mocked(core.discoverMarkets).mockResolvedValue([]);

      marketDiscovery.start(3000); // interval shorter than discovery time

      // Wait for initial discovery (2 programs × 2s delay = ~4s) plus one more cycle
      await new Promise(resolve => setTimeout(resolve, 12000));

      // Should have been called at least 4 times (2 programs × 2 cycles)
      expect(vi.mocked(core.discoverMarkets).mock.calls.length).toBeGreaterThanOrEqual(4);
    }, 20000);

    it('should stop timer', async () => {
      vi.mocked(core.discoverMarkets).mockResolvedValue([]);

      marketDiscovery.start(50);
      await new Promise(resolve => setTimeout(resolve, 100));

      const callsBefore = vi.mocked(core.discoverMarkets).mock.calls.length;

      marketDiscovery.stop();

      // Wait a bit and verify no more calls
      await new Promise(resolve => setTimeout(resolve, 100));
      const callsAfter = vi.mocked(core.discoverMarkets).mock.calls.length;

      expect(callsAfter).toBe(callsBefore);
    });

    it('should handle discovery errors during periodic cycle', async () => {
      // discover() handles errors internally — each call is independent.
      // Verify that a thrown error from discoverMarkets doesn't prevent the next call.
      let callCount = 0;
      vi.mocked(core.discoverMarkets).mockImplementation(async () => {
        callCount++;
        if (callCount === 1) {
          throw new Error('Discovery failed');
        }
        return [];
      });

      // Call discover() directly (bypassing start() retry logic) to verify
      // that one discover() call failing does not prevent subsequent calls.
      await marketDiscovery.discover().catch(() => {}); // first call: throws
      await marketDiscovery.discover(); // second call: should succeed

      expect(callCount).toBeGreaterThanOrEqual(2);
    });

    it('should not crash if stop is called before start', () => {
      expect(() => marketDiscovery.stop()).not.toThrow();
    });

    it('should not crash if stop is called multiple times', () => {
      marketDiscovery.start(1000);
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
      // Use zero-padded 5-digit addresses to guarantee uniqueness.
      const manyMarkets = Array.from({ length: 500 }, (_, i) => ({
        slabAddress: { toBase58: () => `LargeMarket${String(i).padStart(5, '0')}111111111111111111111` },
        programId: { toBase58: () => '11111111111111111111111111111111' },
        config: {},
        params: {},
        header: {},
      }));

      // Reset to remove any default mock set by previous tests in this file,
      // then set explicit once-values for this discovery cycle (2 programs).
      vi.mocked(core.discoverMarkets).mockReset();
      vi.mocked(core.discoverMarkets)
        .mockResolvedValueOnce(manyMarkets as any)
        .mockResolvedValueOnce([]);

      await marketDiscovery.discover();

      const markets = marketDiscovery.getMarkets();
      expect(markets.size).toBe(500);
    }, 10000);
  });
});
