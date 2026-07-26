/**
 * Tests for AdlIndexerPolling — M7: v17 has no standalone ADL instruction, so
 * ADL_TAGS is empty and no signature can ever match. start() must short-circuit
 * (no poll timer, no startup backfill) so we don't burn RPC credits fetching
 * signatures/transactions only to discard 100% of them.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// vi.hoisted so these exist before the hoisted vi.mock factories AND before the
// ESM import of AdlIndexer (which calls createLogger() at module-load time).
const { mockGetSignaturesForAddress, mockGetParsedTransactions, mockLoggerInfo } = vi.hoisted(() => ({
  mockGetSignaturesForAddress: vi.fn().mockResolvedValue([]),
  mockGetParsedTransactions: vi.fn().mockResolvedValue([]),
  mockLoggerInfo: vi.fn(),
}));

vi.mock('@percolatorct/sdk', () => ({
  // ExecuteAdl aliases exist but are NOT added to ADL_TAGS in v17 (intentionally empty).
  IX_TAG: { ExecuteAdl: 101, ExecuteAdl_v12: 50 },
}));

vi.mock('@percolatorct/shared', () => ({
  config: { allProgramIds: ['FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD'] },
  createLogger: vi.fn(() => ({
    info: mockLoggerInfo,
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
  getConnection: vi.fn(() => ({
    getSignaturesForAddress: mockGetSignaturesForAddress,
    getParsedTransactions: mockGetParsedTransactions,
    getAccountInfo: vi.fn().mockResolvedValue(null),
  })),
  getSupabase: vi.fn(() => ({ from: vi.fn(() => ({ upsert: vi.fn().mockResolvedValue({ error: null }) })) })),
  getMarkets: vi.fn(async () => [{ slab_address: 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD' }]),
  getNetwork: vi.fn(() => 'devnet'),
  withRetry: vi.fn(async (fn: any) => fn()),
  captureException: vi.fn(),
  decodeBase58: vi.fn(() => new Uint8Array(0)),
}));

import { AdlIndexerPolling } from '../../src/services/AdlIndexer.js';

describe('AdlIndexerPolling — M7 empty-tag guard', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('does not arm the poller or issue any RPC when ADL_TAGS is empty', async () => {
    const indexer = new AdlIndexerPolling();
    indexer.start();

    // Fire the 5s startup-backfill timeout and multiple 5-min poll intervals.
    await vi.advanceTimersByTimeAsync(5_000);
    await vi.advanceTimersByTimeAsync(15 * 60_000);

    expect(mockGetSignaturesForAddress).not.toHaveBeenCalled();
    expect(mockGetParsedTransactions).not.toHaveBeenCalled();
  });

  it('logs an explanatory skip message', () => {
    const indexer = new AdlIndexerPolling();
    indexer.start();
    const logged = mockLoggerInfo.mock.calls.map((c) => String(c[0])).join('\n');
    expect(logged).toMatch(/ADL_TAGS is empty/i);
  });

  it('start() is idempotent and still issues no RPC on repeated calls', async () => {
    const indexer = new AdlIndexerPolling();
    indexer.start();
    indexer.start();
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(mockGetSignaturesForAddress).not.toHaveBeenCalled();
  });
});
