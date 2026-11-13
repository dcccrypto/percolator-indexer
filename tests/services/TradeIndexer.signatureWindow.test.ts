import { describe, it, expect, vi, beforeEach } from 'vitest';

// Page cap for the runaway-walk test; the window tests stay under it (default page size 50).
vi.hoisted(() => { process.env.TRADE_MAX_SIGNATURE_PAGES = '4'; });

type Sig = { signature: string; err: unknown; slot: number };
let chain: Sig[] = []; // newest-first, like the RPC
let failPage: number | null = null; // 1-based call index that throws
const calls: { limit?: number; before?: string; until?: string }[] = [];

/** getSignaturesForAddress over `chain`: newest-first, `before` and `until` exclusive, at most `limit`. */
const getSignaturesForAddress = vi.fn(async (_pk: unknown, opts: { limit?: number; before?: string; until?: string } = {}) => {
  calls.push({ ...opts });
  if (failPage === calls.length) throw new Error('rpc down');
  let all = chain;
  if (opts.before) {
    const b = all.findIndex((s) => s.signature === opts.before);
    all = b >= 0 ? all.slice(b + 1) : [];
  }
  const cut = opts.until ? all.findIndex((s) => s.signature === opts.until) : -1;
  return (cut >= 0 ? all.slice(0, cut) : all).slice(0, opts.limit ?? 1000);
});
const tx = (slot: number) => ({ slot, blockTime: 0, meta: { err: null, logMessages: [] }, transaction: { message: { instructions: [], accountKeys: [] } } });
const getParsedTransactions = vi.fn(async (sigs: string[]) => sigs.map((s) => tx(Number(s.slice(3)))));

const captureException = vi.fn();
vi.mock('@percolatorct/shared', () => ({
  config: { allProgramIds: ['FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD'] },
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  getConnection: () => ({ getSignaturesForAddress, getParsedTransactions, getParsedTransaction: async () => null }),
  eventBus: { on: vi.fn(), off: vi.fn(), publish: vi.fn() },
  decodeBase58: () => null,
  withRetry: async (fn: () => Promise<unknown>) => fn(),
  captureException: (...a: unknown[]) => captureException(...a),
}));
vi.mock('../../src/db/insertTradeRow.js', () => ({ insertTradeRow: vi.fn(async () => true) }));
vi.mock('../../src/db/storedLegs.js', async (orig) => ({
  ...(await orig<typeof import('../../src/db/storedLegs.js')>()),
  fetchStoredLegs: async () => [],
  fetchStoredLegsMany: async () => new Map(),
}));
vi.mock('../../src/lib/skippedSignatures.js', () => ({ recordSkippedSignatures: async () => undefined }));

import { TradeIndexerPolling } from '../../src/services/TradeIndexer.js';

const SLAB = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
/** n+1 signatures, slot 0 (the cursor) .. slot n, newest-first. */
const makeChain = (n: number): Sig[] => Array.from({ length: n + 1 }, (_, i) => ({ signature: `sig${n - i}`, err: null, slot: n - i }));

describe('TradeIndexerPolling: a poll window larger than TRADE_MAX_SIGNATURES (50)', () => {
  let indexer: any;
  let processed: string[];
  beforeEach(() => {
    calls.length = 0;
    failPage = null;
    captureException.mockClear();
    indexer = new TradeIndexerPolling();
    indexer._running = true;
    processed = [];
    vi.spyOn(indexer, 'processTransaction').mockImplementation(async (...a: unknown[]) => { processed.push(a[1] as string); return false; });
  });

  it('pages back to the cursor: all 120 signatures are processed once, oldest-first, and the cursor moves to the newest', async () => {
    chain = makeChain(120);
    indexer.lastSignature.set(SLAB, 'sig0');
    await indexer.indexTradesForSlab(SLAB);
    expect(calls).toEqual([
      { limit: 50, until: 'sig0' },
      { limit: 50, before: 'sig71', until: 'sig0' },
      { limit: 50, before: 'sig21', until: 'sig0' },
    ]);
    expect(processed).toEqual(Array.from({ length: 120 }, (_, i) => `sig${i + 1}`));
    expect(indexer.lastSignature.get(SLAB)).toBe('sig120');
  });

  it('before the fix only the newest 50 were seen; the same window now matches one untruncated call', async () => {
    chain = makeChain(120);
    const full = (await getSignaturesForAddress(null, { limit: 1000, until: 'sig0' })).map((s) => s.signature).reverse();
    calls.length = 0;
    indexer.lastSignature.set(SLAB, 'sig0');
    await indexer.indexTradesForSlab(SLAB);
    expect(full).toHaveLength(120);
    expect(processed).toEqual(full);
  });

  it('a window of exactly one full page costs one extra (empty) call and nothing else', async () => {
    chain = makeChain(50);
    indexer.lastSignature.set(SLAB, 'sig0');
    await indexer.indexTradesForSlab(SLAB);
    expect(calls).toHaveLength(2);
    expect(processed).toHaveLength(50);
    expect(indexer.lastSignature.get(SLAB)).toBe('sig50');
  });

  it('errored signatures on a later page are still dropped', async () => {
    chain = makeChain(80);
    chain.find((s) => s.signature === 'sig3')!.err = { InstructionError: [0, 'Custom'] };
    indexer.lastSignature.set(SLAB, 'sig0');
    await indexer.indexTradesForSlab(SLAB);
    expect(processed).toHaveLength(79);
    expect(processed).not.toContain('sig3');
  });

  it('a page that fails processes nothing and holds the cursor; the next poll reads the whole window', async () => {
    chain = makeChain(120);
    indexer.lastSignature.set(SLAB, 'sig0');
    failPage = 2;
    await indexer.indexTradesForSlab(SLAB);
    expect(processed).toEqual([]);
    expect(indexer.lastSignature.get(SLAB)).toBe('sig0');
    calls.length = 0;
    failPage = null;
    await indexer.indexTradesForSlab(SLAB);
    expect(processed).toHaveLength(120);
    expect(indexer.lastSignature.get(SLAB)).toBe('sig120');
  });

  it('without a cursor (first pass) it is still one call of the newest maxSigs', async () => {
    chain = makeChain(120);
    await indexer.indexTradesForSlab(SLAB, 100);
    expect(calls).toEqual([{ limit: 100 }]);
    expect(processed).toHaveLength(100);
  });

  it('stops at TRADE_MAX_SIGNATURE_PAGES and reports it', async () => {
    chain = makeChain(300);
    indexer.lastSignature.set(SLAB, 'sig0');
    await indexer.indexTradesForSlab(SLAB);
    expect(calls).toHaveLength(4);
    expect(processed).toHaveLength(200);
    expect(captureException).toHaveBeenCalledTimes(1);
    expect(indexer.lastSignature.get(SLAB)).toBe('sig300');
  });
});
