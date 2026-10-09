import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { existsSync, readFileSync, rmSync } from 'node:fs';
import { PublicKey } from '@solana/web3.js';

vi.hoisted(() => { process.env.INDEXER_BREAKER_ALERT_POLLS = '1'; }); // alert on the first held poll in tests
const mockGetSignaturesForAddress = vi.fn();
const mockGetParsedTransaction = vi.fn();
const mockGetAccountInfo = vi.fn(async () => null);
const mockGetParsedTransactions = vi.fn(async (signatures: string[]) =>
  Promise.all(signatures.map((sig) => mockGetParsedTransaction(sig))),
);

// These tests are about leg numbering / dedup, not about how a TradeCpi's executed size is proven
// (that is tests/matcher-fill-*.test.ts, against real transactions). Resolve every CPI leg to its
// wire size at a fixed price here.
vi.mock('../../src/parsers/matcherFill.js', async (orig) => ({
  ...(await orig<typeof import('../../src/parsers/matcherFill.js')>()),
  resolveCpiLeg: vi.fn(async (a: { wireSizeAbs: bigint }) => ({ kind: 'fill', sizeValue: a.wireSizeAbs, priceE6: 1_500_000n, exact: true })),
}));

vi.mock('@percolatorct/sdk', async (importOriginal) => ({
  // v2.2: the TradeIndexer now reaches the v2.2 log-event decoder, which reads the real SDK tag tables; tests override only what they stub.
  ...(await importOriginal<typeof import('@percolatorct/sdk')>()),
  // v17 IX_TAG: TradeCpiV2 (35) REMOVED; BatchTradeNoCpi (66) and BatchTradeCpi (67) added.
  IX_TAG: {
    TradeNoCpi: 10,
    TradeCpi: 11,
    // TradeCpiV2 deliberately omitted — deprecated in v17, tag 35 no longer in decoder.
    BatchTradeNoCpi: 66,
    BatchTradeCpi: 67,
  },
  // v17 desync additions — default to false/null so existing v12 test paths pass through.
  detectSlabLayout: vi.fn(() => null),
  isV17Account: vi.fn(() => false),
  parseWrapperConfigV17: vi.fn(),
  V17_HEADER_LEN: 16,
}));

// H2/H3: trades are now written via the indexer-local insertTradeRow helper
// (src/db/insertTradeRow.ts), not shared's insertTrade. Mock it directly.
// Resolves true = row written, false = duplicate leg (23505 swallowed).
vi.mock('../../src/db/insertTradeRow.js', () => ({ insertTradeRow: vi.fn(async () => true) }));

// #221: the fill-price reader. Default null = "not a readable v18 slot", which keeps
// every existing test on its legacy mark path; the #221 tests below set a price.
vi.mock('../../src/parsers/markPrice.js', () => ({ readAssetEffectivePriceE6: vi.fn(() => null) }));

vi.mock('@percolatorct/shared', () => ({
  config: {
    allProgramIds: ['FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD'],
  },
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
  getConnection: vi.fn(() => ({
    getSignaturesForAddress: mockGetSignaturesForAddress,
    getParsedTransaction: mockGetParsedTransaction,
    getParsedTransactions: mockGetParsedTransactions,
    getAccountInfo: mockGetAccountInfo,
  })),
  // The poll path must NOT call this (a transaction-level gate drops legs 2..N of a split
  // order); the split-order tests below back it with a fake DB and assert it is never asked.
  tradeExistsBySignature: vi.fn(async () => false),
  getMarkets: vi.fn(async () => []),
  eventBus: {
    on: vi.fn(),
    off: vi.fn(),
    publish: vi.fn(),
  },
  decodeBase58: vi.fn((str: string) => {
    // Simple mock: return a Buffer from the base64 or return bytes
    try {
      return Buffer.from(str, 'base64');
    } catch {
      return null;
    }
  }),
  readU128LE: vi.fn((bytes: Uint8Array) => {
    let value = 0n;
    for (let i = 15; i >= 0; i--) {
      value = (value << 8n) | BigInt(bytes[i]);
    }
    return value;
  }),
  parseTradeSize: vi.fn((sizeBytes: Uint8Array) => {
    const isNegative = sizeBytes[15] >= 128;
    return {
      sizeValue: isNegative ? 500_000n : 1_000_000n,
      side: isNegative ? 'short' as const : 'long' as const,
    };
  }),
  withRetry: vi.fn(async (fn: any) => fn()),
  addBreadcrumb: vi.fn(),
  captureException: vi.fn(),
}));

import { resetSkippedSignatureDedupe } from '../../src/lib/skippedSignatures.js';
import { TradeIndexerPolling } from '../../src/services/TradeIndexer.js';
import * as shared from '@percolatorct/shared';
import { insertTradeRow } from '../../src/db/insertTradeRow.js';
import { readAssetEffectivePriceE6 } from '../../src/parsers/markPrice.js';

const SLAB = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const PROGRAM_ID = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const TRADER = 'So11111111111111111111111111111111111111112';


const A = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const B = 'So11111111111111111111111111111111111111112';
const C = '11111111111111111111111111111111';

/** RPC-credit regression tests: the poller must not stack passes or poll slabs it cannot use. */
describe('TradeIndexerPolling RPC budget', () => {
  let indexer: TradeIndexerPolling;
  beforeEach(() => {
    vi.clearAllMocks();
    indexer = new TradeIndexerPolling();
    (indexer as any)._running = true;
  });
  afterEach(() => indexer.stop());

  it('polls only markets that are not indexer_excluded', async () => {
    vi.mocked(shared.getMarkets).mockResolvedValue([
      { slab_address: A }, { slab_address: B, indexer_excluded: true }, { slab_address: C, indexer_excluded: false },
    ] as any);
    mockGetSignaturesForAddress.mockResolvedValue([]);
    (indexer as any).hasBackfilled = true;
    await (indexer as any).pollAllMarkets();
    const polled = mockGetSignaturesForAddress.mock.calls.map((c) => (c[0] as PublicKey).toBase58());
    expect(polled.sort()).toEqual([A, C].sort());
  }, 10000);

  it('overlapping backfill() calls share ONE walk over the markets', async () => {
    vi.mocked(shared.getMarkets).mockResolvedValue([{ slab_address: A }] as any);
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    mockGetSignaturesForAddress.mockImplementation(async () => { await gate; return []; });
    const p1 = (indexer as any).backfill();
    const p2 = (indexer as any).backfill();
    const p3 = (indexer as any).backfill();
    release();
    await Promise.all([p1, p2, p3]);
    expect(mockGetSignaturesForAddress).toHaveBeenCalledTimes(1);
    // once complete, later calls are no-ops
    await (indexer as any).backfill();
    expect(mockGetSignaturesForAddress).toHaveBeenCalledTimes(1);
  }, 10000);

  it('a poll tick that fires while a pass is still running is skipped, not stacked', async () => {
    vi.mocked(shared.getMarkets).mockResolvedValue([{ slab_address: A }] as any);
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    mockGetSignaturesForAddress.mockImplementation(async () => { await gate; return []; });
    (indexer as any).hasBackfilled = true;
    const first = (indexer as any).pollAllMarkets();
    await new Promise((r) => setTimeout(r, 20));
    await (indexer as any).pollAllMarkets(); // overlapping tick returns immediately
    expect(mockGetSignaturesForAddress).toHaveBeenCalledTimes(1);
    release();
    await first;
    // a later tick runs normally
    mockGetSignaturesForAddress.mockImplementation(async () => []);
    await (indexer as any).pollAllMarkets();
    expect(mockGetSignaturesForAddress).toHaveBeenCalledTimes(2);
  }, 10000);
});
