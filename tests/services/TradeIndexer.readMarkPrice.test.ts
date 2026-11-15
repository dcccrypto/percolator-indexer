import { describe, it, expect, vi, beforeEach } from 'vitest';

// readMarkPriceFromSlab is the price source of every fill the matcher CPI does not price (TradeNoCpi, tag 44,
// batch legs without evidence). A wrong value here is written permanently, so each branch is pinned directly.
const mocks = vi.hoisted(() => ({
  effective: vi.fn<(d: Uint8Array, a: number) => number | null>(() => null),
  magic: vi.fn<(d: Uint8Array) => boolean>(() => false),
  ewma: vi.fn<(d: Uint8Array, p: string) => bigint>(() => 0n),
  report: vi.fn(),
  layout: vi.fn<(n: number) => { engineOff: number; engineMarkPriceOff: number } | null>(() => null),
  warn: vi.fn(),
}));

vi.mock('../../src/parsers/markPrice.js', () => ({ readAssetEffectivePriceE6: mocks.effective }));
vi.mock('../../src/layout/resolve.js', () => ({
  hasWrapperMagic: mocks.magic,
  readMarkEwmaE6: mocks.ewma,
  reportUnknownLayout: mocks.report,
}));
vi.mock('@percolatorct/sdk', async (orig) => ({
  ...(await orig<typeof import('@percolatorct/sdk')>()),
  detectSlabLayout: mocks.layout,
}));
vi.mock('../../src/db/insertTradeRow.js', () => ({ insertTradeRow: vi.fn(), insertTradeRows: vi.fn(), tradeKey: vi.fn() }));
vi.mock('../../src/db/marketRows.js', () => ({ getMarketRowsCached: vi.fn(async () => []) }));
vi.mock('../../src/db/tradeCursor.js', () => ({ loadTradeCursors: vi.fn(async () => new Map()), saveTradeCursor: vi.fn() }));
vi.mock('../../src/services/v22LogEvents.js', () => ({ recordV22LogEvents: vi.fn() }));
vi.mock('../../src/lib/skippedSignatures.js', () => ({ recordSkippedSignatures: vi.fn() }));
vi.mock('@percolatorct/shared', () => ({
  config: { allProgramIds: [] },
  createLogger: () => ({ info: vi.fn(), warn: mocks.warn, error: vi.fn(), debug: vi.fn() }),
  getConnection: vi.fn(),
  eventBus: { on: vi.fn(), off: vi.fn(), publish: vi.fn() },
  decodeBase58: vi.fn(),
  withRetry: vi.fn(async (fn: () => unknown) => fn()),
  captureException: vi.fn(),
}));

import { TradeIndexerPolling } from '../../src/services/TradeIndexer.js';

const SLAB = '11111111111111111111111111111111';
const read = (conn: unknown, asset = 0): Promise<number> =>
  (new TradeIndexerPolling() as unknown as { readMarkPriceFromSlab(c: unknown, s: string, a: number): Promise<number> })
    .readMarkPriceFromSlab(conn, SLAB, asset);
const conn = (info: unknown) => ({ getAccountInfo: vi.fn(async () => info) });
const buf = (n: number) => Buffer.alloc(n);

describe('TradeIndexer.readMarkPriceFromSlab', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.effective.mockReturnValue(null);
    mocks.magic.mockReturnValue(false);
    mocks.layout.mockReturnValue(null);
  });

  it('returns 0 when the account is missing or has no data', async () => {
    expect(await read(conn(null))).toBe(0);
    expect(await read(conn({}))).toBe(0);
  });

  it('prefers the asset effective price (e6 -> USD) and passes the asset index', async () => {
    mocks.effective.mockReturnValue(84_250_000);
    expect(await read(conn({ data: buf(64) }), 3)).toBe(84.25);
    expect(mocks.effective.mock.calls[0][1]).toBe(3);
  });

  it('v17 wrapper: uses the mark EWMA when it is in range', async () => {
    mocks.magic.mockReturnValue(true);
    mocks.ewma.mockReturnValue(1_500_000n);
    expect(await read(conn({ data: buf(64) }))).toBe(1.5);
  });

  it.each([0n, 1_000_000_000_000n])('v17 wrapper: an out-of-range EWMA (%s) yields 0, never a guess', async (v) => {
    mocks.magic.mockReturnValue(true);
    mocks.ewma.mockReturnValue(v);
    expect(await read(conn({ data: buf(64) }))).toBe(0);
  });

  it('v17 wrapper: an unknown layout version is reported and yields 0', async () => {
    mocks.magic.mockReturnValue(true);
    const err = new Error('unknown version');
    mocks.ewma.mockImplementation(() => { throw err; });
    expect(await read(conn({ data: buf(64) }))).toBe(0);
    expect(mocks.report).toHaveBeenCalledWith(SLAB, err, 'TradeIndexer.readMarkPriceFromSlab');
  });

  it('v12: no detected layout, or a V0 layout without a mark price field, yields 0', async () => {
    expect(await read(conn({ data: buf(64) }))).toBe(0);
    mocks.layout.mockReturnValue({ engineOff: 480, engineMarkPriceOff: -1 });
    expect(await read(conn({ data: buf(64) }))).toBe(0);
  });

  it('v12: a slab too short to hold the mark price field yields 0', async () => {
    mocks.layout.mockReturnValue({ engineOff: 10, engineMarkPriceOff: 20 });
    expect(await read(conn({ data: buf(37) }))).toBe(0); // off 30, needs 38
  });

  it('v12 V1: reads the u64 mark price at engineOff + engineMarkPriceOff', async () => {
    mocks.layout.mockReturnValue({ engineOff: 8, engineMarkPriceOff: 8 });
    const data = buf(32);
    data.writeBigUInt64LE(2_000_000n, 16);
    expect(await read(conn({ data }))).toBe(2);
  });

  it.each([0n, 1_000_000_000_000n])('v12 V1: an out-of-range mark price (%s) yields 0', async (v) => {
    mocks.layout.mockReturnValue({ engineOff: 8, engineMarkPriceOff: 8 });
    const data = buf(32);
    data.writeBigUInt64LE(v, 16);
    expect(await read(conn({ data }))).toBe(0);
  });

  it('an RPC failure is logged and yields 0, not a thrown error', async () => {
    const failing = { getAccountInfo: vi.fn(async () => { throw new Error('rpc down'); }) };
    expect(await read(failing)).toBe(0);
    expect(mocks.warn).toHaveBeenCalledWith('Failed to read mark price from slab', expect.objectContaining({ error: 'rpc down' }));
    const nonError = { getAccountInfo: vi.fn(async () => { throw 'boom'; }) };
    expect(await read(nonError)).toBe(0);
  });
});
