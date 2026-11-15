/** TradeIndexerPolling.processTransaction: fill pricing, fees, leg numbering and the skip/validation branches (real SDK tags). */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { PublicKey } from '@solana/web3.js';
import { IX_TAG } from '@percolatorct/sdk';
import { encodeBase58 } from '../../src/lib/base58.js';

const PROGRAM = 'ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB';
const SLAB = 'So11111111111111111111111111111111111111112';
const OTHER = 'SysvarC1ock11111111111111111111111111111111';
const TRADER = 'Vote111111111111111111111111111111111111111';
const SIG = '5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW';

const m = vi.hoisted(() => ({
  insertTradeRow: vi.fn(async (_r?: unknown) => true),
  resolveCpiLeg: vi.fn(),
  effective: vi.fn((_d: Uint8Array, _a: number) => 2_000_000 as number | null),
  skipped: vi.fn(async (_x?: unknown) => undefined),
  publish: vi.fn(),
  stored: { rows: [] as unknown[] | null },
}));

vi.mock('@percolatorct/shared', async (orig) => {
  const actual = await orig<typeof import('@percolatorct/shared')>();
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: ['ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB'] },
    getConnection: vi.fn(() => ({ getAccountInfo: vi.fn(async () => ({ data: Buffer.alloc(64) })) })),
    withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    eventBus: { on: vi.fn(), off: vi.fn(), publish: m.publish },
  };
});
vi.mock('../../src/db/insertTradeRow.js', () => ({ insertTradeRow: m.insertTradeRow, insertTradeRows: vi.fn(), tradeKey: vi.fn() }));
vi.mock('../../src/db/storedLegs.js', async (orig) => ({
  ...(await orig<typeof import('../../src/db/storedLegs.js')>()),
  fetchStoredLegs: vi.fn(async () => m.stored.rows),
}));
vi.mock('../../src/parsers/matcherFill.js', async (orig) => ({ ...(await orig<typeof import('../../src/parsers/matcherFill.js')>()), resolveCpiLeg: m.resolveCpiLeg }));
vi.mock('../../src/parsers/markPrice.js', () => ({ readAssetEffectivePriceE6: m.effective }));
vi.mock('../../src/lib/skippedSignatures.js', () => ({ recordSkippedSignatures: m.skipped, assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock('../../src/services/v22LogEvents.js', () => ({ recordV22LogEvents: vi.fn() }));

import { TradeIndexerPolling } from '../../src/services/TradeIndexer.js';

const pk = (s: string) => new PublicKey(s);
function sizeLE(size: bigint, negative = false): Uint8Array {
  const b = new Uint8Array(16);
  let v = negative ? (1n << 128n) - size : size;
  for (let i = 0; i < 16; i++) { b[i] = Number(v & 0xffn); v >>= 8n; }
  return b;
}
/** v18 TradeNoCpi: asset@33, size_q@43, fee_bps@67. */
function noCpi(asset: number, size: bigint, feeBps = 0n, negative = false): Uint8Array {
  const d = new Uint8Array(75);
  d[0] = IX_TAG.TradeNoCpi;
  d[33] = asset & 0xff; d[34] = asset >> 8;
  d.set(sizeLE(size, negative), 43);
  new DataView(d.buffer).setBigUint64(67, feeBps, true);
  return d;
}
/** v18 TradeCpi: asset@41, size_q@51. */
function cpi(asset: number, size: bigint): Uint8Array {
  const d = new Uint8Array(83);
  d[0] = IX_TAG.TradeCpi;
  d[41] = asset & 0xff; d[42] = asset >> 8;
  d.set(sizeLE(size), 51);
  return d;
}
/** BatchTradeNoCpi: [tag, n, legs of 42B: asset@0, size@10, exec@26, fee_bps@34]. */
function batchNoCpi(legs: Array<{ asset: number; size: bigint; fee?: bigint }>): Uint8Array {
  const d = new Uint8Array(2 + legs.length * 42);
  d[0] = IX_TAG.BatchTradeNoCpi; d[1] = legs.length;
  legs.forEach((l, i) => {
    const o = 2 + i * 42;
    d[o] = l.asset & 0xff; d[o + 1] = l.asset >> 8;
    d.set(sizeLE(l.size), o + 10);
    new DataView(d.buffer).setBigUint64(o + 34, l.fee ?? 0n, true);
  });
  return d;
}
function reduce44(asset: number, q: bigint): Uint8Array {
  const d = new Uint8Array(35);
  d[0] = IX_TAG.RebalanceReduce;
  d[17] = asset & 0xff; d[18] = asset >> 8;
  d.set(sizeLE(q), 19);
  return d;
}
const ix = (data: Uint8Array, accounts: string[], programId = PROGRAM) => ({ programId: pk(programId), accounts: accounts.map(pk), data: encodeBase58(data) });
const tx = (instructions: unknown[], over: Record<string, unknown> = {}) => ({
  slot: 1, blockTime: 1_700_000_000,
  meta: { err: null, logMessages: [], innerInstructions: [] },
  transaction: { message: { instructions } },
  ...over,
}) as never;
const run = (t: unknown, sig = SIG, ...rest: []) => (new TradeIndexerPolling() as unknown as { processTransaction(...a: unknown[]): Promise<boolean> }).processTransaction(t, sig, SLAB, new Set([PROGRAM]), ...rest);
const rows = () => m.insertTradeRow.mock.calls.map((c) => c[0] as Record<string, unknown>);

describe('TradeIndexerPolling.processTransaction', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    m.insertTradeRow.mockResolvedValue(true);
    m.effective.mockReturnValue(2_000_000);
    m.stored.rows = [];
    m.resolveCpiLeg.mockResolvedValue({ kind: 'legacy' });
  });

  it('ignores a failed or meta-less transaction', async () => {
    expect(await run(tx([], { meta: null }))).toBe(false);
    expect(await run(tx([], { meta: { err: { InstructionError: [0, 'x'] } } }))).toBe(false);
    expect(m.insertTradeRow).not.toHaveBeenCalled();
  });

  it('TradeNoCpi: prices from the slab, charges fee on the notional, books the side', async () => {
    const t = tx([ix(noCpi(0, 5_000_000n, 10n), [TRADER, OTHER, SLAB])]);
    expect(await run(t)).toBe(true);
    expect(rows()).toEqual([expect.objectContaining({ slab_address: SLAB, trader: TRADER, side: 'long', size: '5000000', price: 2, leg_index: 0, asset_index: 0, size_provenance: 'legacy_unverified' })]);
    expect(rows()[0].fee).toBeCloseTo(0.01, 6);
    expect(m.publish).toHaveBeenCalledWith('trade.executed', SLAB, expect.objectContaining({ side: 'long', size: '5000000' }));
  });

  it('TradeNoCpi: a short, and a slab price that cannot be read, books price 0 (never a guess)', async () => {
    m.effective.mockReturnValue(null); // no v18 slot, no wrapper magic, no layout
    await run(tx([ix(noCpi(1, 7n, 0n, true), [TRADER, OTHER, SLAB])]));
    expect(rows()[0]).toEqual(expect.objectContaining({ side: 'short', price: 0, fee: 0, asset_index: 1 }));
  });

  it('a fill for another market writes nothing but still consumes its leg number', async () => {
    const t = tx([ix(noCpi(0, 3n), [TRADER, OTHER, OTHER]), ix(noCpi(0, 4n), [TRADER, OTHER, SLAB])]);
    await run(t);
    expect(rows().map((r) => [r.size, r.leg_index])).toEqual([['4', 1]]);
  });

  it('a batch for another market consumes one leg number per leg', async () => {
    const t = tx([
      ix(batchNoCpi([{ asset: 0, size: 1n }, { asset: 1, size: 2n }]), [TRADER, OTHER, OTHER]),
      ix(noCpi(0, 4n), [TRADER, OTHER, SLAB]),
    ]);
    await run(t);
    expect(rows().map((r) => r.leg_index)).toEqual([2]);
  });

  it('a single fill for another market with an undecodable body consumes no leg number', async () => {
    const short = noCpi(0, 3n).slice(0, 50); // below the decoder's minimum length
    await run(tx([ix(short, [TRADER, OTHER, OTHER]), ix(noCpi(0, 4n), [TRADER, OTHER, SLAB])]));
    expect(rows().map((r) => r.leg_index)).toEqual([0]);
  });

  it('BatchTradeNoCpi: one row per leg, priced per asset, with its own fee and leg index', async () => {
    m.effective.mockImplementation((_d, a) => (a === 0 ? 1_000_000 : 4_000_000));
    await run(tx([ix(batchNoCpi([{ asset: 0, size: 1_000_000n, fee: 20n }, { asset: 1, size: 2_000_000n }]), [TRADER, OTHER, SLAB])]));
    expect(rows().map((r) => [r.asset_index, r.price, r.leg_index, r.size])).toEqual([[0, 1, 0, '1000000'], [1, 4, 1, '2000000']]);
    expect(rows()[0].fee).toBeCloseTo(0.002, 6);
    expect(rows()[1].fee).toBe(0);
  });

  it('BatchTradeNoCpi: an empty batch and a batch without a trader write nothing', async () => {
    expect(await run(tx([ix(batchNoCpi([]), [TRADER, OTHER, SLAB])]))).toBe(false);
    const noTrader = { programId: pk(PROGRAM), accounts: [] as PublicKey[], data: encodeBase58(batchNoCpi([{ asset: 0, size: 1n }])) };
    expect(await run(tx([noTrader]))).toBe(false);
    expect(m.insertTradeRow).not.toHaveBeenCalled();
  });

  it('BatchTradeNoCpi: a malformed signature aborts the transaction (returns false)', async () => {
    expect(await run(tx([ix(batchNoCpi([{ asset: 0, size: 1n }]), [TRADER, OTHER, SLAB])]), 'short')).toBe(false);
    expect(m.insertTradeRow).not.toHaveBeenCalled();
  });

  it('BatchTradeNoCpi: a leg beyond the i128 range is skipped, later legs keep their numbers', async () => {
    const d = batchNoCpi([{ asset: 0, size: 1n }, { asset: 1, size: 2n }]);
    d.set(sizeLE(1n << 127n), 2 + 10); // as a signed i128 this is the minimum (-2^127); its abs value exceeds i128Max
    await run(tx([ix(d, [TRADER, OTHER, SLAB])]));
    expect(rows().map((r) => [r.asset_index, r.leg_index])).toEqual([[1, 1]]);
  });

  it('BatchTradeCpi: zero-fill / unverifiable skips, inexact fill and exact fill outcomes', async () => {
    const d = batchNoCpi([{ asset: 0, size: 1n }, { asset: 1, size: 2n }, { asset: 2, size: 3n }, { asset: 3, size: 4n }]);
    d[0] = IX_TAG.BatchTradeCpi;
    m.resolveCpiLeg
      .mockResolvedValueOnce({ kind: 'skip', reason: 'zero-fill', detail: '' })
      .mockResolvedValueOnce({ kind: 'skip', reason: 'unverifiable', detail: 'why' })
      .mockResolvedValueOnce({ kind: 'fill', sizeValue: 2n, priceE6: 3_500_000n, exact: false })
      .mockResolvedValueOnce({ kind: 'fill', sizeValue: 4n, priceE6: 1_000_000n, exact: true });
    await run(tx([ix(d, [TRADER, SLAB])]));
    expect(rows().map((r) => [r.leg_index, r.size, r.price, r.size_provenance])).toEqual([
      [2, '2', 3.5, 'matcher_request_upper_bound'],
      [3, '4', 1, 'matcher_exact'],
    ]);
    expect(m.skipped).toHaveBeenCalledWith([expect.objectContaining({ slab: SLAB, error: expect.stringContaining('TradeCpi unverifiable: why') })]);
  });

  it('BatchTradeCpi: a read error holds the signature (throws) and writes no later leg', async () => {
    const d = batchNoCpi([{ asset: 0, size: 1n }, { asset: 1, size: 2n }]);
    d[0] = IX_TAG.BatchTradeCpi;
    m.resolveCpiLeg.mockResolvedValueOnce({ kind: 'read-error' });
    await expect(run(tx([ix(d, [TRADER, SLAB])]))).rejects.toThrow('matcher context read failed');
    expect(m.insertTradeRow).not.toHaveBeenCalled();
    expect(m.resolveCpiLeg).toHaveBeenCalledTimes(1);
  });

  it('TradeCpi: read error throws, zero-fill writes nothing, unverifiable is recorded as skipped, inexact fill is the upper bound', async () => {
    const t = tx([ix(cpi(0, 9n), [TRADER, SLAB])]);
    m.resolveCpiLeg.mockResolvedValueOnce({ kind: 'read-error' });
    await expect(run(t)).rejects.toThrow('signature will be retried');

    m.resolveCpiLeg.mockResolvedValueOnce({ kind: 'skip', reason: 'zero-fill', detail: '' });
    expect(await run(t)).toBe(false);
    expect(m.skipped).not.toHaveBeenCalled();

    m.resolveCpiLeg.mockResolvedValueOnce({ kind: 'skip', reason: 'no-evidence', detail: 'd' });
    expect(await run(t)).toBe(false);
    expect(m.skipped).toHaveBeenCalledTimes(1);

    m.resolveCpiLeg.mockResolvedValueOnce({ kind: 'fill', sizeValue: 5n, priceE6: 2_500_000n, exact: false });
    expect(await run(t)).toBe(true);
    expect(rows()[0]).toEqual(expect.objectContaining({ size: '5', price: 2.5, size_provenance: 'matcher_request_upper_bound' }));
  });

  it('TradeCpi: a fill already stored under the same leg is not written again', async () => {
    m.stored.rows = [{ slab_address: SLAB, asset_index: 0, leg_index: 0, trader: TRADER, side: 'long', size: '9' }];
    await run(tx([ix(cpi(0, 9n), [TRADER, SLAB])]));
    expect(m.resolveCpiLeg).not.toHaveBeenCalled();
    expect(m.insertTradeRow).not.toHaveBeenCalled();
  });

  it('a malformed signature on a single fill returns false without writing', async () => {
    expect(await run(tx([ix(noCpi(0, 3n), [TRADER, OTHER, SLAB])]), 'short')).toBe(false);
    expect(m.insertTradeRow).not.toHaveBeenCalled();
  });

  it('single fill without a trader account is skipped', async () => {
    const noTrader = { programId: pk(PROGRAM), accounts: [] as PublicKey[], data: encodeBase58(noCpi(0, 3n)) };
    expect(await run(tx([noTrader]))).toBe(false);
  });

  it('skips parsed instructions, other programs, empty data and non-trade tags', async () => {
    const other = ix(noCpi(0, 3n), [TRADER, OTHER, SLAB], OTHER);
    const nonTrade = new Uint8Array(80); nonTrade[0] = 250;
    const t = tx([{ parsed: {}, program: 'system', programId: pk(OTHER) }, other, ix(new Uint8Array(0), [TRADER, OTHER, SLAB]), ix(nonTrade, [TRADER, OTHER, SLAB])]);
    expect(await run(t)).toBe(false);
    expect(m.insertTradeRow).not.toHaveBeenCalled();
  });

  it('tag 44: a malformed reduce, or one for another market, is dropped without a row', async () => {
    const zero = reduce44(0, 1n); zero.set(new Uint8Array(16), 19); // reduce_q 0: the program rejects it
    await run(tx([ix(zero, [TRADER, SLAB]), ix(reduce44(0, 5n), [TRADER, OTHER])]));
    expect(m.insertTradeRow).not.toHaveBeenCalled();
    expect(m.skipped).not.toHaveBeenCalled();
  });

  it('an unqueued fill insert failure propagates to the caller', async () => {
    m.insertTradeRow.mockRejectedValueOnce(new Error('db down'));
    await expect(run(tx([ix(noCpi(0, 3n), [TRADER, OTHER, SLAB])]))).rejects.toThrow('db down');
  });
});
