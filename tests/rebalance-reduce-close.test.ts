import { describe, it, expect, vi, beforeEach } from 'vitest';
import { PublicKey } from '@solana/web3.js';

/**
 * A close made while the asset is ADL reduce-only ("close-only") goes on chain as
 * RebalanceReduce (tag 44), not TradeCpi — the app's Close tab switches to it
 * (launch app/lib/limits/rebalance-close.ts). Neither the poll path nor the webhook
 * path decoded tag 44, so these closes never reached the trader's history or the
 * market's trades.
 *
 * Fixture = the real devnet close
 * 2KzadtJFSM4RDtQKDiRsaJkpnS9q9MAfCwYLFivtFYdkJ6KkheQJaQFSqGyHpfE3Effk8g4VdTXDa9cio6DCNM3L
 * (SOL/USD, 100% close of a ~1.285464 SOL short): [KeeperCrank tag 5, RebalanceReduce tag 44],
 * both with accounts [owner, market, portfolio].
 *
 * The SDK is NOT mocked here, so IX_TAG.RebalanceReduce is the real 44.
 */

const PROGRAM_ID = 'ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB';
const OWNER = '9fie3RtdBTLnhWXDWmiVe9XGVVmJykFNwD38rnoiRDJW';
const SLAB = '9efj3hdgb2qQvkHKYP9DjZYJQZqC1XiY5XgCiZkvss7u';
const PORTFOLIO = '2xowUgZuRS3KXvfkJnKRh6rcMpWr5FNxpb69q17QULFy';
const SIG = '2KzadtJFSM4RDtQKDiRsaJkpnS9q9MAfCwYLFivtFYdkJ6KkheQJaQFSqGyHpfE3Effk8g4VdTXDa9cio6DCNM3L';
/** Instruction data exactly as landed on chain (base58). */
const CRANK_DATA = 'R9yRJ5Qy4j8ckGz2T';
const REBALANCE_DATA = '5PmtfKL2KS8RL5Rky6VeYBP2bPVyuo5LQ1yiyAxCPd8K8wHq';
/** reduce_q in that instruction = 0x139d58. */
const REDUCE_Q = 1_285_464n;
/** The short it closed, as recorded from its TradeCpi open (basis before ADL). */
const OPEN_SHORT_Q = 1_658_210n;

function decodeB58(s: string): Uint8Array {
  const A = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
  let n = 0n;
  for (const c of s) {
    const i = A.indexOf(c);
    if (i < 0) throw new Error('bad base58');
    n = n * 58n + BigInt(i);
  }
  const out: number[] = [];
  while (n > 0n) { out.unshift(Number(n & 0xffn)); n >>= 8n; }
  for (const c of s) { if (c !== '1') break; out.unshift(0); }
  return Uint8Array.from(out);
}

const h = vi.hoisted(() => ({
  /** Fake `trades` rows the position lookup reads (filters are asserted in tests/db/traderNetPosition.test.ts). */
  rows: [] as Array<Record<string, unknown>>,
  /** Rows upserted into skipped_signatures. */
  skipped: [] as Array<Record<string, unknown>>,
  /** base58-string -> bytes overrides for synthetic instructions. */
  data: new Map<string, Uint8Array>(),
  sigs: [] as Array<{ signature: string; err: null }>,
  txs: new Map<string, unknown>(),
}));

function fakeSupabase() {
  return {
    from: (table: string) => {
      if (table === 'skipped_signatures') {
        return { upsert: async (rows: Array<Record<string, unknown>>) => { h.skipped.push(...rows); return { error: null }; } };
      }
      const q: any = {};
      let sig: string | null = null;
      for (const m of ['select', 'neq', 'order']) q[m] = () => q;
      q.eq = (col: string, val: unknown) => { if (col === 'tx_signature') sig = String(val); return q; };
      // Awaiting the chain without .range() is the per-transaction stored-legs read.
      q.then = (res: (v: unknown) => unknown) => res({ data: h.rows.filter((r) => r.tx_signature === sig), error: null });
      q.range = async () => ({ data: h.rows, error: null });
      return q;
    },
  };
}

vi.mock('@percolatorct/shared', () => ({
  config: {
    allProgramIds: ['ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB'],
    webhookSecret: 'test-secret-token',
  },
  createLogger: vi.fn(() => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() })),
  eventBus: { publish: vi.fn(), on: vi.fn(), off: vi.fn() },
  decodeBase58: vi.fn((s: string) => h.data.get(s) ?? decodeB58(s)),
  parseTradeSize: vi.fn(() => ({ sizeValue: 777n, side: 'short' as const })),
  readU128LE: vi.fn(),
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  captureException: vi.fn(),
  addBreadcrumb: vi.fn(),
  tradeExistsBySignature: vi.fn(async () => false),
  getMarkets: vi.fn(async () => []),
  getConnection: vi.fn(() => ({
    getAccountInfo: vi.fn(async () => null),
    getSignaturesForAddress: vi.fn(async () => h.sigs),
    getParsedTransactions: vi.fn(async (sigs: string[]) => sigs.map((x) => h.txs.get(x) ?? null)),
  })),
  getSupabase: vi.fn(() => fakeSupabase()),
  getNetwork: vi.fn(() => 'devnet'),
}));

vi.mock('../src/db/insertTradeRow.js', () => {
  const insertTradeRow = vi.fn();
  return {
    insertTradeRow,
    tradeKey: (r: any) => `${r.tx_signature ?? ''}|${r.asset_index}|${r.leg_index}`,
    insertTradeRows: vi.fn(async (rows: any[]) => {
      const written: any[] = [];
      for (const r of rows) if (await insertTradeRow(r)) written.push(r);
      return written.map((r) => ({ tx_signature: r.tx_signature, asset_index: r.asset_index, leg_index: r.leg_index }));
    }),
  };
});


import { IX_TAG, encodeRebalanceReduce } from '@percolatorct/sdk';
import { insertTradeRow } from '../src/db/insertTradeRow.js';
import * as shared from '@percolatorct/shared';
import { resetSkippedSignatureDedupe } from '../src/lib/skippedSignatures.js';
import { decodeRebalanceReduce, rebalanceReduceFill, parsePercolatorFills } from '../src/parsers/percolatorTxParser.js';
import { TradeIndexerPolling } from '../src/services/TradeIndexer.js';
import { webhookRoutes } from '../src/routes/webhook.js';

const accounts = [OWNER, SLAB, PORTFOLIO];

const BLOCK_TIME = 1_700_000_000; // 2023-11-14T22:13:20Z
const OPEN_ROW_AT = '2023-11-01T00:00:00Z'; // indexed before the close: provably earlier

/** The TradeCpi open that created the short (indexed long before the close). */
const shortOpenRow = (over: Record<string, unknown> = {}) => ({
  side: 'short', size: OPEN_SHORT_Q.toString(), is_liquidation: false, created_at: OPEN_ROW_AT, ...over,
});

const ixOf = (data: string, accts: string[] = accounts) => ({
  programId: new PublicKey(PROGRAM_ID),
  accounts: accts.map((a) => new PublicKey(a)),
  data,
});
const parsedTxOf = (ixs: unknown[], blockTime: number | null = BLOCK_TIME) => ({
  blockTime,
  meta: { err: null, logMessages: [] },
  transaction: { message: { instructions: ixs } },
});
const parsedTx = () => parsedTxOf([ixOf(CRANK_DATA), ixOf(REBALANCE_DATA)]);

const enhIx = (data: string, accts: string[] = accounts) => ({ programId: PROGRAM_ID, data, accounts: accts });
const enhancedTxOf = (ixs: unknown[], timestamp: number | null = BLOCK_TIME, signature: string = SIG, slot?: number) => ({
  signature, ...(slot === undefined ? {} : { slot }), ...(timestamp === null ? {} : { timestamp }), instructions: ixs, innerInstructions: [], accountData: [], logs: [],
});
const enhancedTx = () => enhancedTxOf([enhIx(CRANK_DATA), enhIx(REBALANCE_DATA)]);

/** A tag 44 with an arbitrary asset / reduce_q (bytes registered under a fake key). */
function reduceData(key: string, assetIndex: number, reduceQ: bigint): string {
  h.data.set(key, encodeRebalanceReduce({ portfolioId: 6n, positionEpoch: 0n, assetIndex, reduceQ }));
  return key;
}
/** A TradeCpi (tag 10) single fill; parseTradeSize is mocked to 777 short. */
function tradeCpiData(key: string): string {
  const d = new Uint8Array(85);
  d[0] = IX_TAG.TradeCpi;
  h.data.set(key, d);
  return key;
}

const expectedRow = {
  slab_address: SLAB,
  trader: OWNER,
  side: 'long', // closing a short buys — the side a TradeCpi close of a short is recorded with
  size: REDUCE_Q.toString(),
  fee: 0,
  tx_signature: SIG,
  asset_index: 0,
  leg_index: 0,
};

async function postWebhook(txs: unknown[]) {
  const app = webhookRoutes({ getMarkets: () => new Map([[SLAB, {}]]) });
  return app.fetch(new Request('http://localhost/webhook/trades', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', authorization: 'test-secret-token' },
    body: JSON.stringify(txs),
  }));
}
const poll = (tx: unknown) =>
  (new TradeIndexerPolling() as any).processTransaction(tx, SIG, SLAB, new Set([PROGRAM_ID]));
const written = () => vi.mocked(insertTradeRow).mock.calls.map((c) => c[0] as any);

describe('RebalanceReduce (tag 44) decode', () => {
  it('decodes the on-chain close: asset 0, reduce_q 1.285464 SOL', () => {
    expect(IX_TAG.RebalanceReduce).toBe(44);
    expect(decodeRebalanceReduce(decodeB58(REBALANCE_DATA))).toEqual({ assetIndex: 0, reduceQ: REDUCE_Q });
  });

  it('is byte-exact against the SDK encoder', () => {
    const bytes = encodeRebalanceReduce({ portfolioId: 6n, positionEpoch: 0n, assetIndex: 3, reduceQ: (1n << 100n) + 7n });
    expect(bytes.length).toBe(35);
    expect(decodeRebalanceReduce(bytes)).toEqual({ assetIndex: 3, reduceQ: (1n << 100n) + 7n });
  });

  it('rejects other tags, short data and reduce_q 0', () => {
    expect(decodeRebalanceReduce(decodeB58(CRANK_DATA))).toBeNull();
    expect(decodeRebalanceReduce(decodeB58(REBALANCE_DATA).slice(0, 34))).toBeNull();
    const zero = encodeRebalanceReduce({ portfolioId: 6n, positionEpoch: 0n, assetIndex: 0, reduceQ: 0n });
    expect(decodeRebalanceReduce(zero)).toBeNull();
  });

  it('takes the side opposite the position and caps the size at it', () => {
    expect(rebalanceReduceFill(REDUCE_Q, -OPEN_SHORT_Q)).toEqual({ side: 'long', sizeValue: REDUCE_Q });
    expect(rebalanceReduceFill(5n, 3n)).toEqual({ side: 'short', sizeValue: 3n });
    expect(rebalanceReduceFill(5n, 0n)).toBeNull();
  });
});

describe('a close-only (tag 44) close reaches the trades table', () => {
  const dbRows = new Set<string>();
  beforeEach(() => {
    vi.clearAllMocks();
    h.rows = [shortOpenRow()];
    h.skipped = [];
    h.data.clear();
    h.sigs = [];
    h.txs.clear();
    dbRows.clear();
    resetSkippedSignatureDedupe();
    // Like the unique index: a repeated (sig, asset, leg) reports false (23505 swallowed).
    vi.mocked(insertTradeRow).mockImplementation(async (r: any) => {
      const k = `${r.tx_signature}|${r.asset_index}|${r.leg_index}`;
      if (dbRows.has(k)) return false;
      dbRows.add(k);
      return true;
    });
  });

  it('poll/backfill path (TradeIndexer.processTransaction)', async () => {
    expect(await poll(parsedTx())).toBe(true);
    expect(written()).toHaveLength(1);
    expect(written()[0]).toEqual(expect.objectContaining(expectedRow));
    expect(h.skipped).toHaveLength(0);
  });

  it('poll path ignores a tag 44 on another slab', async () => {
    const other = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
    const did = await (new TradeIndexerPolling() as any).processTransaction(parsedTx(), SIG, other, new Set([PROGRAM_ID]));
    expect(did).toBe(false);
    expect(insertTradeRow).not.toHaveBeenCalled();
  });

  it('webhook path (Helius enhanced tx)', async () => {
    expect((await postWebhook([enhancedTx()])).status).toBe(200);
    expect(written()).toHaveLength(1);
    expect(written()[0]).toEqual(expect.objectContaining({ ...expectedRow, is_liquidation: false }));
  });

  // (a) leg numbering
  describe('leg numbering: one tx-wide fill counter shared with every other fill', () => {
    it('tag 44 then TradeCpi in one tx: legs 0 and 1 on the poll path, never a collision', async () => {
      const ixs = [ixOf(REBALANCE_DATA), ixOf(tradeCpiData('cpi'))];
      expect(await poll(parsedTxOf(ixs))).toBe(true);
      expect(written().map((r) => [r.leg_index, r.size, r.side])).toEqual([[0, REDUCE_Q.toString(), 'long'], [1, '777', 'short']]);
    });

    it('TradeCpi then tag 44 on the SAME position in one tx: the close holds leg 1 but is not resolved (the earlier fill is not in the table yet)', async () => {
      await poll(parsedTxOf([ixOf(tradeCpiData('cpi')), ixOf(REBALANCE_DATA)]));
      expect(written().map((r) => r.leg_index)).toEqual([0]);
      expect(h.skipped).toEqual([expect.objectContaining({ signature: SIG, error: expect.stringContaining('earlier fill on the same position') })]);
    });

    it('TradeCpi on another asset, then tag 44: the close is leg 1 and is resolved', async () => {
      await poll(parsedTxOf([ixOf(tradeCpiData('cpi')), ixOf(reduceData('other-asset', 1, 100n))]));
      expect(written().map((r) => [r.asset_index, r.leg_index])).toEqual([[0, 0], [1, 1]]);
    });

    it('webhook numbers the same way (tag 44 = 0, TradeCpi = 1)', async () => {
      await postWebhook([enhancedTxOf([enhIx(REBALANCE_DATA), enhIx(tradeCpiData('cpi'))])]);
      expect(written().map((r) => [r.leg_index, r.size])).toEqual([[0, REDUCE_Q.toString()], [1, '777']]);
    });

    it('webhook keeps the TradeCpi at leg 1 even when the tag 44 could not be written', async () => {
      h.rows = []; // no indexed position: the close is skipped, but still holds leg 0
      await postWebhook([enhancedTxOf([enhIx(REBALANCE_DATA), enhIx(tradeCpiData('cpi'))])]);
      expect(written().map((r) => [r.leg_index, r.size])).toEqual([[1, '777']]);
    });

    it('the event-stream parser gives a tag 44 the same slot, flagged so it is never written', () => {
      const fills = parsePercolatorFills(parsedTxOf([ixOf(REBALANCE_DATA), ixOf(tradeCpiData('cpi'))]) as any, SIG, [PROGRAM_ID]);
      expect(fills.map((f) => !!f.rebalanceReduce)).toEqual([true, false]);
    });
  });

  // (b) no early return
  describe('the rest of the transaction is still processed', () => {
    const OTHER_ACCOUNTS = [OWNER, SLAB, PORTFOLIO];
    it('two tag 44s on different assets are both indexed (poll and webhook), legs 0 and 1', async () => {
      h.rows = [shortOpenRow()]; // same rows answer both assets in this fake
      const a0 = reduceData('r-a0', 0, 100n);
      const a1 = reduceData('r-a1', 1, 200n);
      expect(await poll(parsedTxOf([ixOf(a0, OTHER_ACCOUNTS), ixOf(a1, OTHER_ACCOUNTS)]))).toBe(true);
      expect(written().map((r) => [r.asset_index, r.leg_index, r.size])).toEqual([[0, 0, '100'], [1, 1, '200']]);

      vi.mocked(insertTradeRow).mockClear();
      dbRows.clear();
      await postWebhook([enhancedTxOf([enhIx(a0), enhIx(a1)])]);
      expect(written().map((r) => [r.asset_index, r.leg_index, r.size])).toEqual([[0, 0, '100'], [1, 1, '200']]);
    });

    it('a trade instruction AFTER a tag 44 is still indexed on the poll path', async () => {
      await poll(parsedTxOf([ixOf(REBALANCE_DATA), ixOf(tradeCpiData('cpi'))]));
      expect(written()).toHaveLength(2);
    });
  });

  // (c) size
  describe('size', () => {
    it('a clipped close (reduce_q larger than the position) is capped at the position', async () => {
      h.rows = [shortOpenRow({ size: '1000' })];
      await poll(parsedTxOf([ixOf(reduceData('big', 0, 5_000n))]));
      expect(written()[0]).toEqual(expect.objectContaining({ side: 'long', size: '1000' }));
    });

    it('a partial close keeps the requested reduce_q', async () => {
      h.rows = [shortOpenRow({ size: '1000' })];
      await poll(parsedTxOf([ixOf(reduceData('part', 0, 400n))]));
      expect(written()[0]).toEqual(expect.objectContaining({ side: 'long', size: '400' }));
    });

    it('undeterminable: no indexed position -> no row, signature recorded in skipped_signatures', async () => {
      h.rows = [];
      expect(await poll(parsedTx())).toBe(false);
      expect(insertTradeRow).not.toHaveBeenCalled();
      expect(h.skipped).toEqual([expect.objectContaining({ signature: SIG, source: 'trade-indexer', slab: SLAB, error: expect.stringContaining('no indexed open position') })]);
    });

    it.each([
      ['a liquidation marker exists for the position', () => [shortOpenRow(), { side: null, size: null, is_liquidation: true, created_at: OPEN_ROW_AT }], 'liquidation marker'],
      ['a fill indexed at/after the close (re-index after later fills)', () => [shortOpenRow(), shortOpenRow({ created_at: '2023-11-20T00:00:00Z' })], 'not provably earlier'],
    ])('undeterminable: %s -> skipped_signatures, never a guessed row', async (_n, rows, why) => {
      h.rows = rows();
      expect(await poll(parsedTx())).toBe(false);
      expect(insertTradeRow).not.toHaveBeenCalled();
      expect(h.skipped).toEqual([expect.objectContaining({ signature: SIG, error: expect.stringContaining(why) })]);
    });

    it('undeterminable: unknown block time -> skipped_signatures', async () => {
      expect(await poll(parsedTxOf([ixOf(REBALANCE_DATA)], null))).toBe(false);
      expect(insertTradeRow).not.toHaveBeenCalled();
      expect(h.skipped).toHaveLength(1);
    });

    it('undeterminable: a second tag 44 on the same position in one tx -> only the first is written, the second is skipped', async () => {
      await poll(parsedTxOf([ixOf(reduceData('x1', 0, 100n)), ixOf(reduceData('x2', 0, 50n))]));
      expect(written().map((r) => [r.leg_index, r.size])).toEqual([[0, '100']]);
      expect(h.skipped).toHaveLength(1);
    });

    it('webhook: undeterminable close lands in skipped_signatures too, nothing written', async () => {
      h.rows = [];
      expect((await postWebhook([enhancedTx()])).status).toBe(200);
      expect(insertTradeRow).not.toHaveBeenCalled();
      expect(h.skipped).toEqual([expect.objectContaining({ signature: SIG, source: 'trade-indexer' })]);
    });
  });

  // F1/F2 + old numbering: a table-backed insert, so rows written in this test are visible to later reads.
  describe('ordering, redelivery and old numbering (table-backed)', () => {
    const SIG_A = '4VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW'; // earlier tx
    const NOW = () => new Date().toISOString();
    const tableBacked = () => vi.mocked(insertTradeRow).mockImplementation(async (r: any) => {
      if (h.rows.some((x) => x.tx_signature === r.tx_signature && x.asset_index === r.asset_index && x.leg_index === r.leg_index)) return false;
      h.rows.push({ ...r, is_liquidation: false, created_at: NOW() });
      return true;
    });
    const sell = (size: bigint) => vi.mocked(shared.parseTradeSize).mockReturnValue({ sizeValue: size, side: 'short' });
    const longOpen = (size: string) => ({ side: 'long', size, is_liquidation: false, created_at: OPEN_ROW_AT, tx_signature: 'OPEN' });
    const closeData = () => reduceData('close10', 0, 10n);

    // Indexed net +20 long. Unindexed tx A sells 30 (now short 10). Tx B (tag 44, reduce 10) must be
    // a "long" 10, not a "short" 10 (what resolving it against the pre-A position gives).
    it('F1 poll: a window is processed oldest-first, so a close sees the unindexed fill before it', async () => {
      h.rows = [longOpen('20')];
      tableBacked();
      sell(30n);
      const A = parsedTxOf([ixOf(tradeCpiData('A'))]);
      const B = parsedTxOf([ixOf(closeData())]);
      h.txs.set(SIG_A, A); h.txs.set(SIG, B);
      h.sigs = [{ signature: SIG, err: null }, { signature: SIG_A, err: null }]; // newest first, as RPC returns
      await (new TradeIndexerPolling() as any).indexTradesForSlab(SLAB, 100);
      expect(written().map((r) => [r.tx_signature === SIG_A ? 'A' : 'B', r.side, r.size])).toEqual([['A', 'short', '30'], ['B', 'long', '10']]);
      expect(h.skipped).toHaveLength(0);
    });

    it('F1 poll: an earlier tx that could not be indexed makes the close unresolvable (recorded, no guess)', async () => {
      h.rows = [longOpen('20')];
      tableBacked();
      h.txs.set(SIG, parsedTxOf([ixOf(closeData())]));
      // A is in the window but the RPC returns null for it.
      h.sigs = [{ signature: SIG, err: null }, { signature: SIG_A, err: null }];
      await (new TradeIndexerPolling() as any).indexTradesForSlab(SLAB, 100);
      expect(written()).toHaveLength(0);
      expect(h.skipped).toEqual([expect.objectContaining({ signature: SIG, error: expect.stringContaining('earlier transaction') })]);
    });

    it.each([['delivered oldest-first', false], ['delivered newest-first (sorted by slot)', true]])('F1 webhook: %s, the close sees the earlier transaction of the same delivery', async (_n, reversed) => {
      h.rows = [longOpen('20')];
      tableBacked();
      sell(30n);
      const txA = enhancedTxOf([enhIx(tradeCpiData('A'))], BLOCK_TIME, SIG_A, 100);
      const txB = enhancedTxOf([enhIx(closeData())], BLOCK_TIME, SIG, 101);
      expect((await postWebhook(reversed ? [txB, txA] : [txA, txB])).status).toBe(200);
      expect(written().map((r) => [r.tx_signature === SIG_A ? 'A' : 'B', r.side, r.size])).toEqual([['A', 'short', '30'], ['B', 'long', '10']]);
      expect(h.skipped).toHaveLength(0);
    });

    // F2: a tag 44 that is already stored is not resolved again, so a redelivery / backfill does not
    // find later rows "not provably earlier" and report a missing trade that is in fact there.
    it('F2 poll: an already stored tag 44 is neither re-resolved nor recorded as skipped', async () => {
      h.rows = [shortOpenRow(), { side: 'long', size: '100', is_liquidation: false, created_at: NOW(), tx_signature: SIG, slab_address: SLAB, asset_index: 0, leg_index: 0, trader: OWNER }, shortOpenRow({ created_at: NOW() })];
      tableBacked();
      expect(await poll(parsedTx())).toBe(false);
      expect(insertTradeRow).not.toHaveBeenCalled();
      expect(h.skipped).toHaveLength(0);
    });

    it('F2 webhook: a redelivered tag 44 is neither re-resolved nor recorded as skipped', async () => {
      h.rows = [shortOpenRow(), { side: 'long', size: '100', is_liquidation: false, created_at: NOW(), tx_signature: SIG, slab_address: SLAB, asset_index: 0, leg_index: 0, trader: OWNER }, shortOpenRow({ created_at: NOW() })];
      tableBacked();
      expect((await postWebhook([enhancedTx()])).status).toBe(200);
      expect(insertTradeRow).not.toHaveBeenCalled();
      expect(h.skipped).toHaveLength(0);
    });

    // A tx indexed under the old per-instruction numbering must not be stored a second time. A split
    // order is IDENTICAL legs, so only fills the stored rows do not already account for are written.
    it('old numbering poll: one identical row stored at another leg number accounts for exactly one of the identical fills', async () => {
      sell(777n);
      h.rows = [{ side: 'short', size: '777', is_liquidation: false, created_at: NOW(), tx_signature: SIG, slab_address: SLAB, asset_index: 0, leg_index: 7, trader: OWNER }];
      tableBacked();
      await poll(parsedTxOf([ixOf(tradeCpiData('x0')), ixOf(tradeCpiData('x1'))]));
      expect(written().map((r) => r.leg_index)).toEqual([1]); // leg 0 is the stored one (at leg 7); leg 1 is genuinely new
    });

    it('old numbering webhook: same', async () => {
      sell(777n);
      h.rows = [{ side: 'short', size: '777', is_liquidation: false, created_at: NOW(), tx_signature: SIG, slab_address: SLAB, asset_index: 0, leg_index: 7, trader: OWNER }];
      tableBacked();
      await postWebhook([enhancedTxOf([enhIx(tradeCpiData('x0')), enhIx(tradeCpiData('x1'))])]);
      expect(written().map((r) => r.leg_index)).toEqual([1]);
    });

    it('a split order of identical legs with nothing stored is still written in full', async () => {
      sell(777n);
      h.rows = [];
      tableBacked();
      await poll(parsedTxOf([ixOf(tradeCpiData('x0')), ixOf(tradeCpiData('x1')), ixOf(tradeCpiData('x2'))]));
      expect(written().map((r) => r.leg_index)).toEqual([0, 1, 2]);
    });
  });

  // (d) duplicates
  describe('a re-index does not duplicate', () => {
    it('poll: second pass over the same tx writes nothing and publishes nothing', async () => {
      expect(await poll(parsedTx())).toBe(true);
      expect(shared.eventBus.publish).toHaveBeenCalledTimes(1);
      vi.mocked(shared.eventBus.publish).mockClear();
      expect(await poll(parsedTx())).toBe(false); // offered, found duplicate
      expect(shared.eventBus.publish).not.toHaveBeenCalled();
      expect(dbRows.size).toBe(1);
    });

    it('webhook redelivery: the duplicate is not counted or published', async () => {
      await postWebhook([enhancedTx()]);
      vi.mocked(shared.eventBus.publish).mockClear();
      await postWebhook([enhancedTx()]);
      expect(shared.eventBus.publish).not.toHaveBeenCalled();
      expect(dbRows.size).toBe(1);
    });
  });
});
