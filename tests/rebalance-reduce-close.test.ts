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

vi.mock('@percolatorct/shared', () => ({
  config: {
    allProgramIds: ['ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB'],
    webhookSecret: 'test-secret-token',
  },
  createLogger: vi.fn(() => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() })),
  eventBus: { publish: vi.fn(), on: vi.fn(), off: vi.fn() },
  decodeBase58: vi.fn((s: string) => decodeB58(s)),
  parseTradeSize: vi.fn(),
  readU128LE: vi.fn(),
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  captureException: vi.fn(),
  addBreadcrumb: vi.fn(),
  tradeExistsBySignature: vi.fn(async () => false),
  getMarkets: vi.fn(async () => []),
  getConnection: vi.fn(() => ({ getAccountInfo: vi.fn(async () => null) })),
  getSupabase: vi.fn(),
  getNetwork: vi.fn(() => 'devnet'),
}));

vi.mock('../src/db/insertTradeRow.js', () => {
  const insertTradeRow = vi.fn();
  return {
    insertTradeRow,
    tradeKey: (r: any) => `${r.tx_signature ?? ''}|${r.asset_index}|${r.leg_index}`,
    insertTradeRows: vi.fn(async (rows: any[]) => {
      for (const r of rows) await insertTradeRow(r);
      return rows.map((r) => ({ tx_signature: r.tx_signature, asset_index: r.asset_index, leg_index: r.leg_index }));
    }),
  };
});

vi.mock('../src/db/traderNetPosition.js', () => ({ fetchTraderNetPositionQ: vi.fn() }));

import { IX_TAG, encodeRebalanceReduce } from '@percolatorct/sdk';
import { insertTradeRow } from '../src/db/insertTradeRow.js';
import { fetchTraderNetPositionQ } from '../src/db/traderNetPosition.js';
import { decodeRebalanceReduce, rebalanceReduceFill } from '../src/parsers/percolatorTxParser.js';
import { TradeIndexerPolling } from '../src/services/TradeIndexer.js';
import { webhookRoutes } from '../src/routes/webhook.js';

const accounts = [OWNER, SLAB, PORTFOLIO];

function parsedTx() {
  const ix = (data: string) => ({
    programId: new PublicKey(PROGRAM_ID),
    accounts: accounts.map((a) => new PublicKey(a)),
    data,
  });
  return {
    meta: { err: null, logMessages: [] },
    transaction: { message: { instructions: [ix(CRANK_DATA), ix(REBALANCE_DATA)] } },
  };
}

function enhancedTx() {
  return {
    signature: SIG,
    instructions: [
      { programId: PROGRAM_ID, data: CRANK_DATA, accounts },
      { programId: PROGRAM_ID, data: REBALANCE_DATA, accounts },
    ],
    innerInstructions: [],
    accountData: [],
    logs: [],
  };
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
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(fetchTraderNetPositionQ).mockResolvedValue(-OPEN_SHORT_Q);
  });

  it('poll/backfill path (TradeIndexer.processTransaction)', async () => {
    const indexer = new TradeIndexerPolling();
    const did = await (indexer as any).processTransaction(parsedTx(), SIG, SLAB, new Set([PROGRAM_ID]));
    expect(did).toBe(true);
    expect(fetchTraderNetPositionQ).toHaveBeenCalledWith(OWNER, SLAB, 0, SIG);
    expect(insertTradeRow).toHaveBeenCalledTimes(1);
    expect(insertTradeRow).toHaveBeenCalledWith(expect.objectContaining(expectedRow));
  });

  it('poll path ignores a tag 44 on another slab', async () => {
    const indexer = new TradeIndexerPolling();
    const other = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
    const did = await (indexer as any).processTransaction(parsedTx(), SIG, other, new Set([PROGRAM_ID]));
    expect(did).toBe(false);
    expect(insertTradeRow).not.toHaveBeenCalled();
  });

  it('webhook path (Helius enhanced tx)', async () => {
    const app = webhookRoutes({ getMarkets: () => new Map([[SLAB, {}]]) });
    const res = await app.fetch(new Request('http://localhost/webhook/trades', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', authorization: 'test-secret-token' },
      body: JSON.stringify([enhancedTx()]),
    }));
    expect(res.status).toBe(200);
    expect(insertTradeRow).toHaveBeenCalledTimes(1);
    expect(insertTradeRow).toHaveBeenCalledWith(expect.objectContaining({ ...expectedRow, is_liquidation: false }));
  });

  it('no indexed open position: nothing is written rather than a guessed side', async () => {
    vi.mocked(fetchTraderNetPositionQ).mockResolvedValue(0n);
    const indexer = new TradeIndexerPolling();
    expect(await (indexer as any).processTransaction(parsedTx(), SIG, SLAB, new Set([PROGRAM_ID]))).toBe(false);
    const app = webhookRoutes({ getMarkets: () => new Map([[SLAB, {}]]) });
    await app.fetch(new Request('http://localhost/webhook/trades', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', authorization: 'test-secret-token' },
      body: JSON.stringify([enhancedTx()]),
    }));
    expect(insertTradeRow).not.toHaveBeenCalled();
  });
});
