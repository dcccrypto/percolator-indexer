import { describe, it, expect, vi, beforeEach } from 'vitest';
import * as nodeCrypto from 'node:crypto';

// These tests are about leg numbering / dedup, not about how a TradeCpi's executed size is proven
// (that is tests/matcher-fill-*.test.ts, against real transactions). Resolve every CPI leg to its
// wire size at a fixed price here.
vi.mock('../../src/parsers/matcherFill.js', async (orig) => ({
  ...(await orig<typeof import('../../src/parsers/matcherFill.js')>()),
  resolveCpiLeg: vi.fn(async (a: { wireSizeAbs: bigint }) => ({ kind: 'fill', sizeValue: a.wireSizeAbs, priceE6: 1_500_000n, exact: true })),
}));

vi.mock('@percolatorct/sdk', async (importOriginal) => ({
  // v2.2: the indexer's layout code uses the REAL SDK layout tables; tests override only what they stub.
  ...(await importOriginal<typeof import('@percolatorct/sdk')>()),
  // v17 IX_TAG: TradeCpiV2 (35) REMOVED; BatchTradeNoCpi (66) and BatchTradeCpi (67) added.
  IX_TAG: {
    TradeNoCpi: 10,
    TradeCpi: 11,
    // TradeCpiV2 deliberately omitted — deprecated in v17, tag 35 no longer exists.
    BatchTradeNoCpi: 66,
    BatchTradeCpi: 67,
  },
  // detectSlabLayout: returns a V1-style layout so engineOff + engineMarkPriceOff = 1040,
  // matching the mock slab buffers built in tests below.
  detectSlabLayout: vi.fn((dataLen: number) => {
    if (dataLen < 8) return null; // too small
    return { version: 1, engineOff: 640, engineMarkPriceOff: 400, engineBitmapOff: 656 };
  }),
  // v17 desync additions — default to false so existing v12 test paths pass through.
  isV17Account: vi.fn(() => false),
  parseWrapperConfigV17: vi.fn(),
  V17_HEADER_LEN: 16,
}));

// NOTE: This constant must be a literal string used inside vi.mock (which is hoisted before const declarations).
// The string 'test-secret-token' is duplicated intentionally — do not replace with a variable reference.
const TEST_WEBHOOK_SECRET = 'test-secret-token';

// H2/H3: trades are now written via the indexer-local insertTradeRow helper
// (src/db/insertTradeRow.ts), not shared's insertTrade. Mock it directly.
//
// The webhook now writes a whole delivery's legs in ONE batched call
// (insertTradeRows). The batch mock forwards each row to the per-row spy so the
// existing per-trade assertions still hold, and returns the inserted count that
// processTransactions reports back.
vi.mock('../../src/db/insertTradeRow.js', () => {
  const insertTradeRow = vi.fn();
  return {
    insertTradeRow,
    // Real implementation — webhook.ts imports this from the same module, so the
    // mock must provide it or it resolves to undefined.
    tradeKey: (r: any) => `${r.tx_signature ?? ""}|${r.asset_index}|${r.leg_index}`,
    insertTradeRows: vi.fn(async (rows: any[]) => {
      for (const r of rows) await insertTradeRow(r);
      // Mirrors the real writer: returns the dedupe key of every row written.
      return rows.map((r) => ({
        tx_signature: r.tx_signature,
        asset_index: r.asset_index,
        leg_index: r.leg_index,
      }));
    }),
  };
});

vi.mock('@percolatorct/shared', () => ({
  config: {
    allProgramIds: ['FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD'],
    webhookSecret: 'test-secret-token', // must be a literal — vi.mock is hoisted
  },
  eventBus: { publish: vi.fn() },
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
  // v18 single-fill wire (TradeNoCpi, tag=10 per the IX_TAG mock above): the
  // real SDK layout is 77 bytes with asset_index@33 (u16) and size_q@43
  // (i128) — see src/parsers/percolatorTxParser.ts's decodeV18SingleFill.
  // parseTradeSize is separately mocked below to ignore its input, so only
  // the BUFFER LENGTH matters here (must clear decodeV18SingleFill's minLen
  // gate for TradeNoCpi); the byte values at 33/43 are inert filler.
  decodeBase58: vi.fn(() => {
    const buf = new Uint8Array(77);
    buf[0] = 10; // IX_TAG.TradeNoCpi (per the mocked IX_TAG above)
    return buf;
  }),
  parseTradeSize: vi.fn(() => ({
    sizeValue: 1_000_000n,
    side: 'long' as const,
  })),
  readU128LE: vi.fn(() => 0n),
  // GH#42: withRetry added in PERC-8265 — pass-through so insertTradeRow mock is called directly
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  // captureException: no-op in tests
  captureException: vi.fn(),
  // #205: extractPrice's fresh-RPC fallback (readFreshMarkPriceE6). Defaults to
  // "no account" (price stays 0, same as pre-#205 behaviour) so every existing
  // test that doesn't care about the RPC path is unaffected; tests that DO care
  // override this mock's return value per-test.
  getConnection: vi.fn(() => ({
    getAccountInfo: vi.fn(async () => null),
  })),
}));

// #221: the fill-price reader. Default null = "not a readable v18 slot", which keeps
// every existing test on its legacy mark path; the #221 test below sets a price.
vi.mock('../../src/parsers/markPrice.js', () => ({ readAssetEffectivePriceE6: vi.fn(() => null) }));

import * as shared from '@percolatorct/shared';
import { insertTradeRow } from '../../src/db/insertTradeRow.js';
import { readAssetEffectivePriceE6 } from '../../src/parsers/markPrice.js';
import { webhookRoutes, verifyWebhookSignature } from '../../src/routes/webhook.js';
import { buildMarket, LAYOUT_V22 } from '../helpers/v22Fixtures.js';

const PROGRAM_ID = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const TRADER = 'So11111111111111111111111111111111111111112';
const SLAB = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SIG = '5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW';
const SIG2 = '4VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW';

function makeRequest(body: any, secret: string | null = TEST_WEBHOOK_SECRET): Request {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  if (secret !== null) headers['authorization'] = secret;
  return new Request('http://localhost/webhook/trades', {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  });
}

function makeBaseInstructions() {
  return [{
    programId: PROGRAM_ID,
    data: 'validbase58data',
    accounts: [TRADER, TRADER, SLAB],
  }];
}


// =====================================================================================================================
// DRAFT (J-20261120-IDX-REPORT-WEBHOOKABUSEREPLAY, report idx-webhook-abuse-replay-20261120.md, finding D1).
// Goes to tests/routes/webhook.unverified-payload.draft.test.ts. Everything above this line is the mock preamble of
// tests/routes/webhook.test.ts at 5e9ecf0, unchanged.
//
// What it pins: a delivery that passes the auth check is written to `trades` from the PAYLOAD ALONE. The route never
// asks the chain whether the signature exists or whether the instruction accounts are the ones the transaction really
// had (no getSignatureStatuses / getTransaction anywhere in src/routes/webhook.ts). Measured on the real server:
// a made-up signature wrote 1 row with 0 getTransaction calls, and a real signature sent first with a swapped trader
// kept the made-up trader for good (the genuine redelivery is ignored by upsert(ignoreDuplicates)).
//
// These tests are agnostic about the fix: the fake connection answers "unknown signature" on every lookup a fix could
// use (getSignatureStatuses, getTransaction, getParsedTransaction(s)). They only assert that no row is written from
// data the chain does not confirm. On 5e9ecf0 both fail.
// =====================================================================================================================
const FORGED_SIG = '3'.repeat(88);
const ATTACKER = 'Stake11111111111111111111111111111111111111';

function unknownToChain() {
  return {
    getAccountInfo: vi.fn(async () => null),
    getSignatureStatuses: vi.fn(async (sigs: string[]) => ({ context: { slot: 1 }, value: sigs.map(() => null) })),
    getSignatureStatus: vi.fn(async () => ({ context: { slot: 1 }, value: null })),
    getTransaction: vi.fn(async () => null),
    getParsedTransaction: vi.fn(async () => null),
    getParsedTransactions: vi.fn(async (sigs: string[]) => sigs.map(() => null)),
  };
}

describe('DRAFT D1: POST /webhook/trades writes rows from data the chain does not confirm', () => {
  let app: ReturnType<typeof webhookRoutes>;

  beforeEach(() => {
    vi.clearAllMocks();
    app = webhookRoutes({ getMarkets: () => new Map([[SLAB, {}]]) });
  });

  it('a signature the chain has never seen writes no trade row', async () => {
    vi.mocked(shared.getConnection).mockReturnValue(unknownToChain() as any);
    const res = await app.fetch(makeRequest([{
      signature: FORGED_SIG,
      instructions: makeBaseInstructions(),
      innerInstructions: [],
      accountData: [],
    }]));
    // Any status is fine (200 "ignored" or 4xx); what must not happen is a stored row.
    expect(res.status).toBeLessThan(500);
    expect(insertTradeRow).not.toHaveBeenCalled();
  });

  it('a made-up trader in the instruction accounts is not stored when the chain cannot confirm the transaction', async () => {
    vi.mocked(shared.getConnection).mockReturnValue(unknownToChain() as any);
    await app.fetch(makeRequest([{
      signature: SIG,
      instructions: [{ programId: PROGRAM_ID, data: 'validbase58data', accounts: [ATTACKER, ATTACKER, SLAB] }],
      innerInstructions: [],
      accountData: [],
    }]));
    expect(insertTradeRow).not.toHaveBeenCalledWith(expect.objectContaining({ trader: ATTACKER }));
  });
});

// =====================================================================================================================
// The fix (J-20261120-IDX-FIX-WEBHOOKROWSFROMRPC): rows come from the chain's transaction for the signature.
// =====================================================================================================================
import { PublicKey } from '@solana/web3.js';
import { resetChainTxMemo } from '../../src/lib/webhookChainTx.js';

/** The chain's view of SIG: the same TradeNoCpi, signed by TRADER. */
function chainTx(trader = TRADER): any {
  return {
    slot: 7,
    blockTime: 1_794_000_000,
    transaction: { message: { instructions: [{ programId: new PublicKey(PROGRAM_ID), accounts: [new PublicKey(trader), new PublicKey(trader), new PublicKey(SLAB)], data: 'validbase58data' }] } },
    meta: { err: null, logMessages: [], innerInstructions: [] },
  };
}

function chainKnows(map: Record<string, any>) {
  return {
    ...unknownToChain(),
    getParsedTransactions: vi.fn(async (sigs: string[]) => sigs.map((s) => map[s] ?? null)),
  };
}

describe('D1 fix: POST /webhook/trades builds rows from the chain transaction', () => {
  let app: ReturnType<typeof webhookRoutes>;

  beforeEach(() => {
    vi.clearAllMocks();
    resetChainTxMemo();
    app = webhookRoutes({ getMarkets: () => new Map([[SLAB, {}]]) });
  });

  it('a REAL signature sent with a made-up trader is stored with the chain\'s trader (report case 09)', async () => {
    vi.mocked(shared.getConnection).mockReturnValue(chainKnows({ [SIG]: chainTx(TRADER) }) as any);
    const res = await app.fetch(makeRequest([{
      signature: SIG,
      instructions: [{ programId: PROGRAM_ID, data: 'validbase58data', accounts: [ATTACKER, ATTACKER, SLAB] }],
      innerInstructions: [],
      accountData: [],
    }]));
    expect(res.status).toBe(200);
    expect(insertTradeRow).toHaveBeenCalledTimes(1);
    expect(insertTradeRow).toHaveBeenCalledWith(expect.objectContaining({ trader: TRADER, slab_address: SLAB, tx_signature: SIG }));
    expect(insertTradeRow).not.toHaveBeenCalledWith(expect.objectContaining({ trader: ATTACKER }));
  });

  it('a transaction the payload calls successful but the chain records as failed writes nothing', async () => {
    const failed = chainTx(TRADER);
    failed.meta.err = { InstructionError: [0, 'Custom'] };
    vi.mocked(shared.getConnection).mockReturnValue(chainKnows({ [SIG]: failed }) as any);
    await app.fetch(makeRequest([{ signature: SIG, transactionError: null, instructions: makeBaseInstructions(), innerInstructions: [], accountData: [] }]));
    expect(insertTradeRow).not.toHaveBeenCalled();
  });

  it('a failed chain read answers 500 and writes nothing, so Helius redelivers', async () => {
    const c = chainKnows({});
    c.getParsedTransactions.mockRejectedValue(new Error('429 Too Many Requests'));
    vi.mocked(shared.getConnection).mockReturnValue(c as any);
    const res = await app.fetch(makeRequest([{ signature: SIG, instructions: makeBaseInstructions(), innerInstructions: [], accountData: [] }]));
    expect(res.status).toBe(500);
    expect(insertTradeRow).not.toHaveBeenCalled();
  });

  it('RPC cost: a 250-fill delivery reads its transactions in 3 getParsedTransactions calls, its redelivery in 0', async () => {
    const sigs = Array.from({ length: 250 }, (_, i) => SIG.slice(0, 80) + String(10_000_000 + i).replace(/0/g, 'z').slice(-8));
    const c = chainKnows(Object.fromEntries(sigs.map((s) => [s, chainTx(TRADER)])));
    vi.mocked(shared.getConnection).mockReturnValue(c as any);
    const body = sigs.map((s) => ({ signature: s, instructions: makeBaseInstructions(), innerInstructions: [], accountData: [] }));
    expect((await app.fetch(makeRequest(body))).status).toBe(200);
    expect(c.getParsedTransactions).toHaveBeenCalledTimes(3);
    expect(c.getTransaction).not.toHaveBeenCalled();
    expect(insertTradeRow).toHaveBeenCalledTimes(250);
    expect((await app.fetch(makeRequest(body))).status).toBe(200);
    expect(c.getParsedTransactions).toHaveBeenCalledTimes(3);
  });
});
