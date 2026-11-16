import { describe, it, expect, vi, beforeEach } from 'vitest';

/**
 * Route-level branch coverage for src/routes/webhook.ts: body-size gates (header and
 * streamed), payload validation at every nesting level, delivery caps, partial write
 * failure, blocked slabs, v22 side-path isolation, and the per-transaction skip rules.
 */

const SECRET = 'test-secret-token';
const PROGRAM_ID = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const TRADER = 'So11111111111111111111111111111111111111112';
const SLAB = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const BLOCKED_SLAB = '7FBXdrm1vQ4ktQJjMwurq4cAHkVB1gKoZ7Hx3CAQv6P4';
const SIG = '5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW';
const SIG2 = '4VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW';

vi.mock('@percolatorct/sdk', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@percolatorct/sdk')>()),
  IX_TAG: { TradeNoCpi: 10, TradeCpi: 11, PermissionlessCrank: 5, RebalanceReduce: 44, BatchTradeNoCpi: 66, BatchTradeCpi: 67 },
  detectSlabLayout: vi.fn(() => ({ version: 1, engineOff: 640, engineMarkPriceOff: 400, engineBitmapOff: 656 })),
  isV17Account: vi.fn(() => false),
  parseWrapperConfigV17: vi.fn(),
  V17_HEADER_LEN: 16,
}));

vi.mock('../../src/db/insertTradeRow.js', () => ({
  insertTradeRow: vi.fn(),
  tradeKey: (r: any) => `${r.tx_signature ?? ''}|${r.asset_index}|${r.leg_index}`,
  insertTradeRows: vi.fn(async (rows: any[]) =>
    rows.map((r) => ({ tx_signature: r.tx_signature, asset_index: r.asset_index, leg_index: r.leg_index }))),
}));

vi.mock('../../src/blocklist.js', () => ({
  isBlockedSlab: (s: string) => s === '7FBXdrm1vQ4ktQJjMwurq4cAHkVB1gKoZ7Hx3CAQv6P4',
}));

vi.mock('../../src/lib/skippedSignatures.js', () => ({ recordSkippedSignatures: vi.fn(async () => {}) }));
vi.mock('../../src/db/traderNetPosition.js', () => ({ resolveRebalanceReduce: vi.fn(async () => ({ ok: false, reason: 'no position' })) }));
vi.mock('../../src/db/insertV22Events.js', () => ({ insertV22Events: vi.fn(async () => {}) }));
vi.mock('../../src/services/v22LogEvents.js', () => ({ recordV22LogEvents: vi.fn(async () => {}) }));
vi.mock('../../src/parsers/v22Events.js', async (orig) => ({
  ...(await orig<typeof import('../../src/parsers/v22Events.js')>()),
  decodeV22Events: vi.fn(() => []),
}));

vi.mock('@percolatorct/shared', () => ({
  config: { allProgramIds: ['FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD'], webhookSecret: 'test-secret-token' },
  eventBus: { publish: vi.fn() },
  createLogger: vi.fn(() => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() })),
  decodeBase58: vi.fn(),
  parseTradeSize: vi.fn(() => ({ sizeValue: 1_000_000n, side: 'long' as const })),
  readU128LE: vi.fn(() => 0n),
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  captureException: vi.fn(),
  getConnection: vi.fn(() => ({ getAccountInfo: vi.fn(async () => null) })),
}));

import * as shared from '@percolatorct/shared';
import { insertTradeRows } from '../../src/db/insertTradeRow.js';
import { insertV22Events } from '../../src/db/insertV22Events.js';
import { recordV22LogEvents } from '../../src/services/v22LogEvents.js';
import { decodeV22Events } from '../../src/parsers/v22Events.js';
import { webhookRoutes } from '../../src/routes/webhook.js';

function tradeBytes(): Uint8Array {
  const b = new Uint8Array(77);
  b[0] = 10; b[43] = 0x40; b[44] = 0x42; b[45] = 0x0f;
  return b;
}

const ix = (accounts: string[] = [TRADER, TRADER, SLAB]) => ({ programId: PROGRAM_ID, data: 'x', accounts });
const tx = (extra: Record<string, unknown> = {}) => ({ signature: SIG, instructions: [ix()], ...extra });

function post(body: unknown, headers: Record<string, string> = {}, raw?: BodyInit): Request {
  return new Request('http://localhost/webhook/trades', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', authorization: SECRET, ...headers },
    body: raw ?? JSON.stringify(body),
  });
}

let app: ReturnType<typeof webhookRoutes>;
const markets = (...slabs: string[]) => ({ getMarkets: () => new Map(slabs.map((s) => [s, {}])) });

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(shared.decodeBase58).mockReturnValue(tradeBytes());
  vi.mocked(shared.withRetry).mockImplementation(async (fn: () => Promise<unknown>) => fn() as any);
  app = webhookRoutes(markets(SLAB));
});

describe('webhook route — body size gates', () => {
  it('413 when Content-Length exceeds the cap (before reading the body)', async () => {
    const res = await app.fetch(post([], { 'content-length': String(11 * 1024 * 1024) }));
    expect(res.status).toBe(413);
  });

  it('413 when the streamed body exceeds the cap despite a small Content-Length', async () => {
    const chunk = new Uint8Array(6 * 1024 * 1024);
    let sent = 0;
    const stream = new ReadableStream<Uint8Array>({
      pull(ctl) {
        if (sent++ < 3) ctl.enqueue(chunk); else ctl.close();
      },
    });
    const req = new Request('http://localhost/webhook/trades', {
      method: 'POST',
      headers: { authorization: SECRET, 'content-length': '10' },
      body: stream,
      // @ts-expect-error node fetch requires duplex for stream bodies
      duplex: 'half',
    });
    const res = await app.fetch(req);
    expect(res.status).toBe(413);
  });

  it('400 when the body stream errors while being read', async () => {
    const stream = new ReadableStream<Uint8Array>({ pull(ctl) { ctl.error(new Error('socket reset')); } });
    const req = new Request('http://localhost/webhook/trades', {
      method: 'POST',
      headers: { authorization: SECRET },
      body: stream,
      // @ts-expect-error node fetch requires duplex for stream bodies
      duplex: 'half',
    });
    const res = await app.fetch(req);
    expect(res.status).toBe(400);
  });

  it('a request with no body at all is treated as empty: auth passes, then 400 invalid JSON', async () => {
    const res = await app.fetch(new Request('http://localhost/webhook/trades', { method: 'POST', headers: { authorization: SECRET } }));
    expect(res.status).toBe(400);
  });

  it('401 on an empty body without credentials (empty body is authenticated like any other)', async () => {
    const res = await app.fetch(new Request('http://localhost/webhook/trades', { method: 'POST' }));
    expect(res.status).toBe(401);
  });
});

describe('webhook route — delivery shape', () => {
  it('400 when the delivery carries more than 500 transactions', async () => {
    const res = await app.fetch(post(Array.from({ length: 501 }, () => ({}))));
    expect(res.status).toBe(400);
    expect(insertTradeRows).not.toHaveBeenCalled();
  });

  it('accepts exactly 500 transactions', async () => {
    const res = await app.fetch(post(Array.from({ length: 500 }, () => ({}))));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ received: 500 });
  });

  it('a single (non-array) transaction object is wrapped and processed', async () => {
    const res = await app.fetch(post(tx()));
    expect(res.status).toBe(200);
    expect(insertTradeRows).toHaveBeenCalledTimes(1);
  });

  it('a JSON scalar is rejected as an invalid transaction format', async () => {
    expect((await app.fetch(post(null, {}, 'null'))).status).toBe(400);
    expect((await app.fetch(post(7, {}, '7'))).status).toBe(400);
  });
});

describe('webhook route — payload validation at every nesting level', () => {
  const bad: [string, unknown][] = [
    ['tx.instructions not an array', { instructions: 'x' }],
    ['instruction is null', { instructions: [null] }],
    ['instruction is a primitive', { instructions: [5] }],
    ['instruction accounts not an array', { instructions: [{ accounts: 'a' }] }],
    ['inner-of-instruction not an array', { instructions: [{ innerInstructions: 'x' }] }],
    ['inner-of-instruction entry null', { instructions: [{ innerInstructions: [null] }] }],
    ['inner-of-instruction poison key', { instructions: [{ innerInstructions: [JSON.parse('{"__proto__":{}}')] }] }],
    ['inner-of-instruction programId type', { instructions: [{ innerInstructions: [{ programId: 1 }] }] }],
    ['inner-of-instruction data type', { instructions: [{ innerInstructions: [{ data: 1 }] }] }],
    ['tx.innerInstructions not an array', { innerInstructions: 'x' }],
    ['inner group null', { innerInstructions: [null] }],
    ['inner group poison key', { innerInstructions: [JSON.parse('{"constructor":1}')] }],
    ['inner group instructions not an array', { innerInstructions: [{ instructions: 'x' }] }],
    ['inner group instruction null', { innerInstructions: [{ instructions: [null] }] }],
    ['inner group instruction poison key', { innerInstructions: [{ instructions: [JSON.parse('{"prototype":1}')] }] }],
    ['inner group instruction programId type', { innerInstructions: [{ instructions: [{ programId: 1 }] }] }],
    ['inner group instruction data type', { innerInstructions: [{ instructions: [{ data: 1 }] }] }],
    ['inner group instruction accounts not an array', { innerInstructions: [{ instructions: [{ accounts: 'a' }] }] }],
    ['inner group instruction account not a string', { innerInstructions: [{ instructions: [{ accounts: [1] }] }] }],
    ['accountData not an array', { accountData: 'x' }],
    ['accountData entry null', { accountData: [null] }],
    ['tx is null', null],
  ];
  for (const [name, body] of bad) {
    it(`400: ${name}`, async () => {
      const res = await app.fetch(post([body]));
      expect(res.status).toBe(400);
      expect(insertTradeRows).not.toHaveBeenCalled();
    });
  }

  it('accepts well-formed nested inner instructions and inner groups', async () => {
    const res = await app.fetch(post([{
      signature: SIG,
      instructions: [{ programId: 'other', data: 'x', accounts: [TRADER], innerInstructions: [{ programId: 'p', data: 'd' }] }],
      innerInstructions: [{ instructions: [{ programId: 'p', data: 'd', accounts: [TRADER] }] }, {}],
      accountData: [{ account: SLAB }],
    }]));
    expect(res.status).toBe(200);
  });
});

describe('webhook route — write path', () => {
  it('500 and a Sentry report when the batch insert fails after retries; nothing is published', async () => {
    vi.mocked(insertTradeRows).mockRejectedValueOnce(new Error('db down'));
    const res = await app.fetch(post([tx()]));
    expect(res.status).toBe(500);
    expect(shared.captureException).toHaveBeenCalledTimes(1);
    expect(shared.eventBus.publish).not.toHaveBeenCalled();
  });

  it('a non-Error rejection is still reported and answers 500', async () => {
    vi.mocked(insertTradeRows).mockRejectedValueOnce('string failure');
    const res = await app.fetch(post([tx()]));
    expect(res.status).toBe(500);
    expect(shared.captureException).toHaveBeenCalledTimes(1);
  });

  it('publishes only the keys the writer reports as written (already-stored legs stay silent)', async () => {
    vi.mocked(insertTradeRows).mockResolvedValueOnce([]);
    const res = await app.fetch(post([tx()]));
    expect(res.status).toBe(200);
    expect(shared.eventBus.publish).not.toHaveBeenCalled();
  });

  it('publishes trade.executed for a written fill', async () => {
    const res = await app.fetch(post([tx()]));
    expect(res.status).toBe(200);
    expect(shared.eventBus.publish).toHaveBeenCalledWith('trade.executed', SLAB, expect.objectContaining({ signature: SIG }));
  });

  it('fills on a blocked slab are skipped, other fills in the delivery are still written', async () => {
    app = webhookRoutes(markets(SLAB, BLOCKED_SLAB));
    const res = await app.fetch(post([
      tx({ instructions: [ix([TRADER, TRADER, BLOCKED_SLAB])] }),
      tx({ signature: SIG2 }),
    ]));
    expect(res.status).toBe(200);
    const rows = vi.mocked(insertTradeRows).mock.calls.flatMap((c) => c[0] as any[]);
    expect(rows.map((r) => r.slab_address)).toEqual([SLAB]);
  });

  it('transactions are processed in ascending slot order (no slot sorts last)', async () => {
    const order: string[] = [];
    vi.mocked(shared.decodeBase58).mockImplementation(() => tradeBytes());
    vi.mocked(insertTradeRows).mockImplementationOnce(async (rows: any[]) => {
      order.push(...rows.map((r) => r.tx_signature));
      return rows.map((r) => ({ tx_signature: r.tx_signature, asset_index: r.asset_index, leg_index: r.leg_index }));
    });
    const SIG3 = '3VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW';
    await app.fetch(post([tx({ signature: SIG3 }), tx({ signature: SIG, slot: 9 }), tx({ signature: SIG2, slot: 5 })]));
    expect(order).toEqual([SIG2, SIG, SIG3]);
  });
});

describe('webhook route — per-transaction skip rules', () => {
  async function rowsFor(body: unknown, a = app): Promise<any[]> {
    const res = await a.fetch(post(body));
    expect(res.status).toBe(200);
    return vi.mocked(insertTradeRows).mock.calls.flatMap((c) => c[0] as any[]);
  }

  it('a transaction without a signature is ignored', async () => {
    expect(await rowsFor([{ instructions: [ix()] }])).toEqual([]);
  });

  it('a failed transaction (transactionError set) is not indexed', async () => {
    expect(await rowsFor([tx({ transactionError: { err: 1 } })])).toEqual([]);
  });

  it('a malformed signature is not indexed', async () => {
    expect(await rowsFor([tx({ signature: 'short' })])).toEqual([]);
  });

  it('instructions of other programs are ignored', async () => {
    expect(await rowsFor([tx({ instructions: [{ programId: 'Other', data: 'x', accounts: [TRADER, TRADER, SLAB] }] })])).toEqual([]);
  });

  it('an instruction with no data, or data shorter than 2 bytes, is ignored', async () => {
    expect(await rowsFor([tx({ instructions: [{ programId: PROGRAM_ID, accounts: [TRADER, TRADER, SLAB] }] })])).toEqual([]);
    vi.mocked(shared.decodeBase58).mockReturnValue(new Uint8Array(1));
    expect(await rowsFor([tx()])).toEqual([]);
  });

  it('an instruction with an unknown tag is ignored', async () => {
    const b = tradeBytes(); b[0] = 99;
    vi.mocked(shared.decodeBase58).mockReturnValue(b);
    expect(await rowsFor([tx()])).toEqual([]);
  });

  it('a trade tag with too few account keys, or an empty trader, is ignored', async () => {
    expect(await rowsFor([tx({ instructions: [ix([TRADER, TRADER])] })])).toEqual([]);
    expect(await rowsFor([tx({ instructions: [ix(['', TRADER, SLAB])] })])).toEqual([]);
  });

  it('a trade with non-base58 pubkeys is ignored', async () => {
    expect(await rowsFor([tx({ instructions: [ix(['not a key!', TRADER, SLAB])] })])).toEqual([]);
  });

  it('a trade on an untracked slab is not written', async () => {
    expect(await rowsFor([tx({ instructions: [ix([TRADER, TRADER, TRADER])] })])).toEqual([]);
  });

  it('without a discovery service every well-formed slab is accepted', async () => {
    const rows = await rowsFor([tx()], webhookRoutes());
    expect(rows).toHaveLength(1);
  });

  it('two identical fills in one tx are numbered 0 and 1', async () => {
    const rows = await rowsFor([tx({ instructions: [ix(), ix()] })]);
    expect(rows.map((r) => r.leg_index)).toEqual([0, 1]);
  });
});

describe('webhook route — v22 side path is isolated from trades', () => {
  const v22Row = (slab: string | null) => ({ slab_address: slab, signature: SIG }) as any;

  it('v22 rows for known markets are inserted; blocked/untracked slabs are filtered out', async () => {
    vi.mocked(decodeV22Events).mockReturnValue([v22Row(SLAB), v22Row(BLOCKED_SLAB), v22Row(TRADER), v22Row(null)]);
    const res = await app.fetch(post([tx({ slot: 5, timestamp: 100 })]));
    expect(res.status).toBe(200);
    const rows = vi.mocked(insertV22Events).mock.calls[0][0] as any[];
    expect(rows.map((r) => r.slab_address)).toEqual([SLAB, null]);
  });

  it('with no discovery service, untracked slabs are kept', async () => {
    vi.mocked(decodeV22Events).mockReturnValue([v22Row(TRADER)]);
    await webhookRoutes().fetch(post([tx()]));
    expect(vi.mocked(insertV22Events).mock.calls[0][0]).toHaveLength(1);
  });

  it('non-numeric slot/timestamp decode with null context', async () => {
    await app.fetch(post([tx({ slot: 'x', timestamp: 'y' })]));
    expect(vi.mocked(decodeV22Events).mock.calls[0][1]).toMatchObject({ slot: null, blockTimeSec: null });
  });

  it('a v22 insert failure never changes the response (trades unaffected)', async () => {
    vi.mocked(decodeV22Events).mockReturnValue([v22Row(SLAB)]);
    vi.mocked(insertV22Events).mockRejectedValueOnce(new Error('v22 down'));
    const res = await app.fetch(post([tx()]));
    expect(res.status).toBe(200);
    expect(insertTradeRows).toHaveBeenCalledTimes(1);
  });

  it('a non-Error v22 failure is also swallowed', async () => {
    vi.mocked(decodeV22Events).mockImplementationOnce(() => { throw 'weird'; });
    expect((await app.fetch(post([tx()]))).status).toBe(200);
  });

  it('wrapper log events are recorded only when the delivery carries logMessages', async () => {
    await app.fetch(post([tx({ slot: 7, timestamp: 9, logMessages: ['Program log: x'] }), tx({ signature: SIG2 }), { instructions: [], logMessages: [] }]));
    expect(recordV22LogEvents).toHaveBeenCalledTimes(1);
    const arg = vi.mocked(recordV22LogEvents).mock.calls[0][0] as any;
    expect(arg).toMatchObject({ signature: SIG, slot: 7, blockTimeSec: 9, err: null });
    expect(arg.isKnownMarket(SLAB)).toBe(true);
    expect(arg.isKnownMarket(TRADER)).toBe(false);
  });

  it('log events: no discovery means every market is known; non-numeric slot is null', async () => {
    await webhookRoutes().fetch(post([tx({ slot: 'x', timestamp: 'y', logMessages: [], transactionError: { e: 1 } })]));
    const arg = vi.mocked(recordV22LogEvents).mock.calls[0][0] as any;
    expect(arg).toMatchObject({ slot: null, blockTimeSec: null, err: { e: 1 } });
    expect(arg.isKnownMarket('anything')).toBe(true);
  });
});

describe('webhook route — extraction failures and RebalanceReduce (tag 44) holds', () => {
  const reduceBytes = (): Uint8Array => {
    const b = new Uint8Array(35);
    b[0] = 44; b[19] = 5; // reduce_q = 5
    return b;
  };
  const reduceIx = (accounts: string[]) => ({ programId: PROGRAM_ID, data: 'r', accounts });

  it('a non-Error extraction throw still answers 500 and is reported', async () => {
    vi.mocked(shared.decodeBase58).mockImplementation(() => { throw 'weird'; });
    const res = await app.fetch(post([tx()]));
    expect(res.status).toBe(500);
    expect(shared.captureException).toHaveBeenCalledTimes(1);
  });

  it('a tag 44 with undecodable (zero) reduce_q is skipped', async () => {
    vi.mocked(shared.decodeBase58).mockReturnValue(new Uint8Array(35).fill(0).map((_, i) => (i === 0 ? 44 : 0)));
    const res = await app.fetch(post([tx({ instructions: [reduceIx([TRADER, SLAB])] })]));
    expect(res.status).toBe(200);
    expect(insertTradeRows).not.toHaveBeenCalled();
  });

  it('a tag 44 with an invalid account key is held, not written', async () => {
    vi.mocked(shared.decodeBase58).mockReturnValue(reduceBytes());
    const res = await app.fetch(post([tx({ instructions: [reduceIx(['bad key', SLAB])] })]));
    expect(res.status).toBe(200);
    expect(insertTradeRows).not.toHaveBeenCalled();
  });

  it('a tag 44 on an untracked market is held, not written', async () => {
    vi.mocked(shared.decodeBase58).mockReturnValue(reduceBytes());
    const res = await app.fetch(post([tx({ instructions: [reduceIx([TRADER, TRADER])] })]));
    expect(res.status).toBe(200);
    expect(insertTradeRows).not.toHaveBeenCalled();
  });

  it('a tag 44 that cannot be resolved is recorded as skipped and holds its leg number', async () => {
    const { recordSkippedSignatures } = await import('../../src/lib/skippedSignatures.js');
    vi.mocked(shared.decodeBase58).mockReturnValue(reduceBytes());
    const res = await app.fetch(post([tx({ instructions: [reduceIx([TRADER, SLAB])] })]));
    expect(res.status).toBe(200);
    expect(recordSkippedSignatures).toHaveBeenCalledTimes(1);
    expect(insertTradeRows).not.toHaveBeenCalled();
  });

  it('a tag 44 whose peer transaction (same slot) touches the same trader+market is not resolved', async () => {
    const { resolveRebalanceReduce } = await import('../../src/db/traderNetPosition.js');
    vi.mocked(shared.decodeBase58).mockReturnValue(reduceBytes());
    const res = await app.fetch(post([
      tx({ slot: 3, instructions: [reduceIx([TRADER, SLAB])] }),
      tx({ signature: SIG2, slot: 3, instructions: [ix([TRADER, SLAB])] }),
    ]));
    expect(res.status).toBe(200);
    expect(resolveRebalanceReduce).not.toHaveBeenCalled();
  });

  it('a resolved tag 44 is written as a fee-free fill', async () => {
    const { resolveRebalanceReduce } = await import('../../src/db/traderNetPosition.js');
    vi.mocked(resolveRebalanceReduce).mockResolvedValueOnce({ ok: true, side: 'long', sizeValue: 5n } as any);
    vi.mocked(shared.decodeBase58).mockReturnValue(reduceBytes());
    const res = await app.fetch(post([tx({ instructions: [reduceIx([TRADER, SLAB])] })]));
    expect(res.status).toBe(200);
    const rows = vi.mocked(insertTradeRows).mock.calls.flatMap((c) => c[0] as any[]);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ size: '5', side: 'long', fee: 0, reduce: true });
  });
});
