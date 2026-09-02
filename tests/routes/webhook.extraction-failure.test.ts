import { describe, it, expect, vi, beforeEach } from 'vitest';

/**
 * GH#160 — extraction errors were swallowed and the webhook answered 200.
 *
 * `processTransactions()` caught any throw from `extractTradesFromEnhancedTx`,
 * logged a warning and continued. The function then resolved, so the route
 * returned `200 OK`. **Helius does not retry a 2xx**, so a transaction that threw
 * during extraction was permanently dropped — no insert was ever attempted, and
 * nothing downstream could tell it had happened.
 *
 * GH#42 fixed the same class on the far side of the insert (retry, then 500).
 * This is the near side.
 *
 * The insert is deliberately still attempted first: whatever WAS extracted is
 * written before the throw, and `insertTradeRows` upserts with ignoreDuplicates,
 * so the redelivery re-inserts nothing.
 */

const TEST_WEBHOOK_SECRET = 'test-secret-token';
const PROGRAM_ID = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const TRADER = 'So11111111111111111111111111111111111111112';
const SLAB = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const SIG = '5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW';

vi.mock('@percolatorct/sdk', () => ({
  IX_TAG: { TradeNoCpi: 10, TradeCpi: 11, PermissionlessCrank: 5, BatchTradeNoCpi: 66, BatchTradeCpi: 67 },
  detectSlabLayout: vi.fn(() => ({ version: 1, engineOff: 640, engineMarkPriceOff: 400, engineBitmapOff: 656 })),
  isV17Account: vi.fn(() => false),
  parseWrapperConfigV17: vi.fn(),
  V17_HEADER_LEN: 16,
}));

vi.mock('../../src/db/insertTradeRow.js', () => {
  const insertTradeRow = vi.fn();
  return {
    insertTradeRow,
    tradeKey: (r: any) => `${r.tx_signature ?? ""}|${r.asset_index}|${r.leg_index}`,
    insertTradeRows: vi.fn(async (rows: any[]) => {
      for (const r of rows) await insertTradeRow(r);
      return rows.map((r) => ({ tx_signature: r.tx_signature, asset_index: r.asset_index, leg_index: r.leg_index }));
    }),
  };
});

vi.mock('@percolatorct/shared', () => ({
  config: { allProgramIds: ['FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD'], webhookSecret: 'test-secret-token' },
  eventBus: { publish: vi.fn() },
  createLogger: vi.fn(() => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() })),
  decodeBase58: vi.fn(),
  parseTradeSize: vi.fn(() => ({ sizeValue: 1_000_000n, side: 'long' as const })),
  readU128LE: vi.fn(() => 0n),
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
  captureException: vi.fn(),
}));

import * as shared from '@percolatorct/shared';
import { insertTradeRow } from '../../src/db/insertTradeRow.js';
import { webhookRoutes } from '../../src/routes/webhook.js';

function makeRequest(body: any): Request {
  return new Request('http://localhost/webhook/trades', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', authorization: TEST_WEBHOOK_SECRET },
    body: JSON.stringify(body),
  });
}

function tradeBytes(): Uint8Array {
  const b = new Uint8Array(21);
  b[0] = 10; b[5] = 0x40; b[6] = 0x42; b[7] = 0x0f;
  return b;
}

const ix = (data: string) => ({ programId: PROGRAM_ID, data, accounts: [TRADER, TRADER, SLAB] });

describe('webhook extraction failures are reported, not swallowed (GH#160)', () => {
  let app: ReturnType<typeof webhookRoutes>;

  beforeEach(() => {
    vi.clearAllMocks();
    app = webhookRoutes({ getMarkets: () => new Map([[SLAB, {}]]) });
  });

  it('returns 500 when extraction throws, so Helius redelivers', async () => {
    // decodeBase58 is called during extraction; make it throw the way a malformed
    // payload would.
    vi.mocked(shared.decodeBase58).mockImplementation(() => {
      throw new Error('boom: unparseable instruction data');
    });

    const res = await app.fetch(makeRequest([{
      signature: SIG, instructions: [ix('bad')], innerInstructions: [], accountData: [], logs: [],
    }]));

    // Before the fix this was 200 — the delivery was acknowledged and the
    // transaction was never seen again.
    expect(res.status).toBe(500);
  });

  it('still writes the trades it DID extract before reporting the failure', async () => {
    // First transaction parses, second throws. The good fill must be durable —
    // the redelivery is idempotent, so failing before the insert would discard
    // real data to report a bad sibling.
    vi.mocked(shared.decodeBase58)
      .mockImplementationOnce(() => tradeBytes())
      .mockImplementationOnce(() => { throw new Error('boom'); });

    const res = await app.fetch(makeRequest([
      { signature: SIG, instructions: [ix('good')], innerInstructions: [], accountData: [], logs: [] },
      { signature: SIG.replace('5V', '4V'), instructions: [ix('bad')], innerInstructions: [], accountData: [], logs: [] },
    ]));

    expect(res.status).toBe(500);
    expect(vi.mocked(insertTradeRow).mock.calls.length).toBeGreaterThan(0);
  });

  it('reports the failure to Sentry with the signature', async () => {
    vi.mocked(shared.decodeBase58).mockImplementation(() => { throw new Error('boom'); });

    await app.fetch(makeRequest([{
      signature: SIG, instructions: [ix('bad')], innerInstructions: [], accountData: [], logs: [],
    }]));

    const calls = vi.mocked(shared.captureException).mock.calls;
    expect(calls.length).toBeGreaterThan(0);
    expect(JSON.stringify(calls)).toContain('webhook-extraction-failure');
  });

  it('still returns 200 when nothing fails — the happy path is unchanged', async () => {
    vi.mocked(shared.decodeBase58).mockReturnValue(tradeBytes());

    const res = await app.fetch(makeRequest([{
      signature: SIG, instructions: [ix('good')], innerInstructions: [], accountData: [], logs: [],
    }]));

    expect(res.status).toBe(200);
  });
});
