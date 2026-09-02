import { describe, it, expect, vi, beforeEach } from 'vitest';

/**
 * GH#195 item 1 — cross-path `leg_index` divergence double-counts fills.
 *
 * The dedup key is `(tx_signature, asset_index, leg_index)` and is SHARED by all
 * three ingestion paths. TradeIndexer and EventStreamService both number fills
 * 0-based and offset liquidation markers by `1000 + i`. The webhook path used to
 * renumber the whole collected array by position, with markers interleaved among
 * the fills — so a tx carrying a marker followed by a real fill gave that fill
 * leg_index 1 here and leg_index 0 there. Different key, `ignoreDuplicates` never
 * collapses them, and the fill is stored twice — inflating volume_24h_by_slab,
 * trade counts and candle volume.
 *
 * Attacker-constructible: bundle a trade with a liquidation crank in one tx.
 *
 * Single-fill, no-marker txs agreed on all three paths (leg_index 0), which is why
 * this survived — only MIXED txs diverge, so this test builds a mixed one.
 */

const TEST_WEBHOOK_SECRET = 'test-secret-token';
const PROGRAM_ID = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const TRADER = 'So11111111111111111111111111111111111111112';
const SLAB = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const PORTFOLIO = '4VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzr';
const SIG = '5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW';

// Unlike webhook.test.ts, this mock DOES expose PermissionlessCrank — parseLiquidation
// early-outs on `tag !== IX_TAG.PermissionlessCrank`, so without it no marker can ever
// be produced and the mixed-tx case is untestable.
vi.mock('@percolatorct/sdk', () => ({
  IX_TAG: {
    TradeNoCpi: 10,
    TradeCpi: 11,
    PermissionlessCrank: 5,
    BatchTradeNoCpi: 66,
    BatchTradeCpi: 67,
  },
  detectSlabLayout: vi.fn(() => ({
    version: 1, engineOff: 640, engineMarkPriceOff: 400, engineBitmapOff: 656,
  })),
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
      return rows.map((r) => ({
        tx_signature: r.tx_signature, asset_index: r.asset_index, leg_index: r.leg_index,
      }));
    }),
  };
});

vi.mock('@percolatorct/shared', () => ({
  config: {
    allProgramIds: ['FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD'],
    webhookSecret: 'test-secret-token',
  },
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

// A liquidation crank: tag 5, action 1 (Liquidate), asset_index u16 LE.
function liquidationBytes(assetIndex: number): Uint8Array {
  const b = new Uint8Array(8);
  b[0] = 5; b[1] = 1;
  b[2] = assetIndex & 0xff; b[3] = (assetIndex >> 8) & 0xff;
  return b;
}

// An ordinary single-leg fill.
function tradeBytes(): Uint8Array {
  const b = new Uint8Array(21);
  b[0] = 10; b[5] = 0x40; b[6] = 0x42; b[7] = 0x0f;
  return b;
}

describe('webhook leg_index parity with TradeIndexer / EventStreamService (GH#195)', () => {
  let app: ReturnType<typeof webhookRoutes>;

  beforeEach(() => {
    vi.clearAllMocks();
    app = webhookRoutes({ getMarkets: () => new Map([[SLAB, {}]]) });
  });

  it('numbers a marker 1000+ and the fill AFTER it 0 — not 0 and 1', async () => {
    // The exact shape the report calls attacker-constructible: a liquidation crank
    // bundled ahead of a real trade in one transaction.
    const mockDecode = vi.mocked(shared.decodeBase58);
    mockDecode.mockReturnValueOnce(liquidationBytes(0)).mockReturnValueOnce(tradeBytes());

    await app.fetch(makeRequest([{
      signature: SIG,
      instructions: [
        { programId: PROGRAM_ID, data: 'liq', accounts: [TRADER, SLAB, PORTFOLIO] },
        { programId: PROGRAM_ID, data: 'trade', accounts: [TRADER, TRADER, SLAB] },
      ],
      innerInstructions: [], accountData: [], logs: [],
    }]));

    const rows = vi.mocked(insertTradeRow).mock.calls.map((c) => c[0] as any);
    const marker = rows.find((r) => r.is_liquidation);
    const fill = rows.find((r) => !r.is_liquidation);

    expect(marker).toBeDefined();
    expect(fill).toBeDefined();

    // The fill is numbered as if the marker were not there. Under the old
    // position-based renumbering this was 1, which is the whole defect: the
    // poll/backfill paths give the same fill 0, so the dedup key differs and the
    // row is stored twice.
    expect(fill.leg_index).toBe(0);

    // Markers live in their own 1000+ range on all three paths.
    expect(marker.leg_index).toBeGreaterThanOrEqual(1000);
  });

  it('numbers markers and fills in independent sequences when interleaved', async () => {
    const mockDecode = vi.mocked(shared.decodeBase58);
    mockDecode
      .mockReturnValueOnce(tradeBytes())
      .mockReturnValueOnce(liquidationBytes(0))
      .mockReturnValueOnce(tradeBytes())
      .mockReturnValueOnce(liquidationBytes(0))
      .mockReturnValueOnce(tradeBytes());

    await app.fetch(makeRequest([{
      signature: SIG,
      instructions: [
        { programId: PROGRAM_ID, data: 't1', accounts: [TRADER, TRADER, SLAB] },
        { programId: PROGRAM_ID, data: 'l1', accounts: [TRADER, SLAB, PORTFOLIO] },
        { programId: PROGRAM_ID, data: 't2', accounts: [TRADER, TRADER, SLAB] },
        { programId: PROGRAM_ID, data: 'l2', accounts: [TRADER, SLAB, PORTFOLIO] },
        { programId: PROGRAM_ID, data: 't3', accounts: [TRADER, TRADER, SLAB] },
      ],
      innerInstructions: [], accountData: [], logs: [],
    }]));

    const rows = vi.mocked(insertTradeRow).mock.calls.map((c) => c[0] as any);
    const fills = rows.filter((r) => !r.is_liquidation).map((r) => r.leg_index);
    const markers = rows.filter((r) => r.is_liquidation).map((r) => r.leg_index);

    // Fills are contiguous from 0 regardless of how many markers sit between them.
    expect(fills).toEqual([0, 1, 2]);
    expect(markers).toEqual([1000, 1001]);
  });

  it('leaves the no-marker case alone — fills still number 0,1,2', async () => {
    // Regression guard: the common path agreed across all three ingesters before
    // this change and must still agree after it.
    const mockDecode = vi.mocked(shared.decodeBase58);
    mockDecode.mockReturnValue(tradeBytes());

    await app.fetch(makeRequest([{
      signature: SIG,
      instructions: [
        { programId: PROGRAM_ID, data: 't1', accounts: [TRADER, TRADER, SLAB] },
        { programId: PROGRAM_ID, data: 't2', accounts: [TRADER, TRADER, SLAB] },
        { programId: PROGRAM_ID, data: 't3', accounts: [TRADER, TRADER, SLAB] },
      ],
      innerInstructions: [], accountData: [], logs: [],
    }]));

    const rows = vi.mocked(insertTradeRow).mock.calls.map((c) => c[0] as any);
    expect(rows.filter((r) => !r.is_liquidation).map((r) => r.leg_index)).toEqual([0, 1, 2]);
  });
});
