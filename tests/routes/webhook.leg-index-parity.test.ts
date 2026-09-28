import { describe, it, expect, vi, beforeEach } from 'vitest';

/**
 * GH#195 item 1 — cross-path `leg_index` divergence double-counts fills.
 *
 * The dedup key is `(tx_signature, asset_index, leg_index)` and is SHARED by all
 * three ingestion paths. TradeIndexer and EventStreamService both number fills
 * 0-based. The webhook path used to renumber the whole collected array by position,
 * so a non-fill instruction (a crank) sitting ahead of a real fill gave that fill
 * leg_index 1 here and 0 there. Different key, `ignoreDuplicates` never collapses
 * them, and the fill is stored twice — inflating volume_24h_by_slab, trade counts
 * and candle volume. The fix numbers fills in their own sequence, so a skipped
 * instruction between fills does not shift them.
 *
 * v18 note: PermissionlessCrank (tag 5) is `nowSlot + observations` with no action
 * byte, and there is no other instruction-level liquidation signal, so cranks yield
 * NO rows (see src/parsers/liquidations.ts). The divergence risk this guards is
 * therefore just "a crank interleaved among fills must not shift the fills" — which
 * this test builds and pins. (Historically the crank produced a `1000+`-offset
 * liquidation marker; those no longer exist under v18.)
 */

const TEST_WEBHOOK_SECRET = 'test-secret-token';
const PROGRAM_ID = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const TRADER = 'So11111111111111111111111111111111111111112';
const SLAB = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
const PORTFOLIO = '4VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzr';
const SIG = '5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW';

// The mock DOES expose PermissionlessCrank so the crank instruction is recognised
// as a crank (and correctly yields no row) rather than an unknown tag.
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

// A v18 PermissionlessCrank: tag 5 + nowSlot(u64) + n_obs(u8). No action byte, so
// it yields no row. nowSlot low byte 0x01 is the exact v17 false-positive shape.
function crankBytes(): Uint8Array {
  const b = new Uint8Array(10);
  b[0] = 5; b[1] = 1; // b[1] is nowSlot's low byte, NOT an action
  return b;
}

// An ordinary single-leg fill — v18 TradeNoCpi (77B; asset_index@33, size_q@43;
// see src/parsers/percolatorTxParser.ts's decodeV18SingleFill).
function tradeBytes(): Uint8Array {
  const b = new Uint8Array(77);
  b[0] = 10; b[43] = 0x40; b[44] = 0x42; b[45] = 0x0f;
  return b;
}

describe('webhook leg_index parity with TradeIndexer / EventStreamService (GH#195)', () => {
  let app: ReturnType<typeof webhookRoutes>;

  beforeEach(() => {
    vi.clearAllMocks();
    app = webhookRoutes({ getMarkets: () => new Map([[SLAB, {}]]) });
  });

  it('a crank bundled ahead of a trade yields no row and the fill still numbers 0', async () => {
    // The exact shape the report calls attacker-constructible: a crank bundled
    // ahead of a real trade in one transaction. Under v18 the crank is not a
    // liquidation (no marker), and it must not shift the fill off leg_index 0.
    const mockDecode = vi.mocked(shared.decodeBase58);
    mockDecode.mockReturnValueOnce(crankBytes()).mockReturnValueOnce(tradeBytes());

    await app.fetch(makeRequest([{
      signature: SIG,
      instructions: [
        { programId: PROGRAM_ID, data: 'crank', accounts: [TRADER, SLAB, PORTFOLIO] },
        { programId: PROGRAM_ID, data: 'trade', accounts: [TRADER, TRADER, SLAB] },
      ],
      innerInstructions: [], accountData: [], logs: [],
    }]));

    const rows = vi.mocked(insertTradeRow).mock.calls.map((c) => c[0] as any);
    const fill = rows.find((r) => !r.is_liquidation);

    // No liquidation marker is produced under v18.
    expect(rows.some((r) => r.is_liquidation)).toBe(false);
    // The fill is numbered as if the crank were not there — matching the
    // poll/backfill paths, so the shared dedup key agrees and it is not stored twice.
    expect(fill).toBeDefined();
    expect(fill.leg_index).toBe(0);
  });

  it('cranks interleaved among fills produce no rows and do not shift fill numbering', async () => {
    const mockDecode = vi.mocked(shared.decodeBase58);
    mockDecode
      .mockReturnValueOnce(tradeBytes())
      .mockReturnValueOnce(crankBytes())
      .mockReturnValueOnce(tradeBytes())
      .mockReturnValueOnce(crankBytes())
      .mockReturnValueOnce(tradeBytes());

    await app.fetch(makeRequest([{
      signature: SIG,
      instructions: [
        { programId: PROGRAM_ID, data: 't1', accounts: [TRADER, TRADER, SLAB] },
        { programId: PROGRAM_ID, data: 'c1', accounts: [TRADER, SLAB, PORTFOLIO] },
        { programId: PROGRAM_ID, data: 't2', accounts: [TRADER, TRADER, SLAB] },
        { programId: PROGRAM_ID, data: 'c2', accounts: [TRADER, SLAB, PORTFOLIO] },
        { programId: PROGRAM_ID, data: 't3', accounts: [TRADER, TRADER, SLAB] },
      ],
      innerInstructions: [], accountData: [], logs: [],
    }]));

    const rows = vi.mocked(insertTradeRow).mock.calls.map((c) => c[0] as any);
    const fills = rows.filter((r) => !r.is_liquidation).map((r) => r.leg_index);

    // Fills are contiguous from 0 regardless of how many cranks sit between them.
    expect(fills).toEqual([0, 1, 2]);
    // No liquidation markers exist under v18.
    expect(rows.filter((r) => r.is_liquidation)).toHaveLength(0);
  });

  it('leaves the no-crank case alone — fills still number 0,1,2', async () => {
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
