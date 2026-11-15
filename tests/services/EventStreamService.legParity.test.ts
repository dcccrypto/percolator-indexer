/**
 * Stream-path half of the leg_index parity check, with the REAL fill parser.
 * A tx whose first fill is on an untracked slab and whose second is on a tracked slab: the stream numbers
 * the tracked fill 1 (position in the parser's list). The webhook must agree (see
 * tests/routes/webhook.leg-index-parity.test.ts), otherwise the shared dedup key
 * (tx_signature, asset_index, leg_index) misses and the fill is stored twice.
 * Also pins the stream's own failure mode: a failed batch write is dropped without a retry.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { IX_TAG, encodeTradeNoCpi } from "@percolatorct/sdk";

const m = vi.hoisted(() => ({ insertRows: vi.fn() }));
vi.mock("../../src/db/insertTradeRow.js", () => ({
  insertTradeRows: m.insertRows,
  tradeKey: (r: any) => `${r.tx_signature}|${r.asset_index}|${r.leg_index}`,
}));
vi.mock("../../src/parsers/markPrice.js", () => ({ readMarkPriceE6: vi.fn(async () => 2_000_000) }));
vi.mock("../../src/db/insertV22Events.js", () => ({ insertV22Events: vi.fn() }));
vi.mock("../../src/services/v22LogEvents.js", () => ({ recordV22LogEvents: vi.fn() }));
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn() }));
vi.mock("../../src/blocklist.js", () => ({ isBlockedSlab: () => false }));

import { EventStreamService } from "../../src/services/EventStreamService.js";

const PROGRAM = "GM8zjJ8LTBMv9xEsverh6H6wLyevgMHEJXcEzyY3rY24";
const TRADER = "11111111111111111111111111111111";
const TRACKED = "So11111111111111111111111111111111111111112";
const UNTRACKED = "SysvarRent111111111111111111111111111111111";

function b58(bytes: Uint8Array): string {
  const A = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const d: number[] = [];
  for (let i = zeros; i < bytes.length; i++) {
    let c = bytes[i];
    for (let j = 0; j < d.length; j++) { c += d[j] << 8; d[j] = c % 58; c = (c / 58) | 0; }
    while (c > 0) { d.push(c % 58); c = (c / 58) | 0; }
  }
  return "1".repeat(zeros) + d.reverse().map((x) => A[x]).join("");
}

function tradeIx(slab: string) {
  const data = encodeTradeNoCpi({
    accountAPortfolioId: 1n, accountAPositionEpoch: 0n, accountBPortfolioId: 2n, accountBPositionEpoch: 0n,
    assetIndex: 0, marketId: 9n, sizeQ: 1_000_000n, execPrice: 50_000_000_000n, feeBps: 30n, backingFeeCapBps: 0,
  });
  return { programId: PROGRAM, data: b58(data), accounts: [TRADER, TRADER, slab] };
}

function tx() {
  return {
    signature: "SIGLEGPARITY",
    slot: 1,
    transaction: { message: { instructions: [tradeIx(UNTRACKED), tradeIx(TRACKED)], accountKeys: [] } },
    meta: { err: null, logMessages: [], innerInstructions: [] },
  };
}

function setup() {
  let cb: (msg: any) => void = () => {};
  const ws: any = { sub: vi.fn(), onNotification: (c: any) => { cb = c; } };
  const svc = new EventStreamService({ ws, programId: PROGRAM, connection: {} as any, autoIndex: true, knownSlabs: [TRACKED] });
  return svc.start().then(() => (t: unknown) => (svc as any).handle({ method: "transactionNotification", params: { result: t } }));
}

beforeEach(() => { vi.clearAllMocks(); m.insertRows.mockImplementation(async (rows: any[]) => rows); });

describe("EventStreamService leg numbering vs the webhook", () => {
  it("numbers a tracked fill by its position in the tx's fill list (untracked fill holds leg 0)", async () => {
    expect(IX_TAG.TradeNoCpi).toBeDefined();
    const handle = await setup();
    await handle(tx());
    const rows = m.insertRows.mock.calls[0][0];
    expect(rows.map((r: any) => [r.slab_address, r.leg_index])).toEqual([[TRACKED, 1]]);
  });

  it("a failed batch write is not retried by the stream (recovery is the webhook / poll)", async () => {
    m.insertRows.mockRejectedValue(new Error("db down"));
    const handle = await setup();
    await handle(tx());
    expect(m.insertRows).toHaveBeenCalledTimes(1);
  });
});
