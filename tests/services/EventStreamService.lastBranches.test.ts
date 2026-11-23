/**
 * Pins the last two untaken branches of EventStreamService (merge-gate base 6877363, v8 branch map):
 *  - handle(): recordV22LogEvents reporting "write_failed" (v22LogEvents status) must still index the fills, then fail
 *    the handler, so a lost wrapper log-event write is never a silent success.
 *  - indexFills(): a market account that does not exist (getAccountInfo -> null / no data) is an unreadable market:
 *    the fill is not written and the signature is recorded for the poller, not dropped silently.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const rowsOut: any[] = [];
const recordSkipped = vi.fn(async () => undefined);
const logEvents = vi.hoisted(() => ({ status: "ok" as string }));
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (...a: unknown[]) => recordSkipped(...(a as [])) }));
vi.mock("../../src/db/insertTradeRow.js", () => ({
  tradeKey: (r: any) => `${r.tx_signature}|${r.asset_index}|${r.leg_index}`,
  insertTradeRows: vi.fn(async (rows: any[]) => { rowsOut.push(...rows); return rows; }),
}));
vi.mock("../../src/services/v22LogEvents.js", () => ({ recordV22LogEvents: vi.fn(async () => ({ status: logEvents.status, rows: 2, slab: "LOGSLAB" })) }));
import { EventStreamService } from "../../src/services/EventStreamService.js";
import { resetLotExpCache } from "../../src/parsers/lotScale.js";
import { buildMarket, LAYOUT_V22 } from "../helpers/v22Fixtures.js";

const f = JSON.parse(readFileSync(new URL("../fixtures/tradecpi/partial.json", import.meta.url), "utf8"));
const SLAB: string = f.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100).accounts[1];

function slabBytes(lot: number): Buffer {
  const d = buildMarket(LAYOUT_V22, { slots: 2 });
  d[LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + LAYOUT_V22.wrapperSlot.profileLotExp] = lot;
  return Buffer.from(d);
}
function mk(slab: Buffer | null | { data: undefined }) {
  const getAccountInfo = vi.fn(async (key: any) => {
    if (key.toBase58() === f.ctx) return { data: Buffer.from(f.ctxReturnHex.padEnd(640, "0"), "hex") };
    return slab && "data" in (slab as any) && (slab as any).data === undefined ? { data: undefined } : slab ? { data: slab } : null;
  });
  const ws: any = { sub: () => {}, onNotification: () => {}, close: () => {}, isOpen: true };
  return new EventStreamService({ ws, programId: PROGRAM, connection: { getAccountInfo } as any, autoIndex: true, knownSlabs: [SLAB] });
}
const msg = () => ({ method: "transactionNotification", params: { result: f.tx } });

describe("EventStreamService last branches", () => {
  beforeEach(() => { resetLotExpCache(); rowsOut.length = 0; recordSkipped.mockClear(); logEvents.status = "ok"; });

  it("log-event write_failed: fills are still indexed, then the handler fails", async () => {
    logEvents.status = "write_failed";
    const err = await (mk(slabBytes(6)) as any).handle(msg()).then(() => null, (e: unknown) => e);
    expect(rowsOut).toHaveLength(1); // fill indexed first, unconditionally
    // The log events are not retried: the error must not claim retries, and the record must carry their slab.
    expect(String(err)).toMatch(/v22 log events \(2\) write failed \(not retried\)/);
    expect(String(err)).not.toMatch(/after retries/);
    expect(recordSkipped).toHaveBeenCalledWith([
      { signature: f.tx.signature, source: "v22-events", slab: "LOGSLAB", error: "v22 log events (2) write failed (not retried)" },
    ]);
  });

  it("log-event ok: handler succeeds (control)", async () => {
    const err = await (mk(slabBytes(6)) as any).handle(msg()).then(() => null, (e: unknown) => e);
    expect(err).toBeNull();
    expect(rowsOut).toHaveLength(1);
    expect(recordSkipped).not.toHaveBeenCalled();
  });

  it.each([["null account", null], ["account without data", { data: undefined }]])(
    "market read returns %s: no row, signature recorded for the poller", async (_n, slab) => {
      await (mk(slab as any) as any).handle(msg());
      expect(rowsOut).toEqual([]);
      expect(recordSkipped).toHaveBeenCalledWith([expect.objectContaining({ error: expect.stringContaining("lot_exp") })]);
    });
});
