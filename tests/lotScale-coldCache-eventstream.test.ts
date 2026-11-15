/** Cold lot_exp cache on the event-stream path (review of #277, H1): the fill is priced per token or skipped, never lot 0. */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const rowsOut: any[] = [];
const recordSkipped = vi.fn(async () => undefined);
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (...a: unknown[]) => recordSkipped(...(a as [])) }));
vi.mock("../src/db/insertTradeRow.js", () => ({
  tradeKey: (r: any) => `${r.tx_signature}|${r.asset_index}|${r.leg_index}`,
  insertTradeRows: vi.fn(async (rows: any[]) => { rowsOut.push(...rows); return rows; }),
}));
import { EventStreamService } from "../src/services/EventStreamService.js";
import { resetLotExpCache } from "../src/parsers/lotScale.js";
import { buildMarket, LAYOUT_V22 } from "./helpers/v22Fixtures.js";

const f = JSON.parse(readFileSync(new URL("./fixtures/tradecpi/partial.json", import.meta.url), "utf8"));
const SLAB: string = f.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100).accounts[1];

function slabBytes(lot: number, markEwmaE6?: bigint): Buffer {
  const d = buildMarket(LAYOUT_V22, { slots: 2, ...(markEwmaE6 !== undefined ? { markEwmaE6 } : {}) });
  d[LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + LAYOUT_V22.wrapperSlot.profileLotExp] = lot;
  return Buffer.from(d);
}
async function run(slab: Buffer | "throw" | ((key: string) => Buffer), tx: any = f.tx) {
  const getAccountInfo = vi.fn(async (key: any) => {
    if (key.toBase58() === f.ctx) return { data: Buffer.from(f.ctxReturnHex.padEnd(640, "0"), "hex") };
    if (slab === "throw") throw new Error("rpc down");
    return { data: typeof slab === "function" ? slab(key.toBase58()) : slab };
  });
  let cb: (m: any) => void = () => {};
  const ws: any = { sub: () => {}, onNotification: (c: any) => { cb = c; }, close: () => {}, isOpen: true };
  const svc = new EventStreamService({ ws, programId: PROGRAM, connection: { getAccountInfo } as any, autoIndex: true, knownSlabs: [SLAB] });
  await svc.start();
  cb({ method: "transactionNotification", params: { result: tx } });
  await new Promise((r) => setTimeout(r, 600));
  return rowsOut.splice(0);
}

describe("event stream: TradeCpi on a cold lot_exp cache", () => {
  beforeEach(() => { resetLotExpCache(); rowsOut.length = 0; recordSkipped.mockClear(); });

  it("lot 6: per-token price", async () => {
    const rows = await run(slabBytes(6));
    expect(rows).toHaveLength(1);
    expect(rows[0].price).toBeCloseTo(13.861751e-6, 18);
  });
  it("lot 0: priceE6 / 1e6 exactly", async () => {
    expect((await run(slabBytes(0)))[0].price).toBe(13_861_751 / 1_000_000);
  });
  it("M1: one failed market read does not drop the fill (retried once)", async () => {
    let n = 0;
    const slab = slabBytes(6);
    const rows = await run((key) => { if (key === SLAB && n++ === 0) throw new Error("rpc blip"); return slab; });
    expect(rows).toHaveLength(1);
    expect(rows[0].price).toBeCloseTo(13.861751e-6, 18);
  });
  it("market stays unreadable: no row (a lot-0 price would be permanent), signature recorded so the poller retries", async () => {
    expect(await run("throw")).toEqual([]);
    expect(recordSkipped).toHaveBeenCalledWith([expect.objectContaining({ error: expect.stringContaining("lot_exp") })]);
  });
});
