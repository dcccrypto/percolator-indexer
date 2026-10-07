/** #213 / #221 on the event-stream path, real jsonParsed transactions. */
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

const fx = (n: string) => JSON.parse(readFileSync(new URL(`./fixtures/tradecpi/${n}.json`, import.meta.url), "utf8"));

async function run(f: any, getAccountInfo: any) {
  let cb: (m: any) => void = () => {};
  const ws: any = { sub: () => {}, onNotification: (c: any) => { cb = c; }, close: () => {}, isOpen: true };
  const wrapperIx = f.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100);
  const svc = new EventStreamService({ ws, programId: PROGRAM, connection: { getAccountInfo } as any, autoIndex: true, knownSlabs: [wrapperIx.accounts[1]] });
  await svc.start();
  cb({ method: "transactionNotification", params: { result: f.tx } });
  await new Promise((r) => setTimeout(r, 50));
  return rowsOut.splice(0);
}

describe("event stream: TradeCpi rows", () => {
  beforeEach(() => { rowsOut.length = 0; recordSkipped.mockClear(); });

  it("partial fill: executed size and booked price; the slab is never read", async () => {
    const f = fx("partial");
    const get = vi.fn(async () => ({ data: Buffer.from(f.ctxReturnHex.padEnd(640, "0"), "hex") }));
    const rows = await run(f, get);
    expect(rows).toEqual([expect.objectContaining({ size: "483", price: 13.861751, side: "short" })]);
    expect(get).toHaveBeenCalledTimes(1);
  });

  it("zero fill: no row", async () => {
    const get = vi.fn(async () => null);
    expect(await run(fx("zeroNoMatcherCall"), get)).toEqual([]);
    expect(get).not.toHaveBeenCalled();
  });

  it("context overwritten: no row, signature recorded", async () => {
    const f = fx("issue213Partial");
    const other = fx("full");
    const rows = await run(f, vi.fn(async () => ({ data: Buffer.from(other.ctxReturnHex.padEnd(640, "0"), "hex") })));
    expect(rows).toEqual([]);
    expect(recordSkipped).toHaveBeenCalledTimes(1);
  });
});
