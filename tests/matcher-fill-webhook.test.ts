/**
 * #213 / #221 on the webhook path, with REAL Helius enhanced payloads (real IX_TAG, real base58).
 * The matcher call is nested under the wrapper instruction in `instructions[].innerInstructions`.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const getAccountInfo = vi.fn();

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB"], webhookSecret: "s3cret" },
    eventBus: { publish: vi.fn(), emit: vi.fn(), on: vi.fn() },
    getConnection: vi.fn(() => ({ getAccountInfo })),
    withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    captureException: vi.fn(),
  };
});
const recordSkipped = vi.fn(async () => undefined);
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (...a: unknown[]) => recordSkipped(...(a as [])) }));
vi.mock("../src/db/insertTradeRow.js", () => ({
  tradeKey: (r: any) => `${r.tx_signature}|${r.asset_index}|${r.leg_index}`,
  insertTradeRow: vi.fn(),
  insertTradeRows: vi.fn(async (rows: any[]) => rows),
}));

const storedLegs: { rows: any[] | null } = { rows: [] };
vi.mock("../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => storedLegs.rows),
  fetchStoredLegsMany: vi.fn(async () => null),
}));
import { insertTradeRows } from "../src/db/insertTradeRow.js";
import { webhookRoutes } from "../src/routes/webhook.js";

const enhanced = JSON.parse(readFileSync(new URL("./fixtures/tradecpi/helius-enhanced.json", import.meta.url), "utf8")).txs as Record<string, any>;
const classic = (n: string) => JSON.parse(readFileSync(new URL(`./fixtures/tradecpi/${n}.json`, import.meta.url), "utf8"));

async function deliver(tx: unknown) {
  const app = webhookRoutes();
  const res = await app.fetch(new Request("http://x/webhook/trades", { method: "POST", headers: { "content-type": "application/json", authorization: "s3cret" }, body: JSON.stringify([tx]) }));
  expect(res.status).toBe(200);
  return vi.mocked(insertTradeRows).mock.calls.flatMap((c) => c[0] as any[]);
}
const ctxReturnBytes = (hex: string) => Buffer.from(hex.padEnd(640, "0"), "hex"); // 320-byte account

describe("webhook: TradeCpi rows carry the executed size and the booked price", () => {
  beforeEach(() => { storedLegs.rows = []; vi.mocked(insertTradeRows).mockClear(); getAccountInfo.mockReset(); recordSkipped.mockClear(); });

  it("matcher partial fill: row is the executed 483, priced at the booked price, NOT the 29.041225 request", async () => {
    const f = classic("partial");
    getAccountInfo.mockResolvedValue({ data: ctxReturnBytes(f.ctxReturnHex) });
    const rows = await deliver(enhanced[f.sig]);
    expect(rows.filter((r) => !r.is_liquidation)).toEqual([expect.objectContaining({ size: "483", price: 13.861751, side: "short", tx_signature: f.sig, leg_index: 0 })]);
    expect(getAccountInfo).toHaveBeenCalledTimes(1); // the matcher context, never the slab
    expect(getAccountInfo.mock.calls[0][0].toBase58()).toBe(f.ctx);
  });

  it("zero fill (no matcher call): nothing written, not even a context read, and not reported as skipped", async () => {
    const f = classic("zeroNoMatcherCall");
    const rows = await deliver(enhanced[f.sig]);
    expect(rows).toEqual([]);
    expect(getAccountInfo).not.toHaveBeenCalled();
    expect(recordSkipped).not.toHaveBeenCalled();
  });

  it("#213 transaction after its context was overwritten: row at the matcher-requested size and booked price, NOT recorded as skipped", async () => {
    const f = classic("issue213Partial");
    getAccountInfo.mockResolvedValue({ data: ctxReturnBytes(classic("full").ctxReturnHex) });
    const rows = await deliver(enhanced[f.sig]);
    expect(rows).toEqual([expect.objectContaining({ size: "822500", price: 121.580511, side: "long", leg_index: 0 })]);
    expect(recordSkipped).not.toHaveBeenCalled();
  });

  it("an already stored leg costs NO matcher-context RPC read and writes nothing", async () => {
    const f = classic("partial");
    const wire = enhanced[f.sig].instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100);
    storedLegs.rows = [{ slab_address: wire.accounts[1], asset_index: 0, leg_index: 0, trader: wire.accounts[0], side: "short", size: "483", is_liquidation: false }];
    getAccountInfo.mockResolvedValue({ data: ctxReturnBytes(f.ctxReturnHex) });
    expect(await deliver(enhanced[f.sig])).toEqual([]);
    expect(getAccountInfo).not.toHaveBeenCalled();
    // negative control: the same leg number but another asset is NOT stored -> read and written
    storedLegs.rows = [{ slab_address: wire.accounts[1], asset_index: 1, leg_index: 0, trader: wire.accounts[0], side: "short", size: "483", is_liquidation: false }];
    expect((await deliver(enhanced[f.sig])).filter((r) => !r.is_liquidation)).toHaveLength(1);
    expect(getAccountInfo).toHaveBeenCalledTimes(1);
  });

  it("strict mode: no row, signature recorded", async () => {
    const f = classic("issue213Partial");
    process.env.TRADECPI_UNVERIFIED_SIZE = "skip";
    try {
      getAccountInfo.mockResolvedValue(null);
      expect(await deliver(enhanced[f.sig])).toEqual([]);
      expect(recordSkipped).toHaveBeenCalledWith([expect.objectContaining({ signature: f.sig, error: expect.stringContaining("size-unverified") })]);
    } finally { delete process.env.TRADECPI_UNVERIFIED_SIZE; }
  });

  it("a payload without nested innerInstructions (unrecognised layout) writes nothing and is recorded", async () => {
    const f = classic("partial");
    const tx = JSON.parse(JSON.stringify(enhanced[f.sig]));
    for (const i of tx.instructions) delete i.innerInstructions;
    expect(await deliver(tx)).toEqual([]);
    expect(recordSkipped).toHaveBeenCalledWith([expect.objectContaining({ error: expect.stringContaining("unrecognised") })]);
  });

  it("real payload yields exactly one row; the matcher program's own instruction is not mistaken for a trade", async () => {
    const f = classic("partial");
    getAccountInfo.mockResolvedValue({ data: ctxReturnBytes(f.ctxReturnHex) });
    const tx = JSON.parse(JSON.stringify(enhanced[f.sig]));
    // the dead tx-level loop used to scan this shape; the real format never has it
    tx.innerInstructions = [{ instructions: tx.instructions.flatMap((i: any) => i.innerInstructions ?? []) }];
    const rows = (await deliver(tx)).filter((r) => !r.is_liquidation);
    expect(rows).toHaveLength(1);
  });

  it("negative control (old behaviour): the wire request would have been written at full size", async () => {
    const f = classic("partial");
    const wire = enhanced[f.sig].instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100);
    expect(wire).toBeDefined(); // instruction carries 29,041,225; the row above says 483
  });
});
