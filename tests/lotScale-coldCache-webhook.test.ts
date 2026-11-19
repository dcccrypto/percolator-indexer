/**
 * Cold lot_exp cache on the webhook path (review of #277, H1/M1/M2): a TradeCpi fill on a lot market must never be
 * priced at lot 0 because the sweep has not seen the market yet, and the EWMA fallback must be per-token with a
 * fee that does not inflate. lot_exp 0 must come out exactly as before.
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
vi.mock("../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async () => null),
}));
import { insertTradeRows } from "../src/db/insertTradeRow.js";
import { webhookRoutes } from "../src/routes/webhook.js";
import { resetLotExpCache } from "../src/parsers/lotScale.js";
import { buildMarket, LAYOUT_V22 } from "./helpers/v22Fixtures.js";

const enhanced = JSON.parse(readFileSync(new URL("./fixtures/tradecpi/helius-enhanced.json", import.meta.url), "utf8")).txs as Record<string, any>;
const classic = (n: string) => JSON.parse(readFileSync(new URL(`./fixtures/tradecpi/${n}.json`, import.meta.url), "utf8"));
const ctxBytes = (hex: string) => Buffer.from(hex.padEnd(640, "0"), "hex");

const f = classic("partial");
const wire = enhanced[f.sig].instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100);
const SLAB: string = wire.accounts[1];

/** A v2.2 market account with `lot` as asset 0's lot_exp and the given mark EWMA (effective_price left 0). */
function slabBytes(lot: number, markEwmaE6?: bigint): Buffer {
  const d = buildMarket(LAYOUT_V22, { slots: 2, ...(markEwmaE6 !== undefined ? { markEwmaE6 } : {}) });
  d[LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + LAYOUT_V22.wrapperSlot.profileLotExp] = lot;
  return Buffer.from(d);
}

async function deliver(tx: unknown) {
  vi.mocked(insertTradeRows).mockClear();
  const app = webhookRoutes();
  const res = await app.fetch(new Request("http://x/webhook/trades", { method: "POST", headers: { "content-type": "application/json", authorization: "s3cret" }, body: JSON.stringify([tx]) }));
  return { status: res.status, rows: vi.mocked(insertTradeRows).mock.calls.flatMap((c) => c[0] as any[]).filter((r) => !r.is_liquidation) };
}

/** getAccountInfo that answers the matcher context and the market account separately. */
function rpc(slab: Buffer | null | "throw") {
  getAccountInfo.mockImplementation(async (pk: any) => {
    if (pk.toBase58() === f.ctx) return { data: ctxBytes(f.ctxReturnHex) };
    if (slab === "throw") throw new Error("rpc down");
    return slab ? { data: slab } : null;
  });
}
const slabReads = () => getAccountInfo.mock.calls.filter((c) => c[0].toBase58() === SLAB).length;

describe("webhook TradeCpi on a cold lot_exp cache", () => {
  beforeEach(() => { resetLotExpCache(); getAccountInfo.mockReset(); recordSkipped.mockClear(); });

  it("lot 6, nothing cached: the market is read once and the row is per-token, never the 10^6 too high lot-0 price", async () => {
    rpc(slabBytes(6));
    const { rows } = await deliver(enhanced[f.sig]);
    expect(rows).toHaveLength(1);
    expect(rows[0].price).toBeCloseTo(13.861751e-6, 18); // NOT 13.861751
    expect(slabReads()).toBe(1);
    // warm now: a second delivery reads no market
    await deliver(enhanced[f.sig]);
    expect(slabReads()).toBe(1);
  });

  it("M1: the slab post-state already in the payload fills the cache, so no market read is made", async () => {
    rpc(null); // a market read would find nothing
    const tx = JSON.parse(JSON.stringify(enhanced[f.sig]));
    tx.accountData = [{ account: SLAB, data: slabBytes(6).toString("base64") }];
    const { rows } = await deliver(tx);
    expect(rows[0].price).toBeCloseTo(13.861751e-6, 18);
    expect(slabReads()).toBe(0);
  });

  it("M1: market unreadable (RPC error): no row, answers 500 so Helius redelivers (not 200-and-dropped); the redelivery writes the right row", async () => {
    rpc("throw");
    const { status, rows } = await deliver(enhanced[f.sig]);
    expect(rows).toEqual([]);
    expect(status).toBe(500);
    expect(recordSkipped).not.toHaveBeenCalled();
    // the redelivery with the RPC back writes the right row
    rpc(slabBytes(6));
    const again = await deliver(enhanced[f.sig]);
    expect(again.status).toBe(200);
    expect(again.rows[0].price).toBeCloseTo(13.861751e-6, 18);
  });

  it("M1: a market that stays unreadable is bounded: after 5 redeliveries the signature is recorded for re-index and the delivery is answered 2xx", async () => {
    rpc("throw");
    for (let i = 0; i < 5; i++) expect((await deliver(enhanced[f.sig])).status).toBe(500);
    const last = await deliver(enhanced[f.sig]);
    expect(last.status).toBe(200);
    expect(last.rows).toEqual([]);
    expect(recordSkipped).toHaveBeenCalledWith([expect.objectContaining({ signature: f.sig, error: expect.stringContaining("lot_exp") })]);
  });

  it("low: the market read is bounded by the delivery deadline (an expired budget makes no market read)", async () => {
    rpc(slabBytes(6));
    // A negative TRADECPI_DELIVERY_BUDGET_MS is no longer accepted (it fell back to 8000), so expire the deadline
    // by making the clock advance a day per read.
    const real = Date.now();
    let calls = 0;
    const spy = vi.spyOn(Date, "now").mockImplementation(() => real + 86_400_000 * calls++);
    try {
      const tx = JSON.parse(JSON.stringify(enhanced[f.sig]));
      const { rows } = await deliver(tx);
      expect(rows).toEqual([]);
      expect(slabReads()).toBe(0);
    } finally {
      spy.mockRestore();
    }
  });

  it("lot_exp 0 is byte-identical to before: price is priceE6 / 1e6 exactly and the fee is unchanged", async () => {
    rpc(slabBytes(0));
    const { rows } = await deliver(enhanced[f.sig]);
    expect(rows[0].price).toBe(13_861_751 / 1_000_000);
    expect(rows[0].size).toBe("483");
    // the same fill with a lot market has the same fee: notional (size x per-lot price) does not depend on the scale
    resetLotExpCache();
    rpc(slabBytes(6));
    const lot6 = (await deliver(enhanced[f.sig])).rows[0];
    expect(lot6.fee).toBeCloseTo(rows[0].fee, 12);
  });
});

describe("webhook mark-EWMA fallback (legacy leg, effective_price 0)", () => {
  beforeEach(() => { resetLotExpCache(); getAccountInfo.mockReset(); recordSkipped.mockClear(); });

  function legacyTx(slab: Buffer) {
    const tx = JSON.parse(JSON.stringify(enhanced[f.sig]));
    for (const i of tx.instructions) delete i.innerInstructions; // unrecognised matcher call -> legacy -> mark fallback
    tx.accountData = [{ account: SLAB, data: slab.toString("base64") }];
    return tx;
  }

  it("lot 6: the EWMA price is per-token (300_000 -> 3e-7), and the fee equals the lot-0 fee on the same per-lot notional", async () => {
    const lot6 = (await deliver(legacyTx(slabBytes(6, 300_000n)))).rows[0];
    expect(lot6.price).toBeCloseTo(3e-7, 20);
    resetLotExpCache();
    const lot0 = (await deliver(legacyTx(slabBytes(0, 300_000n)))).rows[0];
    expect(lot0.price).toBe(300_000 / 1_000_000); // lot 0: unchanged
    expect(lot0.fee).toBeGreaterThan(0);
    expect(lot6.fee).toBeCloseTo(lot0.fee, 12); // was inflated 10^6 when the unscaled EWMA met a warm lot cache
  });
});
