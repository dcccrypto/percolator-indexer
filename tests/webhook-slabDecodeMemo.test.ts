/**
 * Slab post-state decode memo (webhook): a slab account in accountData is decoded from base64 once per extract call
 * however many legs/assets/tags read it, and the rows are identical to the unmemoized decode.
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


function enc58(bytes: Uint8Array): string {
  const A = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let zeros = 0; while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const d: number[] = [];
  for (let i = zeros; i < bytes.length; i++) { let c = bytes[i]; for (let j = 0; j < d.length; j++) { c += d[j] << 8; d[j] = c % 58; c = (c / 58) | 0; } while (c > 0) { d.push(c % 58); c = (c / 58) | 0; } }
  return "1".repeat(zeros) + d.reverse().map((x) => A[x]).join("");
}
const N = 4;
/** BatchTradeCpi with N long legs on asset 0, the matcher batch call nested under it, and the ctx return bytes. */
function batchTx(withSlab: boolean) {
  const tx = JSON.parse(JSON.stringify(enhanced[f.sig]));
  const ix = tx.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 6);
  const wire = Buffer.alloc(2 + N * 42);
  wire[0] = 67; wire[1] = N;
  for (let i = 0; i < N; i++) { const o = 2 + i * 42; wire.writeUInt16LE(0, o); wire.writeBigUInt64LE(5_000_000n, o + 10); }
  const call = Buffer.alloc(18 + 26 * N);
  call[0] = 3; call[1] = N; call.writeBigUInt64LE(77n, 2); call.writeBigUInt64LE(9n, 10);
  for (let i = 0; i < N; i++) { const o = 18 + i * 26; call.writeUInt16LE(0, o); call.writeBigUInt64LE(100_000_000n, o + 2); call.writeBigUInt64LE(5_000_000n, o + 10); }
  const ret = Buffer.alloc(640);
  for (let i = 0; i < N; i++) {
    const o = i * 64;
    ret.writeUInt32LE(3, o); ret.writeUInt32LE(1, o + 4); ret.writeBigUInt64LE(100_000_000n, o + 8); ret.writeBigUInt64LE(4_000_000n, o + 16);
    ret.writeBigUInt64LE(77n, o + 32); ret.writeBigUInt64LE(9n, o + 40); ret.writeBigUInt64LE(100_000_000n, o + 48); ret.writeBigUInt64LE(0n, o + 56);
  }
  ix.data = enc58(wire);
  ix.innerInstructions[0].data = enc58(call);
  tx.accountData = withSlab ? [{ account: SLAB, data: slabBytes(6).toString("base64") }] : [];
  return { tx, ret };
}

describe("webhook slab decode memo", () => {
  beforeEach(() => { resetLotExpCache(); getAccountInfo.mockReset(); recordSkipped.mockClear(); });

  it("a 4-leg BatchTradeCpi decodes the slab base64 once, rows unchanged", async () => {
    const { tx, ret } = batchTx(true);
    getAccountInfo.mockImplementation(async (pk: any) => (pk.toBase58() === f.ctx ? { data: ret } : null));
    const slabLen = tx.accountData[0].data.length;
    const spy = vi.spyOn(Buffer, "from");
    const { status, rows } = await deliver(tx);
    const decodes = spy.mock.calls.filter((c) => c[1] === "base64" && typeof c[0] === "string" && c[0].length === slabLen).length;
    spy.mockRestore();
    console.log(`slab base64 decodes for ${N}-leg batch: ${decodes}`);
    expect(status).toBe(200);
    expect(rows).toHaveLength(N);
    expect(decodes).toBe(1);
    // differential: a second delivery of an identical (fresh) tx yields deep-equal rows (no state leaks between calls)
    resetLotExpCache();
    const again = await deliver(batchTx(true).tx);
    expect(again.rows).toEqual(rows);
  });

  it("rows with the slab in accountData deep-equal rows when the lot_exp comes from the market read", async () => {
    const a = batchTx(true);
    getAccountInfo.mockImplementation(async (pk: any) => (pk.toBase58() === f.ctx ? { data: a.ret } : null));
    const withPayload = await deliver(a.tx);
    resetLotExpCache();
    const b = batchTx(false);
    getAccountInfo.mockImplementation(async (pk: any) => (pk.toBase58() === f.ctx ? { data: b.ret } : { data: slabBytes(6) }));
    const viaRead = await deliver(b.tx);
    expect(withPayload.rows).toEqual(viaRead.rows);
  });
});
