/**
 * Cold lot_exp cache on the poll and backfill paths (review of #277, H1/H2/M2): TradeCpi fills read from history
 * (startup backfill runs before the first StatsCollector sweep) must be priced per token, never at lot 0.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { PublicKey } from "@solana/web3.js";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const getAccountInfo = vi.fn();
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return { ...actual, config: { ...actual.config, allProgramIds: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB"] }, getConnection: vi.fn(() => ({ getAccountInfo })), withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()) };
});
const recordSkipped = vi.fn(async () => undefined);
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (...a: unknown[]) => recordSkipped(...(a as [])), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../src/db/insertTradeRow.js", () => {
  const insertTradeRow = vi.fn(async (_row?: unknown) => true);
  const insertTradeRows = vi.fn(async (rows: unknown[]) => { const out: unknown[] = []; for (const r of rows) if (await insertTradeRow(r)) out.push(r); return out; });
  const tradeKey = (r: { tx_signature: string | null; asset_index: number; leg_index: number }) => `${r.tx_signature ?? ""}|${r.asset_index}|${r.leg_index}`;
  return { insertTradeRow, insertTradeRows, tradeKey };
});
vi.mock("../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async () => null),
}));
import { insertTradeRow } from "../src/db/insertTradeRow.js";
import { TradeIndexerPolling } from "../src/services/TradeIndexer.js";
import { resetLotExpCache } from "../src/parsers/lotScale.js";
import { buildMarket, LAYOUT_V22 } from "./helpers/v22Fixtures.js";

const fx = (n: string) => JSON.parse(readFileSync(new URL(`./fixtures/tradecpi/${n}.json`, import.meta.url), "utf8"));
const pk = (s: string) => new PublicKey(s);
function asWeb3(f: any) {
  const tx = f.tx;
  return {
    slot: tx.slot, blockTime: tx.blockTime,
    meta: { err: null, logMessages: [], innerInstructions: tx.meta.innerInstructions.map((g: any) => ({ index: g.index, instructions: g.instructions.map((i: any) => ({ ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) })) })) },
    transaction: { message: { instructions: tx.transaction.message.instructions.map((i: any) => i.parsed ? i : ({ ...i, programId: pk(i.programId), accounts: (i.accounts ?? []).map(pk) })) } },
  };
}
const f = fx("partial");
const wireIx = f.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100);
const SLAB: string = wireIx.accounts[1];
const MATCHER: string = wireIx.accounts[4];

function enc58(bytes: Uint8Array): string {
  const A = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let zeros = 0; while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const d: number[] = [];
  for (let i = zeros; i < bytes.length; i++) { let c = bytes[i]; for (let j = 0; j < d.length; j++) { c += d[j] << 8; d[j] = c % 58; c = (c / 58) | 0; } while (c > 0) { d.push(c % 58); c = (c / 58) | 0; } }
  return "1".repeat(zeros) + d.reverse().map((x) => A[x]).join("");
}
const SIG = f.sig;
/** BatchTradeCpi (tag 67) with two long legs on asset 0 (the SAME (slab, asset, trader, side) key) and the matcher batch call + return data. */
function batchTx() {
  const wire = Buffer.alloc(2 + 2 * 42);
  wire[0] = 67; wire[1] = 2;
  for (let i = 0; i < 2; i++) { const o = 2 + i * 42; wire.writeUInt16LE(0, o); wire.writeBigUInt64LE(5_000_000n, o + 10); }
  const call = Buffer.alloc(18 + 26 * 2);
  call[0] = 3; call[1] = 2; call.writeBigUInt64LE(77n, 2); call.writeBigUInt64LE(9n, 10);
  for (let i = 0; i < 2; i++) { const o = 18 + i * 26; call.writeUInt16LE(0, o); call.writeBigUInt64LE(100_000_000n, o + 2); call.writeBigUInt64LE(5_000_000n, o + 10); }
  const ret = Buffer.alloc(128);
  for (let i = 0; i < 2; i++) {
    const o = i * 64;
    ret.writeUInt32LE(3, o); ret.writeUInt32LE(1, o + 4); ret.writeBigUInt64LE(100_000_000n, o + 8); ret.writeBigUInt64LE(4_000_000n, o + 16);
    ret.writeBigUInt64LE(77n, o + 32); ret.writeBigUInt64LE(9n, o + 40); ret.writeBigUInt64LE(100_000_000n, o + 48); ret.writeBigUInt64LE(0n, o + 56);
  }
  const accounts = wireIx.accounts.map(pk);
  return {
    slot: 1, blockTime: 1,
    meta: {
      err: null, logMessages: [],
      returnData: { programId: MATCHER, data: [ret.toString("base64"), "base64"] },
      innerInstructions: [{ index: 0, instructions: [{ programId: pk(MATCHER), accounts: [], data: enc58(call) }] }],
    },
    transaction: { message: { instructions: [{ programId: pk(PROGRAM), accounts, data: enc58(wire) }] } },
  };
}
function rpcFlaky(failFirst: number) {
  let n = 0;
  getAccountInfo.mockImplementation(async (key: any) => {
    if (key.toBase58() !== SLAB) return { data: Buffer.alloc(0) };
    if (n++ < failFirst) throw new Error("rpc blip");
    const d = buildMarket(LAYOUT_V22, { slots: 2 });
    d[LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + LAYOUT_V22.wrapperSlot.profileLotExp] = 6;
    return { data: Buffer.from(d) };
  });
}
const written = () => vi.mocked(insertTradeRow).mock.calls.map((c) => c[0] as any);

describe("M2: BatchTradeCpi with an unreadable market", () => {
  beforeEach(() => { resetLotExpCache(); vi.mocked(insertTradeRow).mockClear(); getAccountInfo.mockReset(); recordSkipped.mockClear(); });

  it("holds the WHOLE tx (retryNeeded -> CtxReadRetry): no leg is written, none is recorded as skipped; the next pass writes both legs", async () => {
    rpcFlaky(1); // the first market read fails, the RPC then recovers
    const indexer: any = new TradeIndexerPolling();
    await expect(indexer.processTransaction(batchTx(), SIG, SLAB, new Set([PROGRAM]))).rejects.toThrow(/will be retried/);
    expect(written()).toEqual([]); // before the fix leg 1 was written here while leg 0 was skipped (then lost through the stored-count dedup)
    expect(recordSkipped).not.toHaveBeenCalled();
    await indexer.processTransaction(batchTx(), SIG, SLAB, new Set([PROGRAM]));
    expect(written().map((r) => r.leg_index).sort()).toEqual([0, 1]);
    expect(written().every((r) => r.size === "4000000")).toBe(true);
    expect(written()[0].price).toBeCloseTo(100e-6, 12);
  });

  it("lot 0 batch is unchanged: both legs written in one pass at priceE6 / 1e6", async () => {
    getAccountInfo.mockImplementation(async () => ({ data: Buffer.from(buildMarket(LAYOUT_V22, { slots: 2 })) }));
    await (new TradeIndexerPolling() as any).processTransaction(batchTx(), SIG, SLAB, new Set([PROGRAM]));
    expect(written().map((r) => r.leg_index).sort()).toEqual([0, 1]);
    expect(written()[0].price).toBe(100);
  });
});
