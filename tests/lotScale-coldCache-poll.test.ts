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
const SLAB: string = f.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100).accounts[1];

function slabBytes(lot: number, markEwmaE6?: bigint): Buffer {
  const d = buildMarket(LAYOUT_V22, { slots: 2, ...(markEwmaE6 !== undefined ? { markEwmaE6 } : {}) });
  d[LAYOUT_V22.marketGroupOff + LAYOUT_V22.marketGroupLen + LAYOUT_V22.wrapperSlot.profileLotExp] = lot;
  return Buffer.from(d);
}
function rpc(slab: Buffer | "throw") {
  getAccountInfo.mockImplementation(async (key: any) => {
    if (key.toBase58() === f.ctx) return { data: Buffer.from(f.ctxReturnHex.padEnd(640, "0"), "hex") };
    if (slab === "throw") throw new Error("rpc down");
    return { data: slab };
  });
}
const slabReads = () => getAccountInfo.mock.calls.filter((c) => c[0].toBase58() === SLAB).length;
const process1 = () => (new TradeIndexerPolling() as any).processTransaction(asWeb3(f), f.sig, SLAB, new Set([PROGRAM]));
const written = () => vi.mocked(insertTradeRow).mock.calls.map((c) => c[0] as any);

describe("poll path: TradeCpi on a cold lot_exp cache", () => {
  beforeEach(() => { resetLotExpCache(); vi.mocked(insertTradeRow).mockClear(); getAccountInfo.mockReset(); recordSkipped.mockClear(); });

  it("lot 6: one market read, per-token price (not the 10^6 too high lot-0 price)", async () => {
    rpc(slabBytes(6));
    await process1();
    expect(written()).toEqual([expect.objectContaining({ size: "483" })]);
    expect(written()[0].price).toBeCloseTo(13.861751e-6, 18);
    expect(slabReads()).toBe(1);
  });

  it("lot 0: byte-identical (price = priceE6 / 1e6) and the fee matches the lot-6 fee of the same fill", async () => {
    rpc(slabBytes(0));
    await process1();
    const lot0 = written()[0];
    expect(lot0.price).toBe(13_861_751 / 1_000_000);
    expect(lot0.fee).toBeGreaterThan(0);
    resetLotExpCache(); vi.mocked(insertTradeRow).mockClear();
    rpc(slabBytes(6));
    await process1();
    expect(written()[0].fee).toBeCloseTo(lot0.fee, 12);
  });

  it("market unreadable: no row, signature recorded for re-index; a later pass writes the right row", async () => {
    rpc("throw");
    await process1();
    expect(written()).toEqual([]);
    expect(recordSkipped).toHaveBeenCalledWith([expect.objectContaining({ signature: f.sig, error: expect.stringContaining("lot_exp") })]);
    rpc(slabBytes(6));
    await process1();
    expect(written()[0].price).toBeCloseTo(13.861751e-6, 18);
  });

  it("H2: the startup backfill runs with a cold cache (before the first sweep) and still writes per-token prices", async () => {
    rpc(slabBytes(6));
    const indexer: any = new TradeIndexerPolling();
    indexer._running = true;
    indexer.pollTargets = async () => [{ slab_address: SLAB }];
    indexer.indexTradesForSlab = async () => { await indexer.processTransaction(asWeb3(f), f.sig, SLAB, new Set([PROGRAM])); };
    await indexer.backfill();
    expect(written()).toHaveLength(1);
    expect(written()[0].price).toBeCloseTo(13.861751e-6, 18);
  });
});

describe("TradeIndexer.readMarkPriceFromSlab mark-EWMA fallback", () => {
  beforeEach(() => { resetLotExpCache(); });
  const read = (slab: Buffer) => (new TradeIndexerPolling() as any).readMarkPriceFromSlab({ getAccountInfo: async () => ({ data: slab }) }, SLAB, 0);

  it("scales the EWMA per token: lot 6 300_000 -> 3e-7; lot 0 stays 0.3", async () => {
    expect(await read(slabBytes(6, 300_000n))).toBeCloseTo(3e-7, 20);
    expect(await read(slabBytes(0, 300_000n))).toBe(300_000 / 1_000_000);
  });
});
