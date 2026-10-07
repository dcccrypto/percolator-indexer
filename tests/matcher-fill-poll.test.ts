/** #213 / #221 on the poll path (TradeIndexerPolling.processTransaction), real transactions. */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { PublicKey } from "@solana/web3.js";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const getAccountInfo = vi.fn();
const getSignaturesForAddress = vi.fn();
const getParsedTransactions = vi.fn();
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return { ...actual, config: { ...actual.config, allProgramIds: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB"] }, getConnection: vi.fn(() => ({ getAccountInfo, getSignaturesForAddress, getParsedTransactions })), withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()) };
});
const recordSkipped = vi.fn(async () => undefined);
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (...a: unknown[]) => recordSkipped(...(a as [])), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true) }));
const storedLegs: { rows: any[] | null } = { rows: [] };
vi.mock("../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => storedLegs.rows),
  fetchStoredLegsMany: vi.fn(async () => null),
}));
import { insertTradeRow } from "../src/db/insertTradeRow.js";
import { TradeIndexerPolling } from "../src/services/TradeIndexer.js";

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
const market = (f: any) => f.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100).accounts[1] as string;

async function poll(f: any) {
  const idx: any = new TradeIndexerPolling();
  return idx.processTransaction(asWeb3(f), f.sig, market(f), new Set([PROGRAM]));
}

describe("poll path: TradeCpi rows", () => {
  beforeEach(() => { storedLegs.rows = []; vi.mocked(insertTradeRow).mockClear(); getAccountInfo.mockReset(); recordSkipped.mockClear(); });

  it("partial fill: executed size, booked price, fee on the executed notional", async () => {
    const f = fx("partial");
    getAccountInfo.mockResolvedValue({ data: Buffer.from(f.ctxReturnHex.padEnd(640, "0"), "hex") });
    expect(await poll(f)).toBe(true);
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => c[0])).toEqual([expect.objectContaining({ size: "483", price: 13.861751, side: "short", leg_index: 0 })]);
    expect(getAccountInfo).toHaveBeenCalledTimes(1);
  });

  it("zero fill: no row, no RPC", async () => {
    expect(await poll(fx("zeroNoMatcherCall"))).toBe(false);
    expect(insertTradeRow).not.toHaveBeenCalled();
    expect(getAccountInfo).not.toHaveBeenCalled();
    expect(recordSkipped).not.toHaveBeenCalled();
  });

  it("#213 tx, context since overwritten: matcher-requested size at the booked price, not recorded as skipped", async () => {
    const f = fx("issue213Partial");
    getAccountInfo.mockResolvedValue({ data: Buffer.from(fx("full").ctxReturnHex.padEnd(640, "0"), "hex") });
    expect(await poll(f)).toBe(true);
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => c[0])).toEqual([expect.objectContaining({ size: "822500", price: 121.580511 })]);
    expect(recordSkipped).not.toHaveBeenCalled();
  });

  it("an already stored leg costs NO matcher-context RPC read and writes nothing", async () => {
    const f = fx("partial");
    storedLegs.rows = [{ slab_address: market(f), asset_index: 0, leg_index: 0, trader: f.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100).accounts[0], side: "short", size: "483", is_liquidation: false }];
    getAccountInfo.mockResolvedValue({ data: Buffer.from(f.ctxReturnHex.padEnd(640, "0"), "hex") });
    expect(await poll(f)).toBe(false);
    expect(getAccountInfo).not.toHaveBeenCalled();
    expect(insertTradeRow).not.toHaveBeenCalled();
    storedLegs.rows = []; // negative control: not stored -> read + written
    expect(await poll(f)).toBe(true);
    expect(getAccountInfo).toHaveBeenCalledTimes(1);
  });

  it("strict mode: no row, signature recorded", async () => {
    process.env.TRADECPI_UNVERIFIED_SIZE = "skip";
    try {
      getAccountInfo.mockResolvedValue(null);
      expect(await poll(fx("issue213Partial"))).toBe(false);
      expect(insertTradeRow).not.toHaveBeenCalled();
      expect(recordSkipped).toHaveBeenCalledWith([expect.objectContaining({ signature: fx("issue213Partial").sig })]);
    } finally { delete process.env.TRADECPI_UNVERIFIED_SIZE; }
  });

  it("N3: context overwritten and the post-clip request (189226154805) is smaller than the wire size (378397480755): the REQUEST size is written", async () => {
    const f = fx("clipFull");
    getAccountInfo.mockResolvedValue({ data: Buffer.from(fx("full").ctxReturnHex.padEnd(640, "0"), "hex") });
    expect(await poll(f)).toBe(true);
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => (c[0] as any).size)).toEqual(["189226154805"]);
  });

  it("F2: a transport error is retried once (the signature throws so the cursor is held), then falls back to the request size", async () => {
    const f = fx("clipFull");
    getAccountInfo.mockRejectedValue(new Error("429"));
    const idx: any = new TradeIndexerPolling();
    await expect(idx.processTransaction(asWeb3(f), f.sig, market(f), new Set([PROGRAM]))).rejects.toThrow(/will be retried/);
    expect(insertTradeRow).not.toHaveBeenCalled();
    expect(await idx.processTransaction(asWeb3(f), f.sig, market(f), new Set([PROGRAM]))).toBe(true); // retry: fallback
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => (c[0] as any).size)).toEqual(["189226154805"]);
  });

  it("F2: a retry that finds the context readable writes the exact size", async () => {
    const f = fx("partial");
    const idx: any = new TradeIndexerPolling();
    getAccountInfo.mockRejectedValueOnce(new Error("timeout"));
    await expect(idx.processTransaction(asWeb3(f), f.sig, market(f), new Set([PROGRAM]))).rejects.toThrow();
    getAccountInfo.mockResolvedValue({ data: Buffer.from(f.ctxReturnHex.padEnd(640, "0"), "hex") });
    expect(await idx.processTransaction(asWeb3(f), f.sig, market(f), new Set([PROGRAM]))).toBe(true);
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => (c[0] as any).size)).toEqual(["483"]);
  });

  it("unrecognised evidence (no inner instructions): the wire size, as main writes today", async () => {
    const f = fx("clipFull");
    const w = asWeb3(f);
    w.meta.innerInstructions = undefined as any;
    getAccountInfo.mockResolvedValue(null);
    const idx: any = new TradeIndexerPolling();
    expect(await idx.processTransaction(w, f.sig, market(f), new Set([PROGRAM]))).toBe(true);
    expect(vi.mocked(insertTradeRow).mock.calls.map((c) => (c[0] as any).size)).toEqual(["378397480755"]);
    expect(recordSkipped).not.toHaveBeenCalled();
  });

  // ---- F4: through the REAL polling loop (indexTradesForSlab), not processTransaction directly ----
  describe("F4/F5: a transient context-read failure never loses the fill", () => {
    const dup = (f: any) => {
      // the same wrapper instruction twice (two same-side TradeCpi fills on one asset), with their inner groups
      const w = asWeb3(f);
      const ixs = w.transaction.message.instructions;
      const idx = ixs.findIndex((i: any) => i.programId && i.programId.toBase58?.() === PROGRAM && i.data && i.data.length > 100);
      const group = w.meta.innerInstructions.find((g: any) => g.index === idx);
      ixs.push(ixs[idx]);
      w.meta.innerInstructions.push({ index: ixs.length - 1, instructions: group.instructions });
      return w;
    };
    const slabOf = (f: any) => market(f);
    function wire(sigs: Array<{ f: any; w?: any }>) {
      getSignaturesForAddress.mockResolvedValue([...sigs].reverse().map((x) => ({ signature: x.f.sig, err: null })));
      getParsedTransactions.mockImplementation(async (list: string[]) => list.map((sig) => { const x = sigs.find((y) => y.f.sig === sig)!; return x.w ?? asWeb3(x.f); }));
    }
    const poll1 = async (idx: any, slab: string) => { await idx.indexTradesForSlab(slab); return idx.lastSignature.get(slab) as string | undefined; };
    const sizes = () => vi.mocked(insertTradeRow).mock.calls.map((c) => `${(c[0] as any).leg_index}:${(c[0] as any).size}`);

    it("poll 1: transport error -> no row, cursor NOT advanced; poll 2: the fallback row is written once and the cursor advances", async () => {
      const f = fx("clipFull");
      wire([{ f }]);
      getAccountInfo.mockRejectedValue(new Error("429"));
      const idx: any = new TradeIndexerPolling();
      expect(await poll1(idx, slabOf(f))).toBeUndefined();
      expect(insertTradeRow).not.toHaveBeenCalled();
      expect(await poll1(idx, slabOf(f))).toBe(f.sig); // retry falls back (cannot hold again) and the cursor moves
      expect(sizes()).toEqual(["0:189226154805"]);
      await poll1(idx, slabOf(f));
      expect(insertTradeRow).toHaveBeenCalledTimes(1); // written once
    });

    it("a held signature stops the window: a LATER signature is not processed (not even read) ahead of it, and nothing can hold the cursor twice", async () => {
      const a = fx("clipFull");
      const b = { ...a, sig: "5".repeat(88) }; // a second transaction on the same market, newer than a
      const bw = asWeb3(b);
      wire([{ f: a }, { f: b, w: bw }]); // oldest-first: a then b
      getAccountInfo.mockRejectedValue(new Error("timeout"));
      const idx: any = new TradeIndexerPolling();
      expect(await poll1(idx, slabOf(a))).toBeUndefined();
      expect(insertTradeRow).not.toHaveBeenCalled();
      expect(getAccountInfo).toHaveBeenCalledTimes(1); // b was never attempted ahead of the held a
      getAccountInfo.mockResolvedValue({ data: Buffer.from(fx("full").ctxReturnHex.padEnd(640, "0"), "hex") }); // RPC recovers
      expect(await poll1(idx, slabOf(a))).toBe(b.sig); // a falls back (cannot hold twice), b follows, cursor advances to the newest
      expect(sizes()).toEqual(["0:189226154805", "0:189226154805"]);
    });

    it("F5: two same-side fills, the first read errors: NONE of its later legs is written in that pass, and the retry writes both", async () => {
      const f = fx("clipFull");
      const w = dup(f);
      wire([{ f, w }]);
      getAccountInfo.mockRejectedValueOnce(new Error("timeout")).mockResolvedValue({ data: Buffer.from(fx("full").ctxReturnHex.padEnd(640, "0"), "hex") });
      const idx: any = new TradeIndexerPolling();
      await idx.indexTradesForSlab(slabOf(f));
      expect(insertTradeRow).not.toHaveBeenCalled(); // leg 1 was not written alone
      await idx.indexTradesForSlab(slabOf(f));
      expect(sizes().sort()).toEqual(["0:189226154805", "1:189226154805"]);
    });

    it("F5 mutation guard: leg 0 already stored, leg 1 genuinely new -> leg 1 is written (rank 2 > one stored row); `>= ordinal` must not become `>= 1`", async () => {
      const f = fx("clipFull");
      const w = dup(f);
      const wireIx = f.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100);
      storedLegs.rows = [{ slab_address: slabOf(f), asset_index: 0, leg_index: 0, trader: wireIx.accounts[0], side: "short", size: "189226154805", is_liquidation: false }];
      getAccountInfo.mockResolvedValue({ data: Buffer.from(fx("full").ctxReturnHex.padEnd(640, "0"), "hex") });
      const idx: any = new TradeIndexerPolling();
      expect(await idx.processTransaction(w, f.sig, slabOf(f), new Set([PROGRAM]))).toBe(true);
      expect(sizes()).toEqual(["1:189226154805"]);
    });
  });
});
