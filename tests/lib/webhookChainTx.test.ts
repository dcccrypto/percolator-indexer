import { describe, it, expect, vi, beforeEach } from "vitest";
import { PublicKey } from "@solana/web3.js";

vi.mock("@percolatorct/shared", async (orig) => ({
  ...(await orig<typeof import("@percolatorct/shared")>()),
  withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
}));

import { CHAIN_TX_BATCH, enhancedFromParsedTx, loadChainTxs, resetChainTxMemo, txsFromChain, ChainTxReadError } from "../../src/lib/webhookChainTx.js";

const WRAPPER = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
const MATCHER = "Stake11111111111111111111111111111111111111";
const TRADER = "So11111111111111111111111111111111111111112";
const SLAB = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const ATTACKER = "SysvarRent111111111111111111111111111111111";
const IDS = new Set([WRAPPER]);
const sigN = (n: number) => (String(n % 9 + 1) + "A".repeat(87)).slice(0, 88).replace(/A{6}$/, String(100000 + n).replace(/0/g, "B"));

function parsedTx(trader = TRADER, opts: { err?: unknown; logs?: string[]; meta?: boolean } = {}): any {
  return {
    slot: 42,
    blockTime: 1_794_000_000,
    transaction: {
      message: {
        instructions: [
          { programId: new PublicKey("11111111111111111111111111111111"), program: "system", parsed: { type: "transfer" } },
          { programId: new PublicKey(WRAPPER), accounts: [new PublicKey(trader), new PublicKey(SLAB)], data: "abc" },
        ],
      },
    },
    meta: opts.meta === false ? null : {
      err: opts.err ?? null,
      logMessages: opts.logs ?? ["Program log: x"],
      innerInstructions: [{ index: 1, instructions: [{ programId: new PublicKey(MATCHER), accounts: [new PublicKey(SLAB)], data: "def" }] }],
    },
  };
}

function conn(answer: (sigs: string[]) => any[] | Promise<any[]>) {
  return {
    getParsedTransactions: vi.fn(async (sigs: string[]) => answer(sigs)),
    getParsedTransaction: vi.fn(async () => null),
  };
}

beforeEach(() => resetChainTxMemo());

describe("enhancedFromParsedTx", () => {
  it("rebuilds the enhanced shape from the chain tx: base58 keys, CPIs nested under their instruction, slot/time/err/logs from chain", () => {
    const e = enhancedFromParsedTx("S", parsedTx(TRADER, { err: { InstructionError: [1, "x"] }, logs: ["a", "b"] }))!;
    expect(e).toMatchObject({ signature: "S", slot: 42, timestamp: 1_794_000_000, transactionError: { InstructionError: [1, "x"] }, accountData: [], innerInstructions: [], logMessages: ["a", "b"] });
    expect(e.instructions[0]).toEqual({ programId: "11111111111111111111111111111111", accounts: [], innerInstructions: [] });
    expect(e.instructions[1]).toEqual({ programId: WRAPPER, data: "abc", accounts: [TRADER, SLAB], innerInstructions: [{ programId: MATCHER, data: "def", accounts: [SLAB] }] });
  });

  it("returns null for a tx without meta (status unknown)", () => {
    expect(enhancedFromParsedTx("S", parsedTx(TRADER, { meta: false }))).toBeNull();
  });

  it("keeps instruction indexes aligned when an instruction has no readable programId", () => {
    const tx = parsedTx();
    tx.transaction.message.instructions.unshift({ programId: 7 });
    const e = enhancedFromParsedTx("S", tx)!;
    expect(e.instructions.map((i) => i.programId)).toEqual(["", "11111111111111111111111111111111", WRAPPER]);
    expect(e.instructions[2].innerInstructions).toEqual([]); // inner group index 1 now belongs to the system ix
  });
});

describe("loadChainTxs", () => {
  it(`fetches ${CHAIN_TX_BATCH} signatures per call, dedupes, and memoises found txs (a redelivery costs no call)`, async () => {
    const sigs = Array.from({ length: 250 }, (_, i) => sigN(i));
    const c = conn((s) => s.map(() => parsedTx()));
    const m = await loadChainTxs(c, [...sigs, sigs[0]]);
    expect(c.getParsedTransactions).toHaveBeenCalledTimes(3);
    expect(c.getParsedTransactions.mock.calls.map((x) => x[0].length)).toEqual([100, 100, 50]);
    expect(m.size).toBe(250);
    await loadChainTxs(c, sigs);
    expect(c.getParsedTransactions).toHaveBeenCalledTimes(3);
  });

  it("does not memoise a miss: a signature the RPC did not have yet is asked again next delivery", async () => {
    const c = conn((s) => s.map(() => null));
    expect((await loadChainTxs(c, [sigN(1)])).get(sigN(1))).toBeNull();
    await loadChainTxs(c, [sigN(1)]);
    expect(c.getParsedTransactions).toHaveBeenCalledTimes(2);
  });

  it("throws ChainTxReadError when the batch read fails (transport / rate limit)", async () => {
    const c = conn(() => { throw new Error("429 Too Many Requests"); });
    await expect(loadChainTxs(c, [sigN(1)])).rejects.toBeInstanceOf(ChainTxReadError);
  });
});

describe("txsFromChain", () => {
  it("uses the chain tx for every confirmed signature, drops unknown and malformed ones, flags a payload that differs", async () => {
    const real = sigN(1), forged = sigN(2);
    const c = conn((s) => s.map((x) => (x === real ? parsedTx(TRADER) : null)));
    const out = await txsFromChain([
      { signature: real, instructions: [{ programId: WRAPPER, data: "abc", accounts: [ATTACKER, SLAB] }] },
      { signature: forged, instructions: [{ programId: WRAPPER, data: "abc", accounts: [TRADER, SLAB] }] },
      { signature: "not-a-sig" },
      {},
    ], c, IDS);
    expect(c.getParsedTransactions).toHaveBeenCalledWith([real, forged], { maxSupportedTransactionVersion: 1 });
    expect(out.txs).toHaveLength(1);
    expect(out.txs[0].instructions[1].accounts).toEqual([TRADER, SLAB]);
    expect(out.unknown).toEqual([forged, "not-a-sig", ""]);
    expect(out.mismatched).toEqual([real]);
  });

  it("a payload that matches the chain is not flagged; logs are carried only when the payload carried them", async () => {
    const a = sigN(3), b = sigN(4);
    const c = conn((s) => s.map(() => parsedTx(TRADER, { logs: ["chain log"] })));
    const ix = [{ programId: "11111111111111111111111111111111" }, { programId: WRAPPER, data: "abc", accounts: [TRADER, SLAB] }];
    const out = await txsFromChain([
      { signature: a, instructions: ix, logMessages: ["forged log"] },
      { signature: b, instructions: ix },
    ], c, IDS);
    expect(out.mismatched).toEqual([]);
    expect(out.txs[0].logMessages).toEqual(["chain log"]);
    expect(out.txs[1].logMessages).toBeUndefined();
  });

  it("makes no RPC call for a delivery without a well-formed signature", async () => {
    const c = conn(() => []);
    expect(await txsFromChain([{ signature: "x" }], c, IDS)).toEqual({ txs: [], unknown: ["x"], mismatched: [] });
    expect(c.getParsedTransactions).not.toHaveBeenCalled();
  });
});

describe("loadChainTxs partial failure", () => {
  it("memoises the chunks that came back before throwing, so the redelivery re-reads only the failed chunk", async () => {
    const sigs = Array.from({ length: 150 }, (_, i) => sigN(i));
    let call = 0;
    const c = conn((s) => { call++; if (call === 1) throw new Error("socket hang up"); return s.map(() => parsedTx()); });
    await expect(loadChainTxs(c, sigs)).rejects.toBeInstanceOf(ChainTxReadError);
    const m = await loadChainTxs(c, sigs);
    expect(m.size).toBe(150);
    expect(c.getParsedTransactions.mock.calls.map((x) => x[0].length)).toEqual([100, 50, 100]);
  });
});
