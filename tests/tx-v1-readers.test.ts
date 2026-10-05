/**
 * Transaction v1 (SIMD-0385) readers. Verified read-only on Helius devnet (slot 507817706) and mainnet
 * (slot 453655833): getTransaction with maxSupportedTransactionVersion 0 returns -32015 for a v1 tx, and
 * web3.js 1.98.4 cannot parse a v1 response even with 1. v1 has no lookup tables (meta.loadedAddresses is
 * empty/absent) and carries its budget in message.transactionConfig (priority fee = TOTAL lamports).
 */
import { describe, it, expect } from "vitest";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";
import { MessageV1, PublicKey, type VersionedTransactionResponse } from "@solana/web3.js";
import { normalizeRpcTransaction } from "../src/lpVault/decoder";

function walk(dir: string, out: string[] = []): string[] {
  for (const e of readdirSync(dir)) {
    const f = join(dir, e);
    if (statSync(f).isDirectory()) walk(f, out);
    else if (f.endsWith(".ts")) out.push(f);
  }
  return out;
}

describe("every transaction reader accepts v1", () => {
  it("no src file asks for maxSupportedTransactionVersion 0 (the RPC would answer -32015 for a v1 tx)", () => {
    const offenders = walk("src").filter((f) => /maxSupportedTransactionVersion:\s*0\b/.test(readFileSync(f, "utf8")));
    expect(offenders).toEqual([]);
  });
  it("@solana/web3.js is >= 1.99.0 (1.98.4 throws on a v1 getTransaction result)", () => {
    const v = JSON.parse(readFileSync("node_modules/@solana/web3.js/package.json", "utf8")).version as string;
    const [maj, min] = v.split(".").map(Number);
    expect(maj! > 1 || (maj === 1 && min! >= 99)).toBe(true);
  });
});

describe("normalizeRpcTransaction on a v1 response", () => {
  const k = (n: number) => new PublicKey(new Uint8Array(32).fill(n));
  const [payer, slab, prog] = [k(1), k(2), k(3)];
  const message = new MessageV1({
    header: { numRequiredSignatures: 1, numReadonlySignedAccounts: 0, numReadonlyUnsignedAccounts: 1 },
    staticAccountKeys: [payer, slab, prog],
    recentBlockhash: "11111111111111111111111111111111",
    compiledInstructions: [{ programIdIndex: 2, accountKeyIndexes: [0, 1], data: Uint8Array.from([63, 1, 2]) }],
    transactionConfig: { computeUnitLimit: 48, heapSize: null, loadedAccountsDataSizeLimit: 256000, priorityFee: 0 },
  });
  const resp = (meta: unknown) =>
    ({ slot: 507817706, blockTime: 1791223888, version: 1, transaction: { message, signatures: ["s"] }, meta }) as unknown as VersionedTransactionResponse;

  it("resolves accounts from the static keys when loadedAddresses is empty (the shape Helius returns)", () => {
    const n = normalizeRpcTransaction("sig", resp({ err: null, loadedAddresses: { writable: [], readonly: [] }, innerInstructions: [] }));
    expect(n.failed).toBe(false);
    expect(n.slot).toBe(507817706);
    expect(n.instructions).toHaveLength(1);
    expect(n.instructions[0]).toMatchObject({ programId: prog.toBase58(), accounts: [payer.toBase58(), slab.toBase58()] });
    expect([...n.instructions[0]!.data]).toEqual([63, 1, 2]);
  });
  it("also copes with loadedAddresses absent (jsonParsed shape)", () => {
    const n = normalizeRpcTransaction("sig", resp({ err: null, innerInstructions: [] }));
    expect(n.instructions[0]!.programId).toBe(prog.toBase58());
  });
  it("negative control: an out-of-range account index still throws instead of mis-attributing", () => {
    const bad = new MessageV1({
      header: message.header, staticAccountKeys: [payer, slab, prog], recentBlockhash: "11111111111111111111111111111111",
      compiledInstructions: [{ programIdIndex: 9, accountKeyIndexes: [0], data: new Uint8Array() }],
    });
    const r = { slot: 1, blockTime: 1, transaction: { message: bad, signatures: ["s"] }, meta: { err: null } } as unknown as VersionedTransactionResponse;
    expect(() => normalizeRpcTransaction("sig", r)).toThrow(/out of range/);
  });
});

import { fetchParsedTxsTolerant, isTransientRpcError, isUnreadableTxError } from "../src/lib/tolerantTxFetch";

// X-1: one unreadable transaction in a batch must never wedge the cursor, and must never be skipped silently.
describe("poison-pill tolerant fetch (X-1)", () => {
  const retry = <R>(fn: () => Promise<R>): Promise<R> => fn();
  const V2_ERR = Object.assign(new Error("failed to get transaction: Transaction version (2) is not supported by the requesting client. Please try the request again with the following configuration parameter: \"maxSupportedTransactionVersion\": 2"), { code: -32015 });
  const mk = (bad: Set<string>, transient?: Set<string>, batchError?: Error) => {
    const calls = { batch: 0, single: [] as string[] };
    return {
      calls,
      conn: {
        async getParsedTransactions(sigs: string[]) {
          calls.batch++;
          if (batchError) throw batchError;
          if (sigs.some((s) => bad.has(s))) throw V2_ERR; // web3.js throws for the WHOLE batch
          return sigs.map((s) => ({ sig: s }));
        },
        async getParsedTransaction(sig: string) {
          calls.single.push(sig);
          if (bad.has(sig)) throw V2_ERR;
          if (transient?.has(sig)) throw new Error("429 Too Many Requests");
          return { sig };
        },
      },
    };
  };
  const ten = Array.from({ length: 10 }, (_, i) => `s${i}`);

  it("one unreadable tx in a batch of 10 (10%): the others are returned, it is skipped, the cursor may advance", async () => {
    const { conn } = mk(new Set(["s3"]));
    const r = await fetchParsedTxsTolerant(conn, ten, retry);
    expect(r.failed).toBe(false);
    expect(r.txs.filter(Boolean)).toHaveLength(9);
    expect(r.txs[3]).toBeNull();
    expect(r.skipped.map((s) => s.signature)).toEqual(["s3"]);
  });
  it("a healthy batch is one call, nothing skipped", async () => {
    const { conn, calls } = mk(new Set());
    const r = await fetchParsedTxsTolerant(conn, ["a", "b"], retry);
    expect(r).toMatchObject({ failed: false, skipped: [] });
    expect(calls).toEqual({ batch: 1, single: [] });
  });

  describe("(b) mass-skip circuit breaker", () => {
    it("2 unreadable in 10 (>1 absolute): cursor held, NOTHING skipped", async () => {
      const { conn } = mk(new Set(["s1", "s2"]));
      const r = await fetchParsedTxsTolerant(conn, ten, retry);
      expect(r.failed).toBe(true);
      expect(r.skipped).toEqual([]);
      expect(r.massSkip).toEqual({ skipped: 2, total: 10 });
    });
    it("a batch where EVERY signature is unreadable: held, not skipped", async () => {
      const { conn } = mk(new Set(ten));
      const r = await fetchParsedTxsTolerant(conn, ten, retry);
      expect(r).toMatchObject({ failed: true, skipped: [] });
      expect(r.massSkip).toEqual({ skipped: 10, total: 10 });
    });
    it("1 in a batch of 3 (33% > 20%) is held; a lone signature that is unreadable is held too", async () => {
      expect((await fetchParsedTxsTolerant(mk(new Set(["s1"])).conn, ["s0", "s1", "s2"], retry)).failed).toBe(true);
      expect((await fetchParsedTxsTolerant(mk(new Set(["a"])).conn, ["a"], retry)).failed).toBe(true);
    });
    it("exactly 1 in 5 (20%, not more than 20%) is still skipped", async () => {
      const r = await fetchParsedTxsTolerant(mk(new Set(["s4"])).conn, ten.slice(0, 5), retry);
      expect(r.failed).toBe(false);
      expect(r.skipped).toHaveLength(1);
    });
  });

  describe("(e) no per-signature isolation on an unhealthy RPC", () => {
    it("rate-limit / network / gateway errors on the batch: cursor held, ZERO single fetches, nothing skipped", async () => {
      for (const m of ["429 Too Many Requests", "ECONNRESET", "fetch failed", "502 Bad Gateway", "503 Service Unavailable", "request timed out"]) {
        const { conn, calls } = mk(new Set(), undefined, new Error(m));
        const r = await fetchParsedTxsTolerant(conn, ten, retry);
        expect(r).toMatchObject({ failed: true, skipped: [] });
        expect(calls.single).toEqual([]);
      }
    });
    it("a non-version batch error (anything unrecognised) also holds without fan-out", async () => {
      const { conn, calls } = mk(new Set(), undefined, new Error("something unexpected"));
      expect((await fetchParsedTxsTolerant(conn, ten, retry)).failed).toBe(true);
      expect(calls.single).toEqual([]);
    });
    it("a TRANSIENT failure during isolation holds the cursor (#147), it is never skipped", async () => {
      const { conn } = mk(new Set(["s3"]), new Set(["s5"]));
      const r = await fetchParsedTxsTolerant(conn, ten, retry);
      expect(r.failed).toBe(true);
      expect(String((r.error as Error).message)).toContain("429");
    });
  });

  describe("(a) the classifier is narrow", () => {
    it("ONLY the node's -32015 / exact 'Transaction version (N) is not supported' for N above what we asked", () => {
      expect(isUnreadableTxError(V2_ERR)).toBe(true);
      expect(isUnreadableTxError(Object.assign(new Error("anything"), { code: -32015 }))).toBe(true);
      expect(isUnreadableTxError(new Error("Transaction version (2) is not supported by the requesting client"))).toBe(true);
    });
    it("probes that must NOT classify as unreadable", () => {
      for (const m of [
        "Invalid params: failed to deserialize request body",
        "Invalid params: maxSupportedTransactionVersion must be 0",
        "Transaction version (0) is not supported by the requesting client",
        "Transaction version (1) is not supported by the requesting client", // we asked for 1: a node anomaly, not a poison pill
        "At path: meta -- Expected an object, but received: undefined",
        "<html><body>502 Bad Gateway: Expected the value to satisfy a union</body></html>",
        "Reached end of buffer unexpectedly",
        "At path: version -- Expected the value to satisfy a union of `literal | literal`, but received: 1",
      ]) expect(isUnreadableTxError(new Error(m))).toBe(false);
      expect(isUnreadableTxError(Object.assign(new Error("Transaction version (0) is not supported"), { code: -32015 }))).toBe(false);
    });
    it("(0)-version complaint on EVERY signature of a batch never skips anything", async () => {
      const zero = new Error("Transaction version (0) is not supported by the requesting client");
      const conn = { getParsedTransactions: async () => { throw zero; }, getParsedTransaction: async () => { throw zero; } };
      const r = await fetchParsedTxsTolerant(conn, ten, retry);
      expect(r).toMatchObject({ failed: true, skipped: [] });
    });
    it("transient classifier", () => {
      for (const m of ["429 Too Many Requests", "ECONNRESET", "fetch failed", "502 Bad Gateway"]) expect(isTransientRpcError(new Error(m))).toBe(true);
      expect(isTransientRpcError(V2_ERR)).toBe(false);
    });
  });
});
