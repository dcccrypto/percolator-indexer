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

import { breakerAlertPolls, createBreakerTracker, fetchParsedTxsTolerant, isPoisonTxError, isTransientRpcError, isUnreadableTxError } from "../src/lib/tolerantTxFetch";

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

  it("one unreadable tx in a batch of 10: the others are returned, it is skipped, the cursor may advance", async () => {
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

  describe("(b) mass-skip circuit breaker: successful-sibling rule", () => {
    it("2 poison + 1 good: both poison skipped, the good one returned, the cursor advances", async () => {
      const r = await fetchParsedTxsTolerant(mk(new Set(["p1", "p2"])).conn, ["p1", "good", "p2"], retry);
      expect(r.failed).toBe(false);
      expect(r.skipped.map((x) => x.signature)).toEqual(["p1", "p2"]);
      expect(r.txs).toEqual([null, { sig: "good" }, null]);
    });
    it("up to 5 skips with a good sibling are allowed; a sixth holds the cursor and skips nothing", async () => {
      const five = ["a", "b", "c", "d", "e"];
      const ok = await fetchParsedTxsTolerant(mk(new Set(five)).conn, [...five, "good"], retry);
      expect(ok.failed).toBe(false);
      expect(ok.skipped).toHaveLength(5);
      const six = [...five, "f"];
      const held = await fetchParsedTxsTolerant(mk(new Set(six)).conn, [...six, "good"], retry);
      expect(held).toMatchObject({ failed: true, skipped: [] });
      expect(held.massSkip).toEqual({ skipped: 6, total: 7 });
    });
    it("an ALL-unreadable batch is held, not skipped (any size, including 1)", async () => {
      for (const n of [1, 3, 10]) {
        const sigs = ten.slice(0, n);
        const r = await fetchParsedTxsTolerant(mk(new Set(sigs)).conn, sigs, retry);
        expect(r).toMatchObject({ failed: true, skipped: [] });
        expect(r.massSkip).toEqual({ skipped: n, total: n });
      }
    });
    it("a sibling that returns null (not found) is NOT proof the reader works: still held", async () => {
      const conn = { getParsedTransactions: async () => { throw V2_ERR; }, getParsedTransaction: async (sig: string) => { if (sig === "p") throw V2_ERR; return null; } };
      expect((await fetchParsedTxsTolerant(conn, ["p", "nullsig"], retry)).failed).toBe(true);
    });
    it("a signature is only skipped when it is unreadable on its OWN retry: a batch-level version error whose single retries succeed skips nothing", async () => {
      const conn = { getParsedTransactions: async () => { throw V2_ERR; }, getParsedTransaction: async (sig: string) => ({ sig }) };
      const r = await fetchParsedTxsTolerant(conn, ["a", "b", "c"], retry);
      expect(r).toMatchObject({ failed: false, skipped: [] });
    });
  });

  describe("breaker-held alert after K consecutive polls", () => {
    it("fires at K (and every K) consecutive holds; any clear resets the count", () => {
      const alerts: Array<[string, number]> = [];
      const t = createBreakerTracker(3, (k, n) => alerts.push([k, n]));
      t.held("slabA"); t.held("slabA");
      expect(alerts).toEqual([]);
      t.held("slabA");
      expect(alerts).toEqual([["slabA", 3]]);
      t.clear("slabA");
      t.held("slabA"); t.held("slabA");
      expect(alerts).toHaveLength(1); // reset: 2 polls since the clear
      t.held("slabA");
      expect(alerts).toHaveLength(2);
      t.held("slabB");
      expect(alerts).toHaveLength(2); // keys are independent
      for (let i = 0; i < 3; i++) t.held("slabA");
      expect(alerts.at(-1)).toEqual(["slabA", 6]);
    });
    it("does not alert on every poll after K, only at multiples of K (no alert spam)", () => {
      const alerts: number[] = [];
      const t = createBreakerTracker(3, (_k, n) => alerts.push(n));
      for (let i = 0; i < 5; i++) t.held("x");
      expect(alerts).toEqual([3]);
    });
    it("K comes from INDEXER_BREAKER_ALERT_POLLS (default 10; junk falls back)", () => {
      expect(breakerAlertPolls({})).toBe(10);
      expect(breakerAlertPolls({ INDEXER_BREAKER_ALERT_POLLS: "4" })).toBe(4);
      expect(breakerAlertPolls({ INDEXER_BREAKER_ALERT_POLLS: "0" })).toBe(10);
      expect(breakerAlertPolls({ INDEXER_BREAKER_ALERT_POLLS: "x" })).toBe(10);
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
    it("(E) transient wins on the per-signature path too: a 502 echoing 'version (2)', or -32015 with 429 text, is NOT a poison pill", async () => {
      const bad502 = new Error("502 Bad Gateway: upstream said Transaction version (2) is not supported");
      const code429 = Object.assign(new Error("429 Too Many Requests while reading: Transaction version (2) is not supported"), { code: -32015 });
      expect(isPoisonTxError(bad502)).toBe(false);
      expect(isPoisonTxError(code429)).toBe(false);
      expect(isPoisonTxError(V2_ERR)).toBe(true);
      for (const e of [bad502, code429]) {
        // batch fails with a version error, then the per-signature retry of EVERY signature fails with the transient lookalike
        const conn = { getParsedTransactions: async () => { throw V2_ERR; }, getParsedTransaction: async (sig: string) => { if (sig === "p") throw e; return { sig }; } };
        const r = await fetchParsedTxsTolerant(conn, ["good", "p"], retry);
        expect(r).toMatchObject({ failed: true, skipped: [] });
      }
    });
    it("transient classifier", () => {
      for (const m of ["429 Too Many Requests", "ECONNRESET", "fetch failed", "502 Bad Gateway"]) expect(isTransientRpcError(new Error(m))).toBe(true);
      expect(isTransientRpcError(V2_ERR)).toBe(false);
    });
  });
});
