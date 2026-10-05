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

import { fetchParsedTxsTolerant, isUnreadableTxError } from "../src/lib/tolerantTxFetch";

// X-1: one unreadable transaction in a batch must never wedge the cursor.
describe("poison-pill tolerant fetch (X-1)", () => {
  const retry = <R>(fn: () => Promise<R>): Promise<R> => fn();
  const V1_ERR = new Error("failed to get transaction: Transaction version (1) is not supported by the requesting client. Please try the request again with the following configuration parameter: \"maxSupportedTransactionVersion\": 1");
  const mk = (bad: Set<string>, transient?: Set<string>) => {
    const calls = { batch: 0, single: [] as string[] };
    return {
      calls,
      conn: {
        async getParsedTransactions(sigs: string[]) {
          calls.batch++;
          if (sigs.some((s) => bad.has(s))) throw V1_ERR; // web3.js throws for the WHOLE batch
          return sigs.map((s) => ({ sig: s }));
        },
        async getParsedTransaction(sig: string) {
          calls.single.push(sig);
          if (bad.has(sig)) throw V1_ERR;
          if (transient?.has(sig)) throw new Error("429 Too Many Requests");
          return { sig };
        },
      },
    };
  };

  it("a batch containing an unreadable tx: the others are returned, the poison one is skipped and logged, the cursor may advance", async () => {
    const { conn } = mk(new Set(["b"]));
    const r = await fetchParsedTxsTolerant(conn, ["a", "b", "c"], retry);
    expect(r.failed).toBe(false);
    expect(r.txs).toEqual([{ sig: "a" }, null, { sig: "c" }]);
    expect(r.skipped.map((s) => s.signature)).toEqual(["b"]);
  });
  it("a healthy batch is one call, nothing skipped", async () => {
    const { conn, calls } = mk(new Set());
    const r = await fetchParsedTxsTolerant(conn, ["a", "b"], retry);
    expect(r).toMatchObject({ failed: false, skipped: [] });
    expect(calls).toEqual({ batch: 1, single: [] });
  });
  it("a single-signature batch that is unreadable is skipped too", async () => {
    const { conn } = mk(new Set(["a"]));
    const r = await fetchParsedTxsTolerant(conn, ["a"], retry);
    expect(r).toMatchObject({ failed: false, txs: [null] });
    expect(r.skipped).toHaveLength(1);
  });
  it("negative control: a TRANSIENT failure still holds the cursor (#147 unchanged), it is never skipped", async () => {
    const { conn } = mk(new Set(["b"]), new Set(["c"]));
    const r = await fetchParsedTxsTolerant(conn, ["a", "b", "c"], retry);
    expect(r.failed).toBe(true);
    expect(String((r.error as Error).message)).toContain("429");
    expect(r.skipped.map((s) => s.signature)).toEqual(["b"]);
    const whole = await fetchParsedTxsTolerant({ ...conn, getParsedTransactions: async () => { throw new Error("ECONNRESET"); } }, ["a"], retry);
    expect(whole.failed).toBe(true);
    expect(whole.skipped).toEqual([]);
  });
  it("classifies unreadable-tx errors narrowly", () => {
    for (const m of ["Transaction version (1) is not supported by the requesting client", "-32015 something", "At path: version -- Expected the value to satisfy a union of `literal | literal`, but received: 1"]) expect(isUnreadableTxError(new Error(m))).toBe(true);
    for (const m of ["429 Too Many Requests", "ECONNRESET", "503 Service Unavailable", "Node is behind by 120 slots"]) expect(isUnreadableTxError(new Error(m))).toBe(false);
  });
});
