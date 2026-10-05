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
