import { readFileSync, readdirSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { Keypair, Message, PublicKey, type VersionedTransactionResponse } from "@solana/web3.js";
import { LP_VAULT_TAG, TOKEN_PROGRAM_ID, normalizeRpcTransaction, type NormIx, type NormTopIx, type NormTx } from "../../src/lpVault/decoder.js";

export const WRAPPER = "GnwdeQrAh4qzChJeVLrM21CXXWC1akjLH3DiijwzEEYZ";
export const WRAPPERS: ReadonlySet<string> = new Set([WRAPPER]);
const FIX_DIR = join(dirname(fileURLToPath(import.meta.url)), "..", "fixtures", "lp-vault");

/** Deterministic PRNG (mulberry32). */
export function rng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function loadFixtureTxs(): NormTx[] {
  return readdirSync(FIX_DIR).filter((f) => f.endsWith(".json")).sort().map((f) => {
    const sig = f.replace(/\.json$/, "");
    const raw = JSON.parse(readFileSync(join(FIX_DIR, f), "utf8"));
    const resp = {
      slot: raw.slot,
      blockTime: raw.blockTime,
      transaction: { signatures: raw.transaction.signatures, message: new Message(raw.transaction.message) },
      meta: {
        err: raw.meta.err,
        innerInstructions: raw.meta.innerInstructions,
        loadedAddresses: {
          writable: (raw.meta.loadedAddresses?.writable ?? []).map((k: string) => new PublicKey(k)),
          readonly: (raw.meta.loadedAddresses?.readonly ?? []).map((k: string) => new PublicKey(k)),
        },
      },
    } as unknown as VersionedTransactionResponse;
    return normalizeRpcTransaction(sig, resp);
  });
}

function le(v: bigint, n: number): number[] {
  const o: number[] = [];
  for (let i = 0; i < n; i++) o.push(Number((v >> BigInt(8 * i)) & 0xffn));
  return o;
}

/** `n` synthetic transactions mixing all four event kinds, noise, inner-CPI wrappers, null heights. */
export function synthTxs(n: number, seed = 1): NormTx[] {
  const r = rng(seed);
  const keys = Array.from({ length: 256 }, (_, i) => Keypair.fromSeed(Uint8Array.from({ length: 32 }, (_, j) => (i * 7 + j * 13 + seed) & 0xff)).publicKey.toBase58());
  const k = (): string => keys[Math.floor(r() * keys.length)]!;
  const big = (): bigint => {
    let v = 0n;
    const bytes = 1 + Math.floor(r() * 16);
    for (let i = 0; i < bytes; i++) v = (v << 8n) | BigInt(Math.floor(r() * 256));
    return v;
  };
  const spl = (tag: number, accts: string[], amt: bigint, h: number | null): NormIx => ({
    programId: TOKEN_PROGRAM_ID, accounts: accts, data: Uint8Array.from([tag, ...le(amt & ((1n << 64n) - 1n), 8)]), stackHeight: h,
  });
  const out: NormTx[] = [];
  for (let t = 0; t < n; t++) {
    const kind = Math.floor(r() * 5);
    const h = r() < 0.1 ? null : 2;
    const user = k(), market = k(), registry = k(), mint = k(), ata = k(), src = k(), vt = k(), escrow = k(), dest = k();
    let ix: NormIx;
    let inner: NormIx[] = [];
    if (kind === 0) {
      const amt = big();
      ix = { programId: WRAPPER, accounts: [user, market, registry, mint, ata, src, vt, k(), TOKEN_PROGRAM_ID, k(), k()],
        data: Uint8Array.from([LP_VAULT_TAG.DepositToLpVault, ...le(amt, 16), ...le(BigInt(Math.floor(r() * 65536)), 2)]), stackHeight: 1 };
      inner = [spl(3, [src, vt, user], r() < 0.05 ? amt + 1n : amt, h), spl(r() < 0.5 ? 7 : 14, [mint, ata, k()], big(), h)];
    } else if (kind === 1) {
      ix = { programId: WRAPPER, accounts: [user, registry, mint, ata, escrow, k(), TOKEN_PROGRAM_ID, k()],
        data: Uint8Array.from([LP_VAULT_TAG.RequestRedeemLpShares, ...le(big(), 16)]), stackHeight: 1 };
      inner = [spl(3, [ata, escrow, user], big(), h)];
    } else if (kind === 2) {
      ix = { programId: WRAPPER, accounts: [k(), market, registry, k(), mint, escrow, vt, k(), k(), dest, TOKEN_PROGRAM_ID, k(), user],
        data: Uint8Array.from([LP_VAULT_TAG.ExecuteRedemption, ...le(BigInt(Math.floor(r() * 65536)), 2)]), stackHeight: 1 };
      inner = [spl(r() < 0.5 ? 3 : 12, r() < 0.5 ? [vt, dest, k()] : [vt, k(), dest, k()], big(), h), spl(r() < 0.5 ? 8 : 15, [escrow, mint, k()], big(), h)];
    } else if (kind === 3) {
      ix = { programId: WRAPPER, accounts: [user, registry, k(), mint, ata, escrow, TOKEN_PROGRAM_ID],
        data: Uint8Array.from([LP_VAULT_TAG.CancelRedemption]), stackHeight: 1 };
      inner = [spl(3, [escrow, ata, k()], big(), h)];
    } else {
      ix = { programId: k(), accounts: [k(), k()], data: Uint8Array.from([Math.floor(r() * 256), 1, 2, 3]), stackHeight: 1 };
    }
    const top: NormTopIx = { ...ix, inner };
    // Half the time, nest the wrapper ix as a CPI under an unrelated top-level program.
    const instructions: NormTopIx[] = r() < 0.3
      ? [{ programId: k(), accounts: [k()], data: Uint8Array.of(9), stackHeight: 1, inner: [{ ...ix, stackHeight: 2 }, ...inner.map((x) => ({ ...x, stackHeight: x.stackHeight === null ? null : 3 }))] }]
      : [top];
    out.push({ signature: `s${t}`, slot: 1000 + t, blockTime: r() < 0.1 ? null : 1_700_000_000 + t, failed: r() < 0.03, instructions });
  }
  return out;
}

export function buildCorpus(total = 20_000): NormTx[] {
  const fx = loadFixtureTxs();
  const out: NormTx[] = [];
  const syn = synthTxs(total - fx.length * 200);
  for (let i = 0; i < 200; i++) out.push(...fx);
  out.push(...syn);
  return out.slice(0, total);
}
