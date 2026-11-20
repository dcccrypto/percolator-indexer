/**
 * Differential: the byte-compare parseRedemptionShares and memoised deriveRedemptionPda
 * must equal the original implementations on random inputs (0 mismatches).
 */
import { describe, expect, it } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { HEADER_LEN, KIND_LP_REDEMPTION, LP_REDEMPTION_LEN, deriveRedemptionPda, parseRedemptionShares } from "../../src/lpVault/chain.js";

function refParse(data: Uint8Array | null, registry: string, user: string): bigint {
  if (!data || data.length < LP_REDEMPTION_LEN || data[10] !== KIND_LP_REDEMPTION) return 0n;
  if (data.subarray(0, 8).every((b) => b === 0)) return 0n;
  const reg = new PublicKey(data.subarray(HEADER_LEN, HEADER_LEN + 32)).toBase58();
  const who = new PublicKey(data.subarray(HEADER_LEN + 32, HEADER_LEN + 64)).toBase58();
  if (reg !== registry || who !== user) return 0n;
  let v = 0n;
  for (let i = 15; i >= 0; i--) v = (v << 8n) | BigInt(data[HEADER_LEN + 64 + i]!);
  return v;
}
function refPda(programId: string, registry: string, user: string): PublicKey {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("lp_redemption"), new PublicKey(registry).toBuffer(), new PublicKey(user).toBuffer()],
    new PublicKey(programId),
  )[0];
}

// mulberry32 for reproducible randomness
function rng(seed: number) {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

describe("lpVault chain differential", () => {
  it("parseRedemptionShares matches the original over random inputs", () => {
    const r = rng(1234);
    const byte = () => Math.floor(r() * 256);
    const keys = Array.from({ length: 12 }, () => new PublicKey(Uint8Array.from({ length: 32 }, byte)));
    keys.push(new PublicKey(new Uint8Array(32))); // all-zero key ("1111...")
    const bad = ["", "not-base58-0OIl", "1", keys[0]!.toBase58() + "1", keys[0]!.toBase58().slice(1)];
    let mismatches = 0;
    for (let n = 0; n < 6000; n++) {
      const reg = keys[Math.floor(r() * keys.length)]!;
      const usr = keys[Math.floor(r() * keys.length)]!;
      const len = r() < 0.1 ? Math.floor(r() * LP_REDEMPTION_LEN) : LP_REDEMPTION_LEN + (r() < 0.1 ? 5 : 0);
      const d = new Uint8Array(len);
      for (let i = 0; i < len; i++) d[i] = byte();
      if (len > 10) d[10] = r() < 0.85 ? KIND_LP_REDEMPTION : byte();
      if (r() < 0.2) d.fill(0, 0, 8); // zeroed magic
      if (len >= HEADER_LEN + 64) {
        const wantReg = r() < 0.7 ? reg : keys[Math.floor(r() * keys.length)]!;
        const wantUsr = r() < 0.7 ? usr : keys[Math.floor(r() * keys.length)]!;
        d.set(wantReg.toBytes(), HEADER_LEN);
        d.set(wantUsr.toBytes(), HEADER_LEN + 32);
        if (r() < 0.05) d[HEADER_LEN + 31 - Math.floor(r() * 32)] ^= 1; // one-bit near miss
      }
      const regStr = r() < 0.03 ? bad[Math.floor(r() * bad.length)]! : reg.toBase58();
      const usrStr = r() < 0.03 ? bad[Math.floor(r() * bad.length)]! : usr.toBase58();
      const input = r() < 0.03 ? null : d;
      if (parseRedemptionShares(input, regStr, usrStr) !== refParse(input, regStr, usrStr)) mismatches++;
    }
    expect(mismatches).toBe(0);
  });

  it("matches at least some non-zero results (test is not vacuous)", () => {
    const k = new PublicKey(Uint8Array.from({ length: 32 }, (_, i) => i + 1));
    const d = new Uint8Array(LP_REDEMPTION_LEN);
    d.fill(7, 0, 8);
    d[10] = KIND_LP_REDEMPTION;
    d.set(k.toBytes(), HEADER_LEN);
    d.set(k.toBytes(), HEADER_LEN + 32);
    d[HEADER_LEN + 64] = 9;
    expect(parseRedemptionShares(d, k.toBase58(), k.toBase58())).toBe(9n);
    expect(refParse(d, k.toBase58(), k.toBase58())).toBe(9n);
  });

  it("deriveRedemptionPda memo equals the original, hit or miss, and errors identically", () => {
    const r = rng(99);
    const mk = () => new PublicKey(Uint8Array.from({ length: 32 }, () => Math.floor(r() * 256))).toBase58();
    const progs = [mk(), mk()], regs = [mk(), mk(), mk()], users = Array.from({ length: 20 }, mk);
    let mismatches = 0;
    for (let n = 0; n < 400; n++) {
      const p = progs[n % 2]!, g = regs[n % 3]!, u = users[Math.floor(r() * users.length)]!;
      if (!deriveRedemptionPda(p, g, u).equals(refPda(p, g, u))) mismatches++;
    }
    expect(mismatches).toBe(0);
    expect(() => deriveRedemptionPda(progs[0]!, "bad!", users[0]!)).toThrow();
    expect(() => refPda(progs[0]!, "bad!", users[0]!)).toThrow();
  });
});
