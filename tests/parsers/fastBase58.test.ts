import { describe, it, expect } from "vitest";
import { createRequire } from "node:module";
import { decodeBase58 as sharedDecodeBase58 } from "@percolatorct/shared";
import { decodeBase58Fast } from "../../src/parsers/fastBase58.js";

// bs58 is only a transitive dependency (via @solana/web3.js); resolve it from there rather than adding a dep.
const req = createRequire(require.resolve("@solana/web3.js"));
const bs58 = req("bs58") as {
  encode(b: Uint8Array): string;
  decodeUnsafe(s: string): Uint8Array | undefined;
};

/** Reference result: bs58's decodeUnsafe returns undefined where decodeBase58Fast returns null. */
function ref(s: string): number[] | null {
  const r = bs58.decodeUnsafe(s);
  return r === undefined ? null : Array.from(r);
}
function fast(s: string): number[] | null {
  const r = decodeBase58Fast(s);
  return r === null ? null : Array.from(r);
}

/** mulberry32 */
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

describe("decodeBase58Fast vs bs58", { timeout: 120_000 }, () => {
  it("20,000 random byte strings (len 0..100, random leading zeros) round-trip identically", () => {
    const r = rng(20261118);
    let mismatches = 0;
    for (let n = 0; n < 20_000; n++) {
      const len = Math.floor(r() * 101);
      const bytes = new Uint8Array(len);
      for (let i = 0; i < len; i++) bytes[i] = Math.floor(r() * 256);
      const z = Math.min(len, Math.floor(r() * 4) === 0 ? Math.floor(r() * 5) : 0);
      for (let i = 0; i < z; i++) bytes[i] = 0;
      const s = bs58.encode(bytes);
      const got = decodeBase58Fast(s);
      if (got === null || got.length !== len || !bytes.every((b, i) => got[i] === b)) mismatches++;
    }
    expect(mismatches).toBe(0);
  });

  it("20,000 random strings over the alphabet plus junk characters agree with bs58 (valid and invalid)", () => {
    const r = rng(7);
    const alpha = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    const junk = ["0", "I", "O", "l", " ", "+", "/", "=", "é", "\u0000", "ÿ", "\u0080", "\u{1F600}"];
    let mismatches = 0;
    for (let n = 0; n < 20_000; n++) {
      const len = Math.floor(r() * 90);
      let s = "";
      const bad = r() < 0.3;
      for (let i = 0; i < len; i++) {
        s += bad && r() < 0.05 ? junk[Math.floor(r() * junk.length)] : alpha[Math.floor(r() * 58)];
      }
      if (JSON.stringify(fast(s)) !== JSON.stringify(ref(s))) mismatches++;
    }
    expect(mismatches).toBe(0);
  });

  it("edge cases: empty, all zero bytes, leading '1's, single chars", () => {
    expect(Array.from(decodeBase58Fast("")!)).toEqual([]);
    for (const n of [1, 2, 3, 4, 5, 31, 32, 64, 600]) {
      const zeros = new Uint8Array(n);
      expect(fast(bs58.encode(zeros))).toEqual(ref("1".repeat(n)));
      expect(decodeBase58Fast("1".repeat(n))).toEqual(zeros);
    }
    for (const s of ["1", "11", "2", "z", "12", "112", "1z", "111z", "z1", "zzzz", "zzzzz", "1111111111112"]) {
      expect(fast(s)).toEqual(ref(s));
    }
  });

  it("invalid characters: 0, I, O, l, in every position, and non-ASCII", () => {
    for (const bad of ["0", "I", "O", "l", "\u0080", "é", "\u{1F600}", " ", "\n"]) {
      for (const prefix of ["", "1", "11", "abc", "abcd", "abcde"]) {
        for (const suffix of ["", "2", "abcd"]) {
          const s = prefix + bad + suffix;
          expect(decodeBase58Fast(s)).toBeNull();
          expect(fast(s)).toEqual(ref(s));
        }
      }
    }
    expect(decodeBase58Fast("1111I")).toBeNull();
  });

  it("32-byte keys, 64-byte signatures and over-long input (grows the scratch buffer)", () => {
    const r = rng(99);
    for (const len of [32, 64, 65, 400, 511, 512, 513, 1000]) {
      const bytes = new Uint8Array(len);
      for (let i = 0; i < len; i++) bytes[i] = Math.floor(r() * 256);
      const s = bs58.encode(bytes);
      expect(Array.from(decodeBase58Fast(s)!)).toEqual(Array.from(bytes));
      expect(fast(s)).toEqual(ref(s));
    }
    // a long all-'z' string, then a short one afterwards: scratch is reused dirty, result must still be exact
    const longS = "z".repeat(700);
    expect(fast(longS)).toEqual(ref(longS));
    expect(fast("2")).toEqual(ref("2"));
    expect(fast("zz")).toEqual(ref("zz"));
    // invalid input after a successful decode leaves no state behind
    expect(decodeBase58Fast("abc0")).toBeNull();
    expect(fast(longS)).toEqual(ref(longS));
  });

  it("non-string input is delegated to the shared decoder unchanged", () => {
    for (const v of [undefined, null, 42, {}, [], ["1"]] as unknown[]) {
      let expected: unknown;
      let expectedThrew = false;
      try {
        expected = sharedDecodeBase58(v as never);
      } catch {
        expectedThrew = true;
      }
      if (expectedThrew) {
        expect(() => decodeBase58Fast(v as never)).toThrow();
      } else {
        expect(decodeBase58Fast(v as never)).toEqual(expected);
      }
    }
  });
});
