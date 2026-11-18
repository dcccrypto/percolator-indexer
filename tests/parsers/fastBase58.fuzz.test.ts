import { describe, it, expect } from "vitest";
import { decodeBase58 as sharedDecodeBase58 } from "@percolatorct/shared";
import { decodeBase58Fast } from "../../src/parsers/fastBase58.js";
import { encodeBase58, rng } from "./percolatorTxParser.fixtures.js";

/** Large differential run: decodeBase58Fast must return exactly what the shared decoder returns (value or null). */
describe("decodeBase58Fast fuzz vs shared decodeBase58", { timeout: 300_000 }, () => {
  const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  const BAD = "0OIl+/ -_=é☃\u0000￿";

  it("120k valid encodings: 32/64-byte keys and signatures, random lengths, leading zeros, extremes", () => {
    const r = rng(2026);
    let mismatches = 0;
    for (let i = 0; i < 120_000; i++) {
      const len = i % 3 === 0 ? 32 : i % 3 === 1 ? 64 : Math.floor(r() * 130);
      const mode = r();
      const bytes = Uint8Array.from({ length: len }, () =>
        mode < 0.2 ? 0xff : mode < 0.4 ? (r() < 0.5 ? 0 : 0xff) : r() < 0.1 ? 0 : Math.floor(r() * 256),
      );
      if (r() < 0.3) bytes.fill(0, 0, Math.floor(r() * (len + 1))); // leading zero bytes -> leading '1's
      const s = encodeBase58(bytes);
      const a = decodeBase58Fast(s);
      if (JSON.stringify(a && [...a]) !== JSON.stringify([...(sharedDecodeBase58(s) as Uint8Array)])) mismatches++;
      if (a === null || a.length !== bytes.length || a.some((v, k) => v !== bytes[k])) mismatches++;
    }
    expect(mismatches).toBe(0);
  });

  it("120k arbitrary strings: invalid characters, leading '1's, empty, all-'1', all-'z'", () => {
    const r = rng(2027);
    let mismatches = 0;
    const check = (s: string) => {
      const a = decodeBase58Fast(s);
      const b = sharedDecodeBase58(s);
      if (JSON.stringify(a && [...a]) !== JSON.stringify(b && [...b])) mismatches++;
      if ((a === null) !== (b === null)) mismatches++;
    };
    for (const s of ["", "1", "11", "z", "zzzz", "z".repeat(88), "1".repeat(64), "0", "O", "I", "l", " ", "1z1", "11111111z"]) check(s);
    for (let i = 0; i < 120_000; i++) {
      const n = Math.floor(r() * 100);
      const badRate = r() < 0.5 ? 0 : r() * 0.05;
      let s = "";
      for (let k = 0; k < n; k++) s += r() < badRate ? BAD[Math.floor(r() * BAD.length)] : ALPHABET[Math.floor(r() * 58)];
      if (r() < 0.3) s = "1".repeat(Math.floor(r() * 6)) + s;
      check(s);
    }
    expect(mismatches).toBe(0);
  });
});
