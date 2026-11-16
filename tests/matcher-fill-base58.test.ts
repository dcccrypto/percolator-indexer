/**
 * decodeBase58Fast (matcherFill.ts) must be byte-identical to the shared decodeBase58 it replaced on the
 * parse path: over every instruction-data string in the recorded fixtures, random buffers, and edge cases.
 */
import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { decodeBase58 } from "@percolatorct/shared";
import { decodeBase58Fast } from "../src/parsers/matcherFill.js";

const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
function encode(bytes: Uint8Array): string {
  let zeros = 0;
  while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const digits: number[] = [];
  for (let i = zeros; i < bytes.length; i++) {
    let carry = bytes[i];
    for (let j = 0; j < digits.length; j++) {
      carry += digits[j] << 8;
      digits[j] = carry % 58;
      carry = (carry / 58) | 0;
    }
    while (carry > 0) {
      digits.push(carry % 58);
      carry = (carry / 58) | 0;
    }
  }
  return "1".repeat(zeros) + digits.reverse().map((d) => ALPHABET[d]).join("");
}

function collectData(o: unknown, out: Set<string>): void {
  if (Array.isArray(o)) o.forEach((x) => collectData(x, out));
  else if (o && typeof o === "object") {
    for (const [k, v] of Object.entries(o)) {
      if (k === "data" && typeof v === "string") out.add(v);
      else collectData(v, out);
    }
  }
}

describe("decodeBase58Fast == shared decodeBase58", () => {
  it("every instruction data string in tests/fixtures/tradecpi", () => {
    const dir = new URL("./fixtures/tradecpi/", import.meta.url);
    const all = new Set<string>();
    for (const n of readdirSync(dir)) collectData(JSON.parse(readFileSync(new URL(n, dir), "utf8")), all);
    expect(all.size).toBeGreaterThan(20);
    for (const s of all) expect(decodeBase58Fast(s), s).toEqual(decodeBase58(s));
  });

  it("random buffers (with leading zeros) round-trip and match", () => {
    let seed = 12345;
    const rnd = () => ((seed = (seed * 1103515245 + 12345) & 0x7fffffff) >>> 0) / 0x7fffffff;
    for (let t = 0; t < 2000; t++) {
      const len = Math.floor(rnd() * 120);
      const b = Uint8Array.from({ length: len }, () => Math.floor(rnd() * 256));
      const zeros = Math.floor(rnd() * 4);
      for (let z = 0; z < Math.min(zeros, len); z++) b[z] = 0;
      const s = encode(b);
      expect(decodeBase58Fast(s)).toEqual(decodeBase58(s));
      expect(decodeBase58Fast(s)).toEqual(b);
    }
  });

  it("edge cases: empty, all ones, zero value, invalid characters, non-strings", () => {
    for (const s of ["", "1", "11", "111", "2", "12", "z", "zzzzzzzzzz", "1zzz", "0", "O", "I", "l", "a b", "abé", "\u{1F600}", "11O"]) {
      expect(decodeBase58Fast(s), JSON.stringify(s)).toEqual(decodeBase58(s));
    }
    expect(decodeBase58Fast(undefined as unknown as string)).toBeNull();
  });
});
