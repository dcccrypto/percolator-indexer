// Pins for normalizeProgramIds / parseMarketsFilter and the call sites that read the program list.
// Each test kills a mutant that survived programIds-helper.test.ts and `vitest related` (M04 M06 M08 M19 M30 M31 M32).
import { describe, it, expect, vi } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";
import { normalizeProgramIds, parseMarketsFilter } from "../src/lib/programIds.js";

const W = "6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM"; // v2.2 devnet wrapper
const W_CASE = "6Kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM"; // one char case-flipped: a DIFFERENT valid key
const K43 = "ogrkd17raWEoJtKxxdEtvnirmUY1qZiMwCyLgEpEP9Z";

describe("normalizeProgramIds", () => {
  it("K-M04: a 44-char base58 string that is not 32 bytes throws (regex-only check is not enough)", () => {
    expect(() => normalizeProgramIds(["z".repeat(44)])).toThrow(/ALL_PROGRAM_IDS/);
  });
  it("K-M04b: a real 43-char key is accepted", () => {
    expect(normalizeProgramIds([K43])).toEqual([K43]);
  });
  it("K-M06: dedupe is case-sensitive (base58): W and its case-flipped twin are two ids", () => {
    expect(normalizeProgramIds([W, W_CASE])).toEqual([W, W_CASE]);
  });
  it("K-M08: dedupe compares the trimmed id, not the raw entry", () => {
    expect(normalizeProgramIds([W, ` ${W}`, `${W}\r`])).toEqual([W]);
  });
});

describe("parseMarketsFilter", () => {
  it("K-M19: the separator is comma only; a space-separated pair is ONE bad entry", () => {
    const bad = vi.fn();
    expect(parseMarketsFilter(`${W} ${K43}`, bad)).toEqual([]);
    expect(bad).toHaveBeenCalledTimes(1);
    expect(bad.mock.calls[0][0]).toBe(`${W} ${K43}`);
  });
});

describe("call sites read the normalised list (K-M30/M31/M32)", () => {
  it("no src file outside lib/programIds.ts reads allProgramIds (raw list) in code", () => {
    const hits: string[] = [];
    const walk = (d: string) => {
      for (const f of readdirSync(d)) {
        const p = join(d, f);
        if (statSync(p).isDirectory()) walk(p);
        else if (p.endsWith(".ts") && !p.endsWith(join("lib", "programIds.ts"))) {
          readFileSync(p, "utf8").split("\n").forEach((l, i) => {
            if (/\ballProgramIds\b/.test(l) && !/^\s*(\/\/|\*)/.test(l)) hits.push(`${p}:${i + 1}`);
          });
        }
      }
    };
    walk(join(__dirname, "..", "src"));
    expect(hits).toEqual([]);
  });
});
