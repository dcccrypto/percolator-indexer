/** Differential: the optimised v22FillEvents must be byte-identical to the frozen pre-optimisation copy (fixtures + fuzzed logs). */
import { describe, expect, it } from "vitest";
import * as nu from "../../src/parsers/v22FillEvents.js";
import * as ref from "../helpers/v22FillEventsRef.js";
import { DOC_TOKENS, MARKETS, MATCHER, OTHER, W, WRAPPERS, fixtureTxs, gate, txLogs } from "../helpers/v22FillLogs.js";

const j = (v: unknown): string => JSON.stringify(v, (_k, x) => (typeof x === "bigint" ? `${x}n` : x));
let seed = 0x9e3779b9;
const rnd = (n: number): number => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed % n; };
const pick = <T,>(a: readonly T[]): T => a[rnd(a.length)];

function mutateToken(t: string): string {
  const b = Buffer.from(t, "base64");
  const m = rnd(6);
  if (m === 0 && b.length) b[rnd(b.length)] = rnd(256);
  else if (m === 1) return b.subarray(0, rnd(b.length + 1)).toString("base64");
  else if (m === 2) return t.slice(0, rnd(t.length + 1)) + "!";
  else if (m === 3 && b.length > 109) b[109] = rnd(12);
  else if (m === 4 && b.length > 35) b[35] = rnd(8);
  return m === 5 ? "" : b.toString("base64");
}

function fuzzLines(): string[] {
  const ids = [W, MATCHER, OTHER, MARKETS[0], "x".repeat(40), "0" + W.slice(1), W.slice(0, 31)];
  const lines: string[] = [];
  const n = rnd(14);
  for (let i = 0; i < n; i++) {
    const id = pick(ids);
    lines.push(
      pick([
        `Program ${id} invoke [${rnd(4)}]`, `Program ${id} invoke [1]`, `Program ${id} invoke [2]`, `Program ${id} success`,
        `Program ${id} failed: custom`, `Program ${id} consumed 1 of 2 compute units`, `Program ${id} invoke [1] `, "Program log: success",
        `Program data: ${pick(DOC_TOKENS)}`, `Program data: ${mutateToken(pick(DOC_TOKENS))} ${pick(DOC_TOKENS)}`, "Program data: ", "Log truncated", "x",
      ]),
    );
  }
  return lines;
}

describe("differential vs the pre-optimisation copy", () => {
  it("fixtures: rows and stats identical", () => {
    nu.resetLogEventStats(); ref.resetLogEventStats();
    for (const t of fixtureTxs()) {
      const a = nu.decodeV22LogEvents({ err: null, logMessages: t.logs }, t.ixs, WRAPPERS, gate, { signature: "s", slot: 1, blockTimeSec: 5 });
      const b = ref.decodeV22LogEvents({ err: null, logMessages: t.logs }, t.ixs, WRAPPERS, gate, { signature: "s", slot: 1, blockTimeSec: 5 });
      expect(j(a)).toBe(j(b));
    }
    expect(j(nu.getLogEventStats())).toBe(j(ref.getLogEventStats()));
  });

  it("fuzz: 6000 random logs (attribution, token decode, full decode) give 0 mismatches", () => {
    nu.resetLogEventStats(); ref.resetLogEventStats();
    const ixs = fixtureTxs()[0].ixs;
    let mismatches = 0;
    for (let i = 0; i < 6000; i++) {
      const logs: unknown = rnd(40) === 0 ? pick([null, [], [1, "a"], "str"]) : rnd(3) === 0 ? fuzzLines() : txLogs(Array.from({ length: rnd(4) }, () => mutateToken(pick(DOC_TOKENS))), fuzzLines());
      const err = rnd(30) === 0 ? { e: 1 } : null;
      if (j(nu.attributeWrapperTokens(logs, WRAPPERS)) !== j(ref.attributeWrapperTokens(logs, WRAPPERS))) mismatches++;
      const sel = rnd(5) ? ixs : [];
      const a = nu.decodeV22LogEvents({ err, logMessages: logs }, sel, WRAPPERS, gate, { signature: "s", slot: i });
      const b = ref.decodeV22LogEvents({ err, logMessages: logs }, sel, WRAPPERS, gate, { signature: "s", slot: i });
      if (j(a) !== j(b)) mismatches++;
    }
    expect(mismatches).toBe(0);
    expect(j(nu.getLogEventStats())).toBe(j(ref.getLogEventStats()));
  });

  it("fuzz: decode of random bytes/tokens identical (incl. memo eviction past 4096 distinct keys)", () => {
    let mismatches = 0;
    for (let i = 0; i < 9000; i++) {
      const buf = new Uint8Array(rnd(3) === 0 ? 62 : rnd(2) ? 134 : 100 + 75 * rnd(3));
      for (let k = 0; k < buf.length; k++) buf[k] = k < 2 ? (k === 0 ? 1 + rnd(3) : 1) : rnd(256);
      if (buf.length === 134) buf[109] = 1 + rnd(3);
      if (buf.length === 62) buf[35] = 1 + rnd(5);
      if (buf.length >= 100 && buf.length !== 134 && buf.length !== 62) buf[99] = (buf.length - 100) / 75;
      if (j(nu.decodeFillEventBytes(buf)) !== j(ref.decodeFillEventBytes(buf))) mismatches++;
      const tok = Buffer.from(buf).toString("base64");
      if (j(nu.decodeFillEventToken(tok)) !== j(ref.decodeFillEventToken(tok))) mismatches++;
    }
    expect(mismatches).toBe(0);
  });
});
