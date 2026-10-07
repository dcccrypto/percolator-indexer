/**
 * #213 / #221: executed size + booked price of TradeCpi fills, against REAL devnet transactions
 * (wrapper ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB). See src/parsers/matcherFill.ts for the
 * source of each field. Fixtures: tests/fixtures/tradecpi/*.json.
 */
import { describe, it, expect, vi } from "vitest";
import { readFileSync } from "node:fs";
import { parsePercolatorFills } from "../src/parsers/percolatorTxParser.js";
import {
  decodeMatcherCall, decodeMatcherReturn, cpiEvidence, resolveCpiLeg, unverifiedSizePolicy,
  type MatcherReturn, type CpiEvidence,
} from "../src/parsers/matcherFill.js";

const PROGRAM = "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB";
const fx = (n: string) => JSON.parse(readFileSync(new URL(`./fixtures/tradecpi/${n}.json`, import.meta.url), "utf8"));
const ctxOf = (f: { ctxReturnHex: string }): MatcherReturn => decodeMatcherReturn(Uint8Array.from(Buffer.from(f.ctxReturnHex, "hex")))!;

function fillsOf(f: { sig: string; tx: any }) {
  return parsePercolatorFills(f.tx, f.sig, [PROGRAM]);
}
async function resolve(f: { sig: string; tx: any }, ctx: MatcherReturn | null, policy: "skip" | "request" = "skip") {
  const [fill] = fillsOf(f);
  const read = vi.fn(async () => ctx);
  const r = await resolveCpiLeg({ evidence: fill.cpi!, assetIndex: fill.assetIndex, side: fill.side, wireSizeAbs: fill.sizeAbs, legPos: fill.legPos ?? 0, readContext: read, policy });
  return { fill, r, read };
}

describe("TradeCpi executed size and booked price (real devnet transactions)", () => {
  it("full fill: size = request, price = the oracle price handed to the matcher", async () => {
    const f = fx("full");
    const { fill, r } = await resolve(f, ctxOf(f));
    expect(fill.sizeAbs).toBe(121618497109n);
    expect(r).toEqual({ kind: "fill", sizeValue: 121618497109n, priceE6: 1558n, exact: true });
  });

  it("matcher PARTIAL fill (LINK, requested 29.041225, executed 0.000483): the row carries the executed size", async () => {
    const f = fx("partial");
    const { fill, r } = await resolve(f, ctxOf(f));
    expect(fill.sizeAbs).toBe(29_041_225n); // what the indexer used to write
    expect(r).toEqual({ kind: "fill", sizeValue: 483n, priceE6: 13_861_751n, exact: true });
  });

  it("headroom clip: executed size is the clipped size, not the request", async () => {
    const f = fx("clipFull");
    const { fill, r } = await resolve(f, ctxOf(f));
    expect(fill.sizeAbs).toBe(378_397_480_755n);
    expect(r).toMatchObject({ kind: "fill", sizeValue: 189_226_154_805n, exact: true });
  });

  it("ZERO fill (wrapper clipped to zero headroom, no matcher call): no row, no context read", async () => {
    const f = fx("zeroNoMatcherCall");
    const { fill, r, read } = await resolve(f, null);
    expect(fill.cpi).toEqual({ kind: "no-matcher-call" });
    expect(fill.sizeAbs).toBe(98_424_192_212n); // the old row: 98,424 units of phantom volume
    expect(r).toMatchObject({ kind: "skip", reason: "zero-fill" });
    expect(read).not.toHaveBeenCalled();
  });

  it("#213 transaction (matcher filled 72 of 822500; its context was overwritten since): nothing is written by default", async () => {
    const f = fx("issue213Partial");
    const later = { ...ctxOf(fx("full")) }; // another trade's answer: req_id differs
    const { r } = await resolve(f, later);
    expect(r).toMatchObject({ kind: "skip", reason: "size-unverified" });
  });

  it("... and with TRADECPI_UNVERIFIED_SIZE=request the booked price and the matcher-requested size are used, flagged inexact", async () => {
    const f = fx("issue213Partial");
    const { r } = await resolve(f, null, "request");
    expect(r).toEqual({ kind: "fill", sizeValue: 822_500n, priceE6: 121_580_511n, exact: false });
    expect(unverifiedSizePolicy({ TRADECPI_UNVERIFIED_SIZE: "request" })).toBe("request");
    expect(unverifiedSizePolicy({})).toBe("skip");
  });

  it("an unreadable context (RPC failure) is unverified, never a guess", async () => {
    const f = fx("partial");
    const { r } = await resolve(f, null);
    expect(r).toMatchObject({ kind: "skip", reason: "size-unverified" });
  });

  describe("negative controls: a context answer is trusted only if it is THIS call's answer", () => {
    const base = () => ctxOf(fx("partial"));
    it.each([
      ["req_id of another trade", (c: MatcherReturn) => ({ ...c, reqId: c.reqId + 1n })],
      ["other LP", (c: MatcherReturn) => ({ ...c, lpAccountId: c.lpAccountId + 1n })],
      ["other asset", (c: MatcherReturn) => ({ ...c, assetIndex: 1n })],
      ["other oracle price", (c: MatcherReturn) => ({ ...c, oraclePriceE6: c.oraclePriceE6 + 1n })],
      ["exec_size larger than requested", (c: MatcherReturn) => ({ ...c, execSize: -29_041_226n })],
      ["exec_size on the other side", (c: MatcherReturn) => ({ ...c, execSize: 483n })],
      ["rejected flag", (c: MatcherReturn) => ({ ...c, flags: 5 })],
      ["invalid flag unset", (c: MatcherReturn) => ({ ...c, flags: 2 })],
    ])("%s -> skip", async (_n, mutate) => {
      const { r } = await resolve(fx("partial"), mutate(base()));
      expect(r).toMatchObject({ kind: "skip", reason: "size-unverified" });
    });
    it("matcher answered exec_size 0 for this req_id -> zero fill, no row", async () => {
      const { r } = await resolve(fx("partial"), { ...base(), execSize: 0n });
      expect(r).toMatchObject({ kind: "skip", reason: "zero-fill" });
    });
  });

  it("wire/leg mismatches are not written: matcher size larger than the instruction's, other side, other asset", async () => {
    const f = fx("partial");
    const [fill] = fillsOf(f);
    const ev = fill.cpi as Extract<CpiEvidence, { kind: "call" }>;
    const mk = (leg: Partial<(typeof ev.call.legs)[0]>): CpiEvidence => ({ ...ev, call: { ...ev.call, legs: [{ ...ev.call.legs[0], ...leg }] } });
    for (const evidence of [mk({ reqSize: -30_000_000n }), mk({ reqSize: 29_041_225n }), mk({ assetIndex: 3 }), mk({ oraclePriceE6: 0n })]) {
      const r = await resolveCpiLeg({ evidence, assetIndex: fill.assetIndex, side: fill.side, wireSizeAbs: fill.sizeAbs, legPos: 0, readContext: async () => ctxOf(f) });
      expect(r.kind).toBe("skip");
    }
  });

  it("inner instructions missing from the source: nothing is provable", async () => {
    const f = fx("partial");
    const tx = { ...f.tx, meta: { ...f.tx.meta, innerInstructions: undefined } };
    const [fill] = parsePercolatorFills(tx, f.sig, [PROGRAM]);
    expect(fill.cpi).toEqual({ kind: "unknown" });
    const r = await resolveCpiLeg({ evidence: fill.cpi!, assetIndex: 0, side: fill.side, wireSizeAbs: fill.sizeAbs, legPos: 0, readContext: async () => ctxOf(f) });
    expect(r).toMatchObject({ kind: "skip", reason: "size-unverified" });
  });

  it("leg numbering is unchanged: a zero fill still occupies its slot among the tx's fills", () => {
    const f = fx("zeroNoMatcherCall");
    expect(fillsOf(f)).toHaveLength(1);
    const ix = f.tx.transaction.message.instructions.find((i: any) => i.programId === PROGRAM && i.data.length > 100);
    const twice = { ...f.tx, transaction: { message: { instructions: [ix, ix] } } };
    expect(parsePercolatorFills(twice, f.sig, [PROGRAM])).toHaveLength(2);
  });

  it("the matcher call decodes to what the wrapper sent (real bytes)", () => {
    const f = fx("issue213Partial");
    const [fill] = fillsOf(f);
    const call = (fill.cpi as any).call;
    expect(call).toMatchObject({ batch: false, reqId: 8n });
    expect(call.legs[0]).toEqual({ assetIndex: 0, oraclePriceE6: 121_580_511n, reqSize: 822_500n });
  });
});

describe("BatchTradeCpi (SYNTHETIC: no BatchTradeCpi transaction exists on the live devnet wrapper)", () => {
  // matcher batch call: tag 3, n, req_id, lp_account_id, then n x (asset u16, oracle u64, req_size i128)
  function batchCall(legs: Array<[number, bigint, bigint]>): string {
    const b = Buffer.alloc(18 + 26 * legs.length);
    b[0] = 3; b[1] = legs.length; b.writeBigUInt64LE(77n, 2); b.writeBigUInt64LE(9n, 10);
    legs.forEach(([a, p, s], i) => {
      const o = 18 + i * 26;
      b.writeUInt16LE(a, o); b.writeBigUInt64LE(p, o + 2);
      b.writeBigUInt64LE(BigInt.asUintN(64, s), o + 10); b.writeBigInt64LE(s < 0n ? -1n : 0n, o + 18);
    });
    return encode58(b);
  }
  function ret(asset: number, price: bigint, size: bigint): Buffer {
    const b = Buffer.alloc(64);
    b.writeUInt32LE(3, 0); b.writeUInt32LE(1, 4); b.writeBigUInt64LE(price, 8);
    b.writeBigUInt64LE(BigInt.asUintN(64, size), 16); b.writeBigInt64LE(size < 0n ? -1n : 0n, 24);
    b.writeBigUInt64LE(77n, 32); b.writeBigUInt64LE(9n, 40); b.writeBigUInt64LE(price, 48); b.writeBigUInt64LE(BigInt(asset), 56);
    return b;
  }
  const MATCHER = "EDKKgRaVHna6FCxiY1kgMzegD9rpaN1nwJNSzAzeBUBX";
  const accounts = ["a", "m", "p", "l", MATCHER, "ctx", "d"];

  it("per-leg executed size and per-leg booked price come from the matcher's return data", async () => {
    const data = batchCall([[0, 100_000_000n, 5_000_000n], [1, 2_000_000n, -3_000_000n]]);
    const returns = Buffer.concat([ret(0, 100_000_000n, 4_000_000n), ret(1, 2_000_000n, -3_000_000n)]);
    const ev = cpiEvidence(accounts, [{ programId: MATCHER, data }], { programId: MATCHER, data: Uint8Array.from(returns) });
    const r0 = await resolveCpiLeg({ evidence: ev, assetIndex: 0, side: "long", wireSizeAbs: 5_000_000n, legPos: 0, readContext: async () => null });
    const r1 = await resolveCpiLeg({ evidence: ev, assetIndex: 1, side: "short", wireSizeAbs: 3_000_000n, legPos: 1, readContext: async () => null });
    expect(r0).toEqual({ kind: "fill", sizeValue: 4_000_000n, priceE6: 100_000_000n, exact: true });
    expect(r1).toEqual({ kind: "fill", sizeValue: 3_000_000n, priceE6: 2_000_000n, exact: true });
  });

  it("without return data the sizes are unverified (the batch never writes to the context), prices still known", async () => {
    const ev = cpiEvidence(accounts, [{ programId: MATCHER, data: batchCall([[0, 100_000_000n, 5_000_000n]]) }], null);
    const r = await resolveCpiLeg({ evidence: ev, assetIndex: 0, side: "long", wireSizeAbs: 5_000_000n, legPos: 0, readContext: async () => ret(0, 100_000_000n, 5_000_000n) as unknown as MatcherReturn });
    expect(r).toMatchObject({ kind: "skip", reason: "size-unverified" });
  });
});

function encode58(bytes: Uint8Array): string {
  const A = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let zeros = 0; while (zeros < bytes.length && bytes[zeros] === 0) zeros++;
  const d: number[] = [];
  for (let i = zeros; i < bytes.length; i++) { let c = bytes[i]; for (let j = 0; j < d.length; j++) { c += d[j] << 8; d[j] = c % 58; c = (c / 58) | 0; } while (c > 0) { d.push(c % 58); c = (c / 58) | 0; } }
  return "1".repeat(zeros) + d.reverse().map((x) => A[x]).join("");
}
