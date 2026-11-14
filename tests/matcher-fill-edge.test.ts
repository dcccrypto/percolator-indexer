/**
 * Edge cases of src/parsers/matcherFill.ts with synthetic bytes: short/garbage input, zero and
 * max sizes, partial fills, mismatching legs, ordering, the periodic summary log.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  decodeMatcherCall, decodeMatcherReturn, cpiEvidence, resolveCpiLeg, getTradecpiCounters,
  getTradecpiSizeUnverifiedCount, resetTradecpiCounters, type ContextRead, type MatcherReturn, type CpiEvidence,
} from "../src/parsers/matcherFill.js";

const ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
function b58(bytes: Uint8Array): string {
  let n = 0n;
  for (const b of bytes) n = (n << 8n) | BigInt(b);
  let s = "";
  while (n > 0n) { s = ALPHABET[Number(n % 58n)] + s; n /= 58n; }
  for (const b of bytes) { if (b === 0) s = "1" + s; else break; }
  return s;
}
function put128(v: DataView, off: number, x: bigint) {
  v.setBigUint64(off, BigInt.asUintN(64, x), true);
  v.setBigInt64(off + 8, BigInt.asIntN(64, x >> 64n), true);
}

interface Leg { asset: number; price: bigint; size: bigint }
function singleCall(reqId: bigint, lp: bigint, l: Leg): Uint8Array {
  const b = new Uint8Array(43); const v = new DataView(b.buffer);
  b[0] = 0; v.setBigUint64(1, reqId, true); v.setUint16(9, l.asset, true); v.setBigUint64(11, lp, true);
  v.setBigUint64(19, l.price, true); put128(v, 27, l.size);
  return b;
}
function batchCall(reqId: bigint, lp: bigint, legs: Leg[]): Uint8Array {
  const b = new Uint8Array(18 + legs.length * 26); const v = new DataView(b.buffer);
  b[0] = 3; b[1] = legs.length; v.setBigUint64(2, reqId, true); v.setBigUint64(10, lp, true);
  legs.forEach((l, i) => {
    const o = 18 + i * 26;
    v.setUint16(o, l.asset, true); v.setBigUint64(o + 2, l.price, true); put128(v, o + 10, l.size);
  });
  return b;
}
function ret(r: Partial<MatcherReturn> & { execSize: bigint }): Uint8Array {
  const b = new Uint8Array(64); const v = new DataView(b.buffer);
  v.setUint32(0, r.abiVersion ?? 1, true); v.setUint32(4, r.flags ?? 1, true);
  v.setBigUint64(8, r.execPriceE6 ?? 100n, true); put128(v, 16, r.execSize);
  v.setBigUint64(32, r.reqId ?? 7n, true); v.setBigUint64(40, r.lpAccountId ?? 9n, true);
  v.setBigUint64(48, r.oraclePriceE6 ?? 100n, true); v.setBigUint64(56, r.assetIndex ?? 2n, true);
  return b;
}

const MATCHER = "MatcherProgram1111111111111111111111111111111";
const CTX = "MatcherContext1111111111111111111111111111111";
const accounts = ["a", "b", "c", "d", MATCHER, CTX];
const baseLeg: Leg = { asset: 2, price: 100n, size: 1000n };
const okRead = (r: Uint8Array | null) => vi.fn(async (): Promise<ContextRead> => ({ kind: "ok", ret: r ? decodeMatcherReturn(r) : null }));

beforeEach(() => resetTradecpiCounters());

describe("decodeMatcherCall", () => {
  it("rejects empty, unknown tag, and short single/batch payloads", () => {
    expect(decodeMatcherCall(new Uint8Array(0))).toBeNull();
    expect(decodeMatcherCall(Uint8Array.of(9, 0, 0, 0))).toBeNull();
    expect(decodeMatcherCall(singleCall(1n, 1n, baseLeg).subarray(0, 42))).toBeNull();
    expect(decodeMatcherCall(batchCall(1n, 1n, [baseLeg]).subarray(0, 17))).toBeNull();
    // header says 2 legs, only one present
    const b = batchCall(1n, 1n, [baseLeg, baseLeg]).subarray(0, 18 + 26);
    expect(decodeMatcherCall(b)).toBeNull();
  });

  it("decodes max u64 ids/prices and i128 extremes with sign", () => {
    const MAX_I128 = (1n << 127n) - 1n, MIN_I128 = -(1n << 127n), U64 = (1n << 64n) - 1n;
    const c = decodeMatcherCall(singleCall(U64, U64, { asset: 65535, price: U64, size: MIN_I128 }))!;
    expect(c).toMatchObject({ batch: false, reqId: U64, lpAccountId: U64 });
    expect(c.legs[0]).toEqual({ assetIndex: 65535, oraclePriceE6: U64, reqSize: MIN_I128 });
    expect(decodeMatcherCall(singleCall(1n, 1n, { ...baseLeg, size: MAX_I128 }))!.legs[0].reqSize).toBe(MAX_I128);
    expect(decodeMatcherCall(singleCall(1n, 1n, { ...baseLeg, size: -1n }))!.legs[0].reqSize).toBe(-1n);
  });

  it("keeps batch leg order and accepts a zero-leg batch", () => {
    const legs = [{ asset: 3, price: 5n, size: -7n }, { asset: 1, price: 6n, size: 8n }, { asset: 3, price: 5n, size: -7n }];
    const c = decodeMatcherCall(batchCall(4n, 5n, legs))!;
    expect(c.batch).toBe(true);
    expect(c.legs.map((l) => [l.assetIndex, l.reqSize])).toEqual([[3, -7n], [1, 8n], [3, -7n]]);
    expect(decodeMatcherCall(batchCall(4n, 5n, []))!.legs).toEqual([]);
  });
});

describe("decodeMatcherReturn", () => {
  it("rejects <64 bytes, reads a larger buffer's first 64 bytes, handles byteOffset views", () => {
    expect(decodeMatcherReturn(new Uint8Array(63))).toBeNull();
    const padded = new Uint8Array(70); padded.set(ret({ execSize: -5n }), 3);
    expect(decodeMatcherReturn(padded.subarray(3))!.execSize).toBe(-5n);
  });
});

describe("cpiEvidence", () => {
  const call = (b: Uint8Array) => ({ programId: MATCHER, data: b58(b) });

  it("is unknown without inner ix or without a matcher program account (batch flag carried)", () => {
    expect(cpiEvidence(accounts, null, null, false)).toEqual({ kind: "unknown" });
    expect(cpiEvidence(accounts, undefined, null, true)).toEqual({ kind: "unknown", batch: true });
    expect(cpiEvidence(["a"], [], null, false)).toEqual({ kind: "unknown" });
  });

  it("no matcher CPI: zero fill for single, unknown for batch; other programs are skipped", () => {
    const other = [{ programId: "Other", data: "1" }];
    expect(cpiEvidence(accounts, other, null, false)).toEqual({ kind: "no-matcher-call" });
    expect(cpiEvidence(accounts, [], null, false)).toEqual({ kind: "no-matcher-call" });
    expect(cpiEvidence(accounts, other, null, true)).toEqual({ kind: "unknown", batch: true });
  });

  it("garbage data, or a call of the wrong kind, is unknown", () => {
    expect(cpiEvidence(accounts, [{ programId: MATCHER, data: "0OIl" }], null, false).kind).toBe("unknown");
    expect(cpiEvidence(accounts, [call(singleCall(1n, 1n, baseLeg))], null, true)).toEqual({ kind: "unknown", batch: true });
    expect(cpiEvidence(accounts, [call(batchCall(1n, 1n, [baseLeg]))], null, false)).toEqual({ kind: "unknown" });
  });

  it("uses the FIRST matcher CPI when there are duplicates", () => {
    const e = cpiEvidence(accounts, [call(singleCall(1n, 1n, baseLeg)), call(singleCall(2n, 1n, baseLeg))], null, false);
    expect(e).toMatchObject({ kind: "call", matcherContext: CTX });
    expect((e as any).call.reqId).toBe(1n);
  });

  it("batch return data is split per leg only when program and length match", () => {
    const legs = [baseLeg, { ...baseLeg, asset: 3 }];
    const ix = [call(batchCall(7n, 9n, legs))];
    const data = new Uint8Array(128); data.set(ret({ execSize: 10n, assetIndex: 2n }), 0); data.set(ret({ execSize: 20n, assetIndex: 3n }), 64);
    const good = cpiEvidence(accounts, ix, { programId: MATCHER, data }, true) as Extract<CpiEvidence, { kind: "call" }>;
    expect(good.batchReturns!.map((r) => r.execSize)).toEqual([10n, 20n]);
    expect((cpiEvidence(accounts, ix, { programId: "x", data }, true) as any).batchReturns).toBeNull();
    expect((cpiEvidence(accounts, ix, { programId: MATCHER, data: data.subarray(0, 64) }, true) as any).batchReturns).toBeNull();
    expect((cpiEvidence(accounts, ix, null, true) as any).batchReturns).toBeNull();
    // single call never takes batch return data
    expect((cpiEvidence(accounts, [call(singleCall(7n, 9n, baseLeg))], { programId: MATCHER, data }, false) as any).batchReturns).toBeNull();
  });
});

describe("resolveCpiLeg", () => {
  const evOf = (b: Uint8Array, ctx = true) => {
    const e = cpiEvidence(ctx ? accounts : accounts.slice(0, 5), [{ programId: MATCHER, data: b58(b) }], null, false);
    return e;
  };
  const run = (evidence: CpiEvidence, readContext: any, o: Record<string, unknown> = {}) =>
    resolveCpiLeg({ evidence, assetIndex: 2, side: "long", wireSizeAbs: 1000n, legPos: 0, readContext, policy: "request", ...o } as any);

  it("partial fill: books the matcher's exec size at the oracle price", async () => {
    const r = await run(evOf(singleCall(7n, 9n, baseLeg)), okRead(ret({ execSize: 1n })));
    expect(r).toEqual({ kind: "fill", sizeValue: 1n, priceE6: 100n, exact: true });
    expect(getTradecpiCounters().exact).toBe(1);
  });

  it("zero exec size is a zero fill; negative exec on a short is a fill of the absolute size", async () => {
    const z = await run(evOf(singleCall(7n, 9n, baseLeg)), okRead(ret({ execSize: 0n })));
    expect(z).toMatchObject({ kind: "skip", reason: "zero-fill" });
    const short = await run(evOf(singleCall(7n, 9n, { ...baseLeg, size: -1000n })), okRead(ret({ execSize: -400n })), { side: "short" });
    expect(short).toMatchObject({ kind: "fill", sizeValue: 400n, exact: true });
    expect(getTradecpiCounters()).toMatchObject({ zeroFill: 1, exact: 1 });
  });

  it("max i128 request filled exactly stays exact (no overflow)", async () => {
    const MAX = (1n << 127n) - 1n;
    const r = await run(evOf(singleCall(7n, 9n, { ...baseLeg, size: MAX })), okRead(ret({ execSize: MAX })), { wireSizeAbs: MAX });
    expect(r).toEqual({ kind: "fill", sizeValue: MAX, priceE6: 100n, exact: true });
  });

  it("no-matcher-call is a zero fill; unknown is legacy (batch bumps legacyBatch and warns)", async () => {
    const read = okRead(null);
    expect(await run({ kind: "no-matcher-call" }, read)).toMatchObject({ kind: "skip", reason: "zero-fill" });
    expect(await run({ kind: "unknown" }, read)).toMatchObject({ kind: "legacy" });
    expect(await run({ kind: "unknown", batch: true }, read, { signature: "sig" })).toMatchObject({ kind: "legacy" });
    expect(getTradecpiCounters()).toMatchObject({ zeroFill: 1, legacy: 2, legacyBatch: 1 });
    expect(read).not.toHaveBeenCalled();
  });

  it.each([
    ["missing leg", { legPos: 3 }, singleCall(7n, 9n, baseLeg)],
    ["asset mismatch", { assetIndex: 5 }, singleCall(7n, 9n, baseLeg)],
    ["zero oracle price", {}, singleCall(7n, 9n, { ...baseLeg, price: 0n })],
    ["zero request size", {}, singleCall(7n, 9n, { ...baseLeg, size: 0n })],
    ["side mismatch", { side: "short" }, singleCall(7n, 9n, baseLeg)],
    ["request larger than wire size", { wireSizeAbs: 999n }, singleCall(7n, 9n, baseLeg)],
  ])("legacy when the call does not line up: %s", async (_n, o, bytes) => {
    const read = okRead(ret({ execSize: 1n }));
    expect(await run(evOf(bytes), read, o)).toMatchObject({ kind: "legacy" });
    expect(read).not.toHaveBeenCalled();
    expect(getTradecpiCounters().legacy).toBe(1);
  });

  it.each([
    ["not valid flag", { flags: 0 }],
    ["rejected flag", { flags: 5 }],
    ["other req_id", { reqId: 8n }],
    ["other lp", { lpAccountId: 1n }],
    ["other asset", { assetIndex: 3n }],
    ["other oracle price", { oraclePriceE6: 101n }],
    ["exec bigger than request", { execSize: 1001n }],
    ["exec sign opposite to request", { execSize: -1n }],
  ])("stale/mismatched context (%s) falls back to the upper bound", async (_n, o) => {
    const r = await run(evOf(singleCall(7n, 9n, baseLeg)), okRead(ret({ execSize: 10n, ...o })));
    expect(r).toEqual({ kind: "fill", sizeValue: 1000n, priceE6: 100n, exact: false });
    expect(getTradecpiSizeUnverifiedCount()).toBe(1);
  });

  it("no context account or an empty context: unverified; strict policy skips with the reason", async () => {
    const read = okRead(null);
    expect(await run(evOf(singleCall(7n, 9n, baseLeg), false), read)).toMatchObject({ exact: false });
    expect(read).not.toHaveBeenCalled();
    const s = await run(evOf(singleCall(7n, 9n, baseLeg)), read, { policy: "skip" });
    expect(s).toMatchObject({ kind: "skip", reason: "size-unverified" });
    expect((s as any).detail).toContain("req_id 7");
    expect(getTradecpiCounters()).toMatchObject({ unverified: 1, skipped: 1, readError: 0 });
  });

  it("read errors: counted and falling back by default; returned under report", async () => {
    const bad = vi.fn(async (): Promise<ContextRead> => ({ kind: "error", detail: "rpc 429" }));
    const ev = evOf(singleCall(7n, 9n, baseLeg));
    expect(await run(ev, bad)).toMatchObject({ kind: "fill", exact: false });
    expect(await run(ev, bad, { policy: "skip" })).toMatchObject({ kind: "skip" });
    expect(await run(ev, bad, { onReadError: "report" })).toEqual({ kind: "read-error", detail: "rpc 429" });
    expect(getTradecpiCounters()).toMatchObject({ unverified: 1, skipped: 1, readError: 2 });
  });

  it("batch: uses the per-leg return data by position and never reads the context", async () => {
    const legs = [baseLeg, { asset: 3, price: 50n, size: -200n }];
    const data = new Uint8Array(128);
    data.set(ret({ execSize: 600n, assetIndex: 2n }), 0);
    data.set(ret({ execSize: 0n, assetIndex: 3n, oraclePriceE6: 50n }), 64);
    const ev = cpiEvidence(accounts, [{ programId: MATCHER, data: b58(batchCall(7n, 9n, legs)) }], { programId: MATCHER, data }, true);
    const read = okRead(null);
    expect(await run(ev, read, { legPos: 0 })).toEqual({ kind: "fill", sizeValue: 600n, priceE6: 100n, exact: true });
    expect(await run(ev, read, { legPos: 1, assetIndex: 3, side: "short", wireSizeAbs: 200n })).toMatchObject({ reason: "zero-fill" });
    expect(read).not.toHaveBeenCalled();
  });

  it("batch without return data: upper bound, no context read", async () => {
    const ev = cpiEvidence(accounts, [{ programId: MATCHER, data: b58(batchCall(7n, 9n, [baseLeg])) }], null, true);
    const read = okRead(ret({ execSize: 5n }));
    expect(await run(ev, read)).toMatchObject({ kind: "fill", exact: false, sizeValue: 1000n });
    expect(read).not.toHaveBeenCalled();
  });

  it("batch return chunk shorter than the leg position falls back instead of throwing", async () => {
    const ev = { kind: "call", call: decodeMatcherCall(batchCall(7n, 9n, [baseLeg]))!, matcherContext: CTX, batchReturns: [] } as CpiEvidence;
    expect(await run(ev, okRead(null))).toMatchObject({ exact: false });
  });

  it("the same evidence resolved twice gives the same answer (duplicate signature) and counts twice", async () => {
    const ev = evOf(singleCall(7n, 9n, baseLeg));
    const read = okRead(ret({ execSize: 300n }));
    const a = await run(ev, read), b = await run(ev, read);
    expect(a).toEqual(b);
    expect(getTradecpiCounters().exact).toBe(2);
  });
});

describe("unverified-size policy default", () => {
  it("uses process env when no env passed to resolveCpiLeg", async () => {
    vi.stubEnv("TRADECPI_UNVERIFIED_SIZE", "skip");
    try {
      const ev = cpiEvidence(accounts, [{ programId: MATCHER, data: b58(singleCall(7n, 9n, baseLeg)) }], null, false);
      const r = await resolveCpiLeg({ evidence: ev, assetIndex: 2, side: "long", wireSizeAbs: 1000n, legPos: 0, readContext: okRead(null) });
      expect(r).toMatchObject({ kind: "skip", reason: "size-unverified" });
    } finally { vi.unstubAllEnvs(); }
  });
});

describe("summary log", () => {
  afterEach(() => vi.useRealTimers());

  it("does not throw and keeps counting past the 60s window and the 500-fill threshold", async () => {
    vi.useFakeTimers();
    const ev: CpiEvidence = { kind: "no-matcher-call" };
    const go = () => resolveCpiLeg({ evidence: ev, assetIndex: 2, side: "long", wireSizeAbs: 1n, legPos: 0, readContext: okRead(null), policy: "request" });
    vi.advanceTimersByTime(61_000);
    await go();
    for (let i = 0; i < 500; i++) await go();
    expect(getTradecpiCounters().zeroFill).toBe(501);
  });
});
