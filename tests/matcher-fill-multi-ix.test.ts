/**
 * Solana keeps only the LAST return data set in a transaction. cpiEvidence gets that tx-level
 * return data for EVERY wrapper instruction, so it must not hand it to a batch instruction
 * unless it can be shown to belong to that instruction's matcher call.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { cpiEvidenceFromParsed } from "../src/parsers/percolatorTxParser.js";
import { cpiEvidence, resolveCpiLeg, decodeMatcherReturn, resetTradecpiCounters, type ContextRead } from "../src/parsers/matcherFill.js";

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
function ret(r: { execSize: bigint; reqId: bigint; lp: bigint; asset: bigint; price: bigint }): Uint8Array {
  const b = new Uint8Array(64); const v = new DataView(b.buffer);
  v.setUint32(0, 1, true); v.setUint32(4, 1, true);
  v.setBigUint64(8, r.price, true); put128(v, 16, r.execSize);
  v.setBigUint64(32, r.reqId, true); v.setBigUint64(40, r.lp, true);
  v.setBigUint64(48, r.price, true); v.setBigUint64(56, r.asset, true);
  return b;
}
const concat = (...p: Uint8Array[]) => { const o = new Uint8Array(p.reduce((n, x) => n + x.length, 0)); let i = 0; for (const x of p) { o.set(x, i); i += x.length; } return o; };

const MATCHER = "MatcherProgram1111111111111111111111111111111";
const CTX = "MatcherContext1111111111111111111111111111111";
const accounts = ["a", "b", "c", "d", MATCHER, CTX];
const leg: Leg = { asset: 2, price: 100n, size: 1000n };
const noRead = async (): Promise<ContextRead> => ({ kind: "ok", ret: null });
const inner = (b: Uint8Array) => [{ programId: MATCHER, data: b58(b) }];
async function resolve(ev: ReturnType<typeof cpiEvidence>) {
  return resolveCpiLeg({ evidence: ev, assetIndex: 2, side: "long", wireSizeAbs: 1000n, legPos: 0, readContext: noRead });
}
void decodeMatcherReturn;

describe("batch return data shared by several matcher-calling instructions", () => {
  beforeEach(() => resetTradecpiCounters());

  const ixA = { accounts };
  const groups = (a: Uint8Array, b: Uint8Array) => [{ index: 0, instructions: inner(a) }, { index: 1, instructions: inner(b) }];
  it("two batch ixs, same leg count, different req_id: ix A must not be booked from ix B's return data", async () => {
    // tx return data = the LAST set = instruction B's return (reqId 8, executed 300)
    const returnData = { programId: MATCHER, data: ret({ execSize: 300n, reqId: 8n, lp: 9n, asset: 2n, price: 100n }) };
    const a = cpiEvidenceFromParsed(ixA, 0, groups(batchCall(7n, 9n, [leg]), batchCall(8n, 9n, [leg])), returnData, true);
    expect(await resolve(a)).toMatchObject({ kind: "fill", exact: false, sizeValue: 1000n });
  });

  it("two batch ixs with identical call keys (req_id, lp, asset, price): ix A gets ix B's exec_size", async () => {
    const returnData = { programId: MATCHER, data: ret({ execSize: 300n, reqId: 7n, lp: 9n, asset: 2n, price: 100n }) };
    // A's own return was 1000; B's (last) is 300. A must NOT be reported exact 300.
    const a = cpiEvidenceFromParsed(ixA, 0, groups(batchCall(7n, 9n, [leg]), batchCall(7n, 9n, [leg])), returnData, true);
    expect(await resolve(a)).toMatchObject({ kind: "fill", exact: false, sizeValue: 1000n });
  });

  it("different leg counts: length check rejects the foreign return data", async () => {
    const returnData = { programId: MATCHER, data: concat(ret({ execSize: 5n, reqId: 7n, lp: 9n, asset: 2n, price: 100n }), ret({ execSize: 5n, reqId: 7n, lp: 9n, asset: 3n, price: 100n })) };
    const a = cpiEvidenceFromParsed(ixA, 0, groups(batchCall(7n, 9n, [leg]), batchCall(7n, 9n, [leg, leg])), returnData, true);
    expect(await resolve(a)).toMatchObject({ exact: false });
  });

  it("a non-call matcher ix before the real call", () => {
    const junk = { programId: MATCHER, data: b58(new Uint8Array([9, 9, 9])) };
    const real = inner(batchCall(7n, 9n, [leg]))[0];
    expect(cpiEvidence(accounts, [junk, real], null, true).kind).toBe("unknown");
  });

  it("single matcher-calling ix: exact return data is still trusted", async () => {
    const returnData = { programId: MATCHER, data: ret({ execSize: 600n, reqId: 7n, lp: 9n, asset: 2n, price: 100n }) };
    const a = cpiEvidenceFromParsed(ixA, 0, [{ index: 0, instructions: inner(batchCall(7n, 9n, [leg])) }], returnData, true);
    expect(await resolve(a)).toMatchObject({ exact: true, sizeValue: 600n });
  });
  void singleCall;
});
