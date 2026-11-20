import { describe, it, expect } from "vitest";
import { feeFromAtoms, logFillFor } from "../../src/parsers/logFill.js";
import { resolveCpiLeg, type CpiEvidence } from "../../src/parsers/matcherFill.js";
import type { V22EventRow } from "../../src/parsers/v22Events.js";

const row = (o: Partial<V22EventRow> & { detail?: Record<string, unknown> } = {}): V22EventRow => ({
  signature: "S", ix_index: 4, inner_index: 1_000_000, kind: "fill_event", slab_address: "M", asset_index: 0, actor: "T", subject: "L", amount: "5", slot: 1, block_time: null,
  ...o,
  detail: { source: "log", rec_index: 0, zero: false, executed_q: "5", price_e6: "2000000", fee_atoms_total: "7", ...(o.detail ?? {}) } as V22EventRow["detail"],
});
const key = { ixIndex: 4, slab: "M", assetIndex: 0, legPos: 0 };

describe("logFillFor", () => {
  it("returns the leg's record", () => {
    expect(logFillFor([row()], key)).toEqual({ zero: false, executedQ: 5n, priceE6: 2000000n, feeAtoms: 7n });
  });
  it("no rows / other instruction, market, asset or kind / non-log source: null", () => {
    expect(logFillFor(undefined, key)).toBeNull();
    expect(logFillFor([], key)).toBeNull();
    expect(logFillFor([row({ ix_index: 3 })], key)).toBeNull();
    expect(logFillFor([row({ slab_address: "X" })], key)).toBeNull();
    expect(logFillFor([row({ asset_index: 1 })], key)).toBeNull();
    expect(logFillFor([row({ kind: "reduce_event" })], key)).toBeNull();
    expect(logFillFor([row({ detail: { source: "ix" } })], key)).toBeNull();
  });
  it("several records for the key: picked by rec_index, else ambiguous -> null", () => {
    const two = [row({ detail: { rec_index: 0, executed_q: "1" } }), row({ detail: { rec_index: 1, executed_q: "2" } })];
    expect(logFillFor(two, { ...key, legPos: 1 })?.executedQ).toBe(2n);
    expect(logFillFor([row(), row()], key)).toBeNull();
  });
  it("malformed numbers or flag: null", () => {
    expect(logFillFor([row({ detail: { executed_q: "x" } })], key)).toBeNull();
    expect(logFillFor([row({ detail: { price_e6: 5 } })], key)).toBeNull();
    expect(logFillFor([row({ detail: { fee_atoms_total: null } })], key)).toBeNull();
    expect(logFillFor([row({ detail: { zero: "no" } })], key)).toBeNull();
  });
  it("feeFromAtoms: atoms / 1e6", () => {
    expect(feeFromAtoms(140130n)).toBe(0.14013);
    expect(feeFromAtoms(0n)).toBe(0);
  });
});

describe("resolveCpiLeg with a log fill (context cannot prove the size)", () => {
  const evidence: CpiEvidence = { kind: "call", call: { batch: false, reqId: 1n, lpAccountId: 0n, legs: [{ assetIndex: 0, oraclePriceE6: 2000000n, reqSize: 100n }] }, matcherContext: "C", batchReturns: null };
  const base = { evidence, assetIndex: 0, side: "long" as const, wireSizeAbs: 100n, legPos: 0, readContext: async () => ({ kind: "ok" as const, ret: null }), policy: "request" as const };
  const lf = (o: Partial<{ zero: boolean; executedQ: bigint; priceE6: bigint; feeAtoms: bigint }>) => ({ zero: false, executedQ: 60n, priceE6: 2000000n, feeAtoms: 3n, ...o });

  it("ZERO record: zero-fill skip, not the request", async () => {
    expect(await resolveCpiLeg({ ...base, logFill: lf({ zero: true, executedQ: 0n }) })).toMatchObject({ kind: "skip", reason: "zero-fill" });
  });
  it("executed_q: exact fill at the logged price", async () => {
    expect(await resolveCpiLeg({ ...base, logFill: lf({}) })).toEqual({ kind: "fill", sizeValue: 60n, priceE6: 2000000n, exact: true });
  });
  it("a read error is not held when the log proves the size", async () => {
    const r = await resolveCpiLeg({ ...base, onReadError: "report", readContext: async () => ({ kind: "error" as const, detail: "429" }), logFill: lf({}) });
    expect(r).toMatchObject({ kind: "fill", sizeValue: 60n, exact: true });
  });
  it.each([
    ["ZERO flag with a size", lf({ zero: true })],
    ["no ZERO flag with executed 0", lf({ executedQ: 0n })],
    ["opposite direction", lf({ executedQ: -60n })],
    ["larger than the request", lf({ executedQ: 101n })],
    ["price 0", lf({ priceE6: 0n })],
  ])("inconsistent record (%s) is ignored: request upper bound, as before", async (_n, logFill) => {
    expect(await resolveCpiLeg({ ...base, logFill })).toEqual({ kind: "fill", sizeValue: 100n, priceE6: 2000000n, exact: false });
  });
});
