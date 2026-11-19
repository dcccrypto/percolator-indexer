import { describe, it, expect } from "vitest";
import { basisKnown, foldPosition, type FoldEvent } from "../../src/lpVault/position.js";

function e(kind: FoldEvent["kind"], signature: string, slot: number, collateral: bigint, lp: bigint | null): FoldEvent {
  return { kind, slot, signature, ixIndex: 0, collateralAtoms: collateral, lpAmount: lp };
}

describe("same-slot events from different signatures (no tx index in source rows)", () => {
  it("(a) deposit + full redeem in one slot on an existing position is not reported as exact", () => {
    const f = foldPosition([
      e("deposit", "s0", 1, 60n, 100n), // existing position: 100 shares, basis 60
      e("deposit", "a-dep", 10, 50n, 40n),
      e("execute_redeem", "b-red", 10, 110n, 100n), // true order: deposit then redeem 100 of 140
    ]);
    // Order is ambiguous across signatures: the result must not claim an exact basis.
    expect(f.eventsConsistent).toBe(false);
    expect(basisKnown(f, null)).toBe(false);
  });

  it("(b) redeem sorting before deposit in the same slot never yields consistent phantom shares", () => {
    const f = foldPosition([
      e("execute_redeem", "a-red", 10, 110n, 100n),
      e("deposit", "b-dep", 10, 100n, 100n),
    ]);
    expect(basisKnown(f, null)).toBe(false);
  });

  it("a later fresh start (strictly later slot) still recovers", () => {
    const f = foldPosition([
      e("execute_redeem", "a-red", 10, 110n, 100n),
      e("deposit", "b-dep", 10, 100n, 100n),
      e("execute_redeem", "c-red", 11, 100n, 100n),
      e("deposit", "d-dep", 12, 50n, 50n),
    ]);
    expect(f.eventsConsistent).toBe(true);
    expect(f.lpShares).toBe(50n);
  });

  it("same-signature or order-independent same-slot events stay exact", () => {
    const f = foldPosition([
      e("deposit", "a", 10, 50n, 50n),
      e("deposit", "b", 10, 50n, 50n),
    ]);
    expect(f.eventsConsistent).toBe(true);
    const g = foldPosition([
      { ...e("deposit", "a", 10, 50n, 50n), ixIndex: 0 },
      { ...e("execute_redeem", "a", 10, 60n, 50n), ixIndex: 1 },
    ]);
    expect(g.eventsConsistent).toBe(true);
  });

  it("execute_redeem sorting before its request in one slot is flagged (pending would be wrong)", () => {
    const f = foldPosition([
      e("deposit", "s0", 1, 100n, 100n),
      e("execute_redeem", "a-exe", 10, 100n, 100n),
      e("request_redeem", "b-req", 10, 0n, 100n),
    ]);
    expect(f.eventsConsistent).toBe(false);
  });

  it("over-burn is not re-marked consistent by a same-slot deposit (same signature)", () => {
    const f = foldPosition([
      { ...e("execute_redeem", "a", 10, 100n, 100n), ixIndex: 0 },
      { ...e("deposit", "a", 10, 50n, 50n), ixIndex: 1 },
    ]);
    expect(f.eventsConsistent).toBe(false);
  });
});
