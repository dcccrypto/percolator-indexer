import { describe, it, expect } from "vitest";
import { basisKnown, compareEvents, foldPosition, type FoldEvent } from "../src/lpVault/position.js";

const ev = (o: Partial<FoldEvent> & Pick<FoldEvent, "kind">): FoldEvent => ({
  slot: 1,
  signature: "sig",
  ixIndex: 0,
  collateralAtoms: 0n,
  lpAmount: null,
  ...o,
});

describe("compareEvents tie-breaks", () => {
  it("orders by slot first", () => {
    expect(compareEvents(ev({ kind: "deposit", slot: 1 }), ev({ kind: "deposit", slot: 3 }))).toBe(-2);
  });
  it("same slot: orders by signature, both directions", () => {
    const a = ev({ kind: "deposit", signature: "a" });
    const b = ev({ kind: "deposit", signature: "b" });
    expect(compareEvents(a, b)).toBe(-1);
    expect(compareEvents(b, a)).toBe(1);
  });
  it("same slot and signature: orders by ixIndex", () => {
    expect(compareEvents(ev({ kind: "deposit", ixIndex: 5 }), ev({ kind: "deposit", ixIndex: 2 }))).toBe(3);
    expect(compareEvents(ev({ kind: "deposit" }), ev({ kind: "deposit" }))).toBe(0);
  });
  it("fold uses the full tie-break: same slot, signature order decides deposit before redeem", () => {
    const f = foldPosition([
      ev({ kind: "execute_redeem", signature: "b", collateralAtoms: 150n, lpAmount: 100n }),
      ev({ kind: "deposit", signature: "a", collateralAtoms: 100n, lpAmount: 100n }),
    ]);
    expect(f.eventsConsistent).toBe(true);
    expect(f.realizedPnlAtoms).toBe(50n);
    expect(f.lpShares).toBe(0n);
  });
  it("fold uses ixIndex within one signature", () => {
    const f = foldPosition([
      ev({ kind: "execute_redeem", ixIndex: 2, collateralAtoms: 90n, lpAmount: 100n }),
      ev({ kind: "deposit", ixIndex: 1, collateralAtoms: 100n, lpAmount: 100n }),
    ]);
    expect(f.realizedPnlAtoms).toBe(-10n);
    expect(f.costBasisAtoms).toBe(0n);
  });
});

describe("foldPosition unreadable share amounts", () => {
  it("empty input folds to a zero, consistent position", () => {
    const f = foldPosition([]);
    expect(f).toMatchObject({ lpShares: 0n, eventCount: 0, lastSlot: 0, eventsConsistent: true, inconsistency: null });
  });
  it("request with no share amount flags and leaves pending alone", () => {
    const f = foldPosition([ev({ kind: "request_redeem", signature: "r1" })]);
    expect(f.pendingRedeemShares).toBe(0n);
    expect(f.eventsConsistent).toBe(false);
    expect(f.inconsistency).toBe("request r1 has no share amount");
  });
  it("cancel with no share amount flags and leaves pending alone", () => {
    const f = foldPosition([
      ev({ kind: "deposit", slot: 1, collateralAtoms: 10n, lpAmount: 10n }),
      ev({ kind: "request_redeem", slot: 2, lpAmount: 4n }),
      ev({ kind: "cancel_redeem", slot: 3, signature: "c1" }),
    ]);
    expect(f.pendingRedeemShares).toBe(4n);
    expect(f.inconsistency).toBe("cancel c1 has no share amount");
  });
  it("cancel larger than pending floors at zero", () => {
    const f = foldPosition([
      ev({ kind: "deposit", slot: 1, collateralAtoms: 10n, lpAmount: 10n }),
      ev({ kind: "request_redeem", slot: 2, lpAmount: 4n }),
      ev({ kind: "cancel_redeem", slot: 3, lpAmount: 9n }),
    ]);
    expect(f.pendingRedeemShares).toBe(0n);
    expect(f.eventsConsistent).toBe(true);
  });
  it("partial cancel leaves the remainder pending", () => {
    const f = foldPosition([
      ev({ kind: "request_redeem", slot: 1, lpAmount: 10n }),
      ev({ kind: "cancel_redeem", slot: 2, lpAmount: 4n }),
    ]);
    expect(f.pendingRedeemShares).toBe(6n);
  });
  it("cancel equal to pending clears it exactly", () => {
    const f = foldPosition([
      ev({ kind: "request_redeem", slot: 1, lpAmount: 4n }),
      ev({ kind: "cancel_redeem", slot: 2, lpAmount: 4n }),
    ]);
    expect(f.pendingRedeemShares).toBe(0n);
  });
  it("execute with no Burn amount books the whole payout as realized, keeps basis, flags", () => {
    const f = foldPosition([
      ev({ kind: "deposit", slot: 1, collateralAtoms: 100n, lpAmount: 100n }),
      ev({ kind: "execute_redeem", slot: 2, signature: "x1", collateralAtoms: 30n }),
    ]);
    expect(f.realizedPnlAtoms).toBe(30n);
    expect(f.totalRedeemedAtoms).toBe(30n);
    expect(f.costBasisAtoms).toBe(100n);
    expect(f.lpShares).toBe(100n);
    expect(f.inconsistency).toBe("redemption x1 has no readable Burn amount");
  });
  it("only the FIRST inconsistency message is kept", () => {
    const f = foldPosition([
      ev({ kind: "request_redeem", slot: 1, signature: "first" }),
      ev({ kind: "cancel_redeem", slot: 2, signature: "second" }),
    ]);
    expect(f.inconsistency).toBe("request first has no share amount");
  });
  it("deposit with no MintTo amount still counts collateral toward basis and deposited", () => {
    const f = foldPosition([ev({ kind: "deposit", signature: "d1", collateralAtoms: 70n })]);
    expect(f.costBasisAtoms).toBe(70n);
    expect(f.totalDepositedAtoms).toBe(70n);
    expect(f.lpShares).toBe(0n);
    expect(f.eventsConsistent).toBe(false);
  });
});

describe("foldPosition over-burn and pending clamp", () => {
  it("over-burn realizes against known basis and zeroes shares and pending", () => {
    const f = foldPosition([
      ev({ kind: "deposit", slot: 1, collateralAtoms: 100n, lpAmount: 100n }),
      ev({ kind: "request_redeem", slot: 2, lpAmount: 100n }),
      ev({ kind: "execute_redeem", slot: 3, signature: "o", collateralAtoms: 500n, lpAmount: 400n }),
    ]);
    expect(f.realizedPnlAtoms).toBe(400n);
    expect(f.costBasisAtoms).toBe(0n);
    expect(f.lpShares).toBe(0n);
    expect(f.pendingRedeemShares).toBe(0n);
    expect(f.eventsConsistent).toBe(false);
    expect(f.inconsistency).toContain("burned 400 > indexed 100");
  });
  it("partial burn keeps pending when pending exceeds burned, clamps when not", () => {
    const base = [
      ev({ kind: "deposit", slot: 1, collateralAtoms: 100n, lpAmount: 100n }),
      ev({ kind: "request_redeem", slot: 2, lpAmount: 60n }),
    ];
    const keep = foldPosition([...base, ev({ kind: "execute_redeem", slot: 3, collateralAtoms: 20n, lpAmount: 20n })]);
    expect(keep.pendingRedeemShares).toBe(40n);
    const clamp = foldPosition([...base, ev({ kind: "execute_redeem", slot: 3, collateralAtoms: 80n, lpAmount: 80n })]);
    expect(clamp.pendingRedeemShares).toBe(0n);
    expect(clamp.lpShares).toBe(20n);
    expect(clamp.costBasisAtoms).toBe(20n);
  });
  it("a second deposit into a non-empty position is not a fresh start", () => {
    const f = foldPosition([
      ev({ kind: "deposit", slot: 1, collateralAtoms: 10n, lpAmount: 10n }),
      ev({ kind: "deposit", slot: 5, collateralAtoms: 10n, lpAmount: 10n }),
    ]);
    expect(f.freshStartSlot).toBe(1);
    expect(f.lastSlot).toBe(5);
  });
});

describe("basisKnown", () => {
  const consistent = foldPosition([ev({ kind: "deposit", slot: 10, collateralAtoms: 1n, lpAmount: 1n })]);
  it("false when events are inconsistent, whatever the transfer slot", () => {
    const bad = foldPosition([ev({ kind: "request_redeem" })]);
    expect(basisKnown(bad, null)).toBe(false);
  });
  it("true with no transfer detected", () => expect(basisKnown(consistent, null)).toBe(true));
  it("transfer after fresh start keeps it unknown; strictly older transfer clears it", () => {
    expect(basisKnown(consistent, 10)).toBe(false);
    expect(basisKnown(consistent, 11)).toBe(false);
    expect(basisKnown(consistent, 9)).toBe(true);
  });
});
