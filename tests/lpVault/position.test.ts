import { describe, it, expect } from "vitest";
import {
  basisKnown,
  compareEvents,
  foldPosition,
  type FoldEvent,
  type FoldedPosition,
} from "../../src/lpVault/position.js";

let n = 0;
function ev(kind: FoldEvent["kind"], collateral: bigint, lp: bigint | null, o: Partial<FoldEvent> = {}): FoldEvent {
  n += 1;
  return { kind, slot: n * 10, signature: `sig${n}`, ixIndex: 0, collateralAtoms: collateral, lpAmount: lp, ...o };
}

describe("compareEvents", () => {
  const base: FoldEvent = { kind: "deposit", slot: 5, signature: "b", ixIndex: 1, collateralAtoms: 0n, lpAmount: 0n };
  it("orders by slot first", () => {
    expect(compareEvents({ ...base, slot: 4 }, base)).toBeLessThan(0);
    expect(compareEvents({ ...base, slot: 6 }, base)).toBeGreaterThan(0);
  });
  it("same slot: orders by signature", () => {
    expect(compareEvents({ ...base, signature: "a" }, base)).toBe(-1);
    expect(compareEvents({ ...base, signature: "c" }, base)).toBe(1);
  });
  it("same slot and signature: orders by ixIndex", () => {
    expect(compareEvents({ ...base, ixIndex: 0 }, base)).toBe(-1);
    expect(compareEvents({ ...base, ixIndex: 3 }, base)).toBe(2);
    expect(compareEvents(base, { ...base })).toBe(0);
  });
  it("fold sorts same-slot events by signature then ixIndex regardless of input order", () => {
    const dep = ev("deposit", 100n, 100n, { slot: 1, signature: "a", ixIndex: 0 });
    const red = ev("execute_redeem", 100n, 100n, { slot: 1, signature: "a", ixIndex: 1 });
    const f = foldPosition([red, dep]);
    expect(f.lpShares).toBe(0n);
    expect(f.costBasisAtoms).toBe(0n);
    expect(f.eventsConsistent).toBe(true);
    expect(f.lastSlot).toBe(1);
    const dep2 = ev("deposit", 50n, 50n, { slot: 1, signature: "b", ixIndex: 0 });
    // a(dep, redeem) then b(dep): ends holding 50
    expect(foldPosition([dep2, red, dep]).lpShares).toBe(50n);
  });
});

describe("foldPosition: redemption edge cases", () => {
  it("empty list yields zeroed, consistent position", () => {
    const f = foldPosition([]);
    expect(f).toMatchObject({ lpShares: 0n, costBasisAtoms: 0n, eventCount: 0, lastSlot: 0, eventsConsistent: true, inconsistency: null });
  });

  it("redeeming more than held closes out, flags inconsistent, realizes against known basis", () => {
    // hold 100 shares at basis 1000; burn 150 for 2000 out
    const f = foldPosition([ev("deposit", 1000n, 100n), ev("request_redeem", 0n, 150n), ev("execute_redeem", 2000n, 150n)]);
    expect(f.lpShares).toBe(0n);
    expect(f.pendingRedeemShares).toBe(0n);
    expect(f.costBasisAtoms).toBe(0n);
    expect(f.realizedPnlAtoms).toBe(2000n - 1000n);
    expect(f.totalRedeemedAtoms).toBe(2000n);
    expect(f.eventsConsistent).toBe(false);
    expect(f.inconsistency).toContain("burned 150 > indexed 100");
    expect(basisKnown(f, null)).toBe(false);
  });

  it("redeeming from an empty position realizes the whole payout", () => {
    const f = foldPosition([ev("execute_redeem", 500n, 10n)]);
    expect(f.realizedPnlAtoms).toBe(500n);
    expect(f.eventsConsistent).toBe(false);
  });

  it("full redemption removes all basis (no rounding residue) and realizes the gain", () => {
    // 3 shares for 10 atoms; burn 1 -> removed floor(10*1/3)=3; then burn 2 == held -> removes remaining 7
    const f = foldPosition([ev("deposit", 10n, 3n), ev("execute_redeem", 4n, 1n), ev("execute_redeem", 9n, 2n)]);
    expect(f.lpShares).toBe(0n);
    expect(f.costBasisAtoms).toBe(0n);
    expect(f.realizedPnlAtoms).toBe((4n - 3n) + (9n - 7n));
    expect(f.eventsConsistent).toBe(true);
  });

  it("partial redemption rounds removed basis down (toward leaving basis)", () => {
    // 7 shares for 100 atoms; burn 2 -> removed floor(200/7)=28
    const f = foldPosition([ev("deposit", 100n, 7n), ev("execute_redeem", 30n, 2n)]);
    expect(f.costBasisAtoms).toBe(72n);
    expect(f.lpShares).toBe(5n);
    expect(f.realizedPnlAtoms).toBe(2n);
  });

  it("redeem with unreadable burn amount flags and realizes full payout", () => {
    const f = foldPosition([ev("deposit", 100n, 100n), ev("execute_redeem", 120n, null)]);
    expect(f.realizedPnlAtoms).toBe(120n);
    expect(f.lpShares).toBe(100n);
    expect(f.eventsConsistent).toBe(false);
    expect(f.inconsistency).toContain("no readable Burn amount");
  });
});

describe("foldPosition: deposits, requests, cancels", () => {
  it("zero-share deposit adds basis but no shares", () => {
    const f = foldPosition([ev("deposit", 50n, 0n)]);
    expect(f.lpShares).toBe(0n);
    expect(f.costBasisAtoms).toBe(50n);
    expect(f.totalDepositedAtoms).toBe(50n);
    expect(f.eventsConsistent).toBe(true);
  });

  it("deposit without mint amount counts basis, flags, and only first reason is kept", () => {
    const f = foldPosition([ev("deposit", 40n, null), ev("request_redeem", 0n, null)]);
    expect(f.costBasisAtoms).toBe(40n);
    expect(f.lpShares).toBe(0n);
    expect(f.eventsConsistent).toBe(false);
    expect(f.inconsistency).toContain("no readable MintTo amount");
  });

  it("request/cancel adjust pending; over-cancel clamps to zero; null amounts flag", () => {
    const f = foldPosition([ev("deposit", 100n, 100n), ev("request_redeem", 0n, 60n), ev("cancel_redeem", 0n, 20n)]);
    expect(f.pendingRedeemShares).toBe(40n);
    expect(f.lpShares).toBe(100n);
    const g = foldPosition([ev("request_redeem", 0n, 5n), ev("cancel_redeem", 0n, 9n)]);
    expect(g.pendingRedeemShares).toBe(0n);
    const h = foldPosition([ev("cancel_redeem", 0n, null)]);
    expect(h.inconsistency).toContain("cancel");
  });

  it("execute_redeem clamps pending when burn exceeds pending", () => {
    const f = foldPosition([ev("deposit", 100n, 100n), ev("request_redeem", 0n, 30n), ev("execute_redeem", 80n, 50n)]);
    expect(f.pendingRedeemShares).toBe(0n);
    expect(f.lpShares).toBe(50n);
    expect(f.costBasisAtoms).toBe(50n);
  });

  it("deposit into a fully exited position resets earlier inconsistency and records freshStartSlot", () => {
    const f = foldPosition([
      ev("execute_redeem", 10n, 5n, { slot: 1 }),
      ev("deposit", 100n, 100n, { slot: 2 }),
    ]);
    expect(f.eventsConsistent).toBe(true);
    expect(f.inconsistency).toBeNull();
    expect(f.freshStartSlot).toBe(2);
  });
});

describe("basisKnown", () => {
  const mk = (over: Partial<FoldedPosition>): FoldedPosition => ({ ...foldPosition([]), ...over });
  it("true when consistent and no transfer detected", () => expect(basisKnown(mk({}), null)).toBe(true));
  it("false when inconsistent regardless of transfer", () => expect(basisKnown(mk({ eventsConsistent: false }), null)).toBe(false));
  it("transfer is sticky until a strictly later fresh start", () => {
    expect(basisKnown(mk({ freshStartSlot: 10 }), 10)).toBe(false);
    expect(basisKnown(mk({ freshStartSlot: 11 }), 10)).toBe(true);
    expect(basisKnown(mk({ freshStartSlot: 0 }), 10)).toBe(false);
  });
});
