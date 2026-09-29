/**
 * GH#207 — average-cost basis for one (vault, user) LP-vault position.
 *
 * The position is always RECOMPUTED from the full, ordered event list rather
 * than mutated incrementally, so a replayed or out-of-order delivery can never
 * double-apply an event: the events table is the source of truth, keyed
 * (signature, ix_index), and this fold is a pure function of it.
 *
 * Accounting (all amounts in integer atoms / raw LP units):
 *  - deposit:        shares += minted;  basis += collateral_in
 *  - request_redeem: pending += escrowed. The shares stay the user's claim — the
 *                    wrapper only moves them to the vault's escrow (I2 invariant),
 *                    so neither shares nor basis change.
 *  - cancel_redeem:  pending -= returned
 *  - execute_redeem: basis removed pro rata to the shares burned
 *                    (all of it when the position is fully redeemed, so rounding
 *                    never leaves a residue); realized += collateral_out − removed.
 *
 * `earned` for the Earn card is then:
 *     unrealized = value(lp_shares at current NAV) − cost_basis_atoms
 *     total      = unrealized + realized_pnl_atoms
 */
import type { LpVaultEventKind } from "./decoder.js";

export interface FoldEvent {
  kind: LpVaultEventKind;
  slot: number;
  signature: string;
  ixIndex: number;
  collateralAtoms: bigint;
  lpAmount: bigint | null;
}

export interface FoldedPosition {
  /** Shares the user has a claim on: held + escrowed for a pending redemption. */
  lpShares: bigint;
  pendingRedeemShares: bigint;
  costBasisAtoms: bigint;
  realizedPnlAtoms: bigint;
  totalDepositedAtoms: bigint;
  totalRedeemedAtoms: bigint;
  /**
   * False when the event stream alone cannot support an exact basis: an event
   * whose share amount could not be read, or a redemption burning more shares
   * than the indexed position holds (shares arrived by a plain SPL transfer the
   * indexer never sees). Reconciliation against the chain can ALSO clear it —
   * see `basis_known` in the store.
   */
  eventsConsistent: boolean;
  inconsistency: string | null;
  /**
   * Slot of the most recent deposit made into an EMPTY position (no shares, no
   * pending redemption, no basis). Everything before it is closed history, so a
   * transfer detected before this slot no longer taints the basis. 0 if none.
   */
  freshStartSlot: number;
  lastSlot: number;
  eventCount: number;
}

/** Deterministic order: slot, then signature, then instruction position. */
export function compareEvents(a: FoldEvent, b: FoldEvent): number {
  if (a.slot !== b.slot) return a.slot - b.slot;
  if (a.signature !== b.signature) return a.signature < b.signature ? -1 : 1;
  return a.ixIndex - b.ixIndex;
}

export function foldPosition(events: readonly FoldEvent[]): FoldedPosition {
  const sorted = [...events].sort(compareEvents);
  let lpShares = 0n;
  let pending = 0n;
  let basis = 0n;
  let realized = 0n;
  let deposited = 0n;
  let redeemed = 0n;
  let consistent = true;
  let inconsistency: string | null = null;
  const flag = (why: string): void => {
    if (consistent) inconsistency = why;
    consistent = false;
  };

  let freshStartSlot = 0;

  for (const e of sorted) {
    switch (e.kind) {
      case "deposit":
        if (lpShares === 0n && pending === 0n && basis === 0n) {
          // Fully exited before this deposit: earlier inconsistencies concern
          // shares that no longer exist in the indexed position.
          freshStartSlot = e.slot;
          consistent = true;
          inconsistency = null;
        }
        deposited += e.collateralAtoms;
        if (e.lpAmount === null) {
          flag(`deposit ${e.signature} has no readable MintTo amount`);
          // Collateral still counts toward basis; the share count is unknown.
          basis += e.collateralAtoms;
          break;
        }
        lpShares += e.lpAmount;
        basis += e.collateralAtoms;
        break;
      case "request_redeem":
        if (e.lpAmount === null) { flag(`request ${e.signature} has no share amount`); break; }
        pending += e.lpAmount;
        break;
      case "cancel_redeem":
        if (e.lpAmount === null) { flag(`cancel ${e.signature} has no share amount`); break; }
        pending = pending > e.lpAmount ? pending - e.lpAmount : 0n;
        break;
      case "execute_redeem": {
        redeemed += e.collateralAtoms;
        const burned = e.lpAmount;
        if (burned === null) {
          flag(`redemption ${e.signature} has no readable Burn amount`);
          realized += e.collateralAtoms;
          break;
        }
        if (burned > lpShares) {
          // More shares redeemed than the index ever saw minted: some arrived by
          // transfer. Their basis is unknowable; close out what we do know.
          flag(`redemption ${e.signature} burned ${burned} > indexed ${lpShares}`);
          realized += e.collateralAtoms - basis;
          basis = 0n;
          lpShares = 0n;
          pending = 0n;
          break;
        }
        const removed = burned === lpShares ? basis : (basis * burned) / lpShares;
        realized += e.collateralAtoms - removed;
        basis -= removed;
        lpShares -= burned;
        pending = pending > burned ? pending - burned : 0n;
        break;
      }
    }
  }

  return {
    lpShares,
    pendingRedeemShares: pending,
    costBasisAtoms: basis,
    realizedPnlAtoms: realized,
    totalDepositedAtoms: deposited,
    totalRedeemedAtoms: redeemed,
    eventsConsistent: consistent,
    inconsistency,
    freshStartSlot,
    lastSlot: sorted.length ? sorted[sorted.length - 1]!.slot : 0,
    eventCount: sorted.length,
  };
}

/**
 * The `basis_known` flag for a position.
 *
 * `transferDetectedSlot` is the context slot at which reconciliation last saw an
 * on-chain LP claim different from the indexed shares — i.e. shares moved by a
 * plain SPL transfer the indexer cannot price. It is STICKY: no later event makes
 * that basis exact again, EXCEPT a fresh start (a deposit into an empty indexed
 * position after the detection). If the user still held transferred shares at
 * that point, the next reconciliation detects it again at a newer slot.
 */
export function basisKnown(folded: FoldedPosition, transferDetectedSlot: number | null): boolean {
  if (!folded.eventsConsistent) return false;
  if (transferDetectedSlot === null) return true;
  return folded.freshStartSlot > transferDetectedSlot;
}
