import { IX_TAG } from "@percolatorct/sdk";

/**
 * Liquidation markers — DISABLED under v18 (see below).
 *
 * ## v17 model (historical)
 * v17 had no liquidation instruction (LiquidateAtOracle tag 7 was removed). A
 * liquidation was a PermissionlessCrank (tag 5) whose second byte was the crank
 * ACTION (`0=Refresh, 1=Liquidate, 2=SettleB`). The indexer detected `data[1]==1`
 * and recorded a size-less marker (the crank carries no size/side/price).
 *
 * ## Why this is a no-op under v18
 * The v18 PermissionlessCrank wire dropped the action byte entirely. The SDK's
 * `encodePermissionlessCrank` (the ground truth for what the deployed wrapper
 * accepts) now emits:
 *
 *     tag(u8=5) + nowSlot(u64) + n_observations(u8) + [assetIndex(u16)+oracleAccounts(u8)]*n
 *
 * So `data[1]` is no longer an action — it is the low byte of `nowSlot`. Decoding
 * it as an action produced a FALSE liquidation marker on ~1/256 of all cranks
 * (whenever `nowSlot & 0xff == 1`), and never caught a real one. Worse, tags 7
 * (LiquidateAtOracle) and 101 (ExecuteAdl) are both `removedInstruction` in the
 * v18 SDK, and the v18 engine/wrapper emit NO liquidation log or event
 * (`grep -ni 'msg!.*liquidat'` on both programs: nothing). In v18 a liquidation
 * is a runtime side effect of a crank against an underwater portfolio — it is not
 * declared by the instruction, so it is NOT identifiable from transaction data at
 * all (neither instruction bytes nor logs).
 *
 * Correctly reflecting that, `parseLiquidation` now always returns null: no
 * instruction is a liquidation under v18. This removes the phantom markers; it
 * does not lose real ones, because there were never any real instruction-level
 * ones to catch. Volume/candle aggregation already excludes `is_liquidation`
 * rows, so producing none is strictly cleaner.
 *
 * ## Future: real v18 liquidation tracking
 * The only remaining signal is account-state diffing — detecting a portfolio
 * whose position was forcibly closed during a crank (compare pre/post crank
 * balances). That is a separate, heavier feature; wire it here (and set the
 * marker fields from the state diff) if a liquidation feed is ever needed.
 */
export interface LiquidationMarker {
  /** Market-group slab. */
  slabAddress: string;
  /** The liquidated portfolio account — NOT the owner wallet. */
  portfolio: string;
  /** Asset/domain index within the market group. */
  assetIndex: number;
}

/**
 * v18: no instruction is a liquidation (see the module doc). Always returns null.
 * The signature is kept so the existing call sites (webhook, EventStream,
 * TradeIndexer) need no change, and so a future state-diff implementation has a
 * single home.
 *
 * @param tag       instruction tag (data[0])
 * @param _data     raw instruction data (unused — v18 cranks carry no action byte)
 * @param _accounts instruction account list (unused)
 */
export function parseLiquidation(
  tag: number,
  _data: Uint8Array,
  _accounts: (string | undefined)[],
): LiquidationMarker | null {
  // Kept as an explicit reference: tag 5 IS the crank, but its v18 wire
  // (nowSlot + observations, no action byte) cannot express "this was a
  // liquidation". Every other tag is likewise not a liquidation instruction.
  void tag; // IX_TAG.PermissionlessCrank === 5
  void IX_TAG;
  return null;
}
