import { IX_TAG } from "@percolatorct/sdk";

/**
 * A v17 liquidation marker.
 *
 * v17 has NO liquidation instruction (LiquidateAtOracle tag 7 was removed). A
 * liquidation is a PermissionlessCrank (tag 5) with action byte 1 (Liquidate). The
 * crank instruction data and account list identify WHO/WHICH-asset/WHEN, but NOT the
 * size or price — the engine selects the liquidation size at runtime and there is no
 * structured log. So markers carry no size/side/price; they are excluded from volume
 * and candle aggregation (WHERE is_liquidation = false).
 *
 * Wire (encodePermissionlessCrank): tag(1) + action(u8) + asset_index(u16) + ...
 * Accounts (v16_program handle_permissionless_crank): [0] owner, [1] market,
 * [2] portfolio (the liquidated portfolio).
 */
export interface LiquidationMarker {
  /** Market-group slab (accounts[1]). */
  slabAddress: string;
  /** The liquidated portfolio account (accounts[2]) — NOT the owner wallet. */
  portfolio: string;
  /** Asset/domain index within the market group. */
  assetIndex: number;
}

const CRANK_ACTION_LIQUIDATE = 1; // 0=Refresh, 1=Liquidate, 2=SettleB

/**
 * Return a LiquidationMarker if `ix` is a liquidation crank, else null.
 * Cranks fire constantly (mostly action 0 = Refresh), so this is a cheap early-out.
 *
 * @param tag       instruction tag (data[0])
 * @param data      raw instruction data
 * @param accounts  instruction account list as base58 strings
 */
export function parseLiquidation(
  tag: number,
  data: Uint8Array,
  accounts: (string | undefined)[],
): LiquidationMarker | null {
  if (tag !== IX_TAG.PermissionlessCrank) return null; // tag 5
  // Need tag(1)+action(1)+asset_index(2) = 4 bytes, and action must be Liquidate.
  if (data.length < 4 || data[1] !== CRANK_ACTION_LIQUIDATE) return null;
  const slabAddress = accounts[1];
  const portfolio = accounts[2];
  if (!slabAddress || !portfolio) return null;
  const assetIndex = (data[2] | (data[3] << 8)) >>> 0; // u16 LE
  return { slabAddress, portfolio, assetIndex };
}
