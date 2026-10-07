import { Connection, PublicKey } from "@solana/web3.js";
import {
  parseEngine,
  detectSlabLayout,
  isV17Account,
  isV17MarketAccount,
  parseWrapperConfigV17,
  V17_HEADER_LEN,
  V17_MARKET_GROUP_OFF,
  V17_MARKET_GROUP_LEN,
  V17_MARKET_ASSET_SLOT_LEN,
  V17_ASSET_ORACLE_WRAPPER_LEN,
} from "@percolatorct/sdk";
import { createLogger, withRetry } from "@percolatorct/shared";

const logger = createLogger("indexer:mark-price");

/**
 * Offset of `effective_price` inside the engine's `AssetStateV16Account`:
 * market_id[8] + retired_slot[8] + lifecycle[1] + raw_oracle_target_price[8] = 25.
 * Same value SDK >= 8 uses in `readAssetPricesP3` (`ASSET_STATE_EFFECTIVE_PRICE_OFF_P3`).
 */
export const ASSET_STATE_EFFECTIVE_PRICE_REL = 25;

/**
 * #221: the price the engine books a fill at — `asset[assetIndex].effective_price`
 * (raw e6) — read from a v18 market account's bytes, or `null` when the account is
 * not a v18 market, the slot is out of range, or the value is zero/out of range.
 *
 * Every fill settles at this price ("the position enters/settles at the asset mark
 * (effective_price), NOT at the caller-supplied exec_price" — percolator-prog
 * F-TRADENOCPI-FEE), and a trade never changes it: it only nudges the oracle
 * profile's `mark_ewma_e6` toward the matcher's quote. So unlike the mark EWMA,
 * the post-trade value of this field IS the fill's price.
 *
 * Slot layout (SDK 6.0.0 constants, verified on a live v18 market): slots start at
 * V17_MARKET_GROUP_OFF + V17_MARKET_GROUP_LEN, each V17_MARKET_ASSET_SLOT_LEN long,
 * with the engine's AssetStateV16Account after the V17_ASSET_ORACLE_WRAPPER_LEN wrapper.
 */
export function readAssetEffectivePriceE6(data: Uint8Array, assetIndex: number): number | null {
  if (!Number.isInteger(assetIndex) || assetIndex < 0) return null;
  try {
    if (!isV17MarketAccount(data)) return null;
  } catch {
    return null;
  }
  const off =
    V17_MARKET_GROUP_OFF +
    V17_MARKET_GROUP_LEN +
    assetIndex * V17_MARKET_ASSET_SLOT_LEN +
    V17_ASSET_ORACLE_WRAPPER_LEN +
    ASSET_STATE_EFFECTIVE_PRICE_REL;
  if (off + 8 > data.length) return null;
  const v = new DataView(data.buffer, data.byteOffset, data.byteLength).getBigUint64(off, true);
  if (v <= 0n || v >= 1_000_000_000_000n) return null;
  return Number(v);
}

/**
 * Read the slab's on-chain `mark_price_e6` for a Percolator market.
 *
 * Returns the raw e6 integer (e.g. 85_187_279 for $85.187279). Returns `null` on any
 * failure — account missing, layout undetectable, V0 slab (no mark_price field),
 * RPC error, or zero/out-of-range value.
 *
 * Used as the canonical price source for trade fills when log-derived prices are
 * untrustworthy (the sol_log_64 "Program log: idx, price, 0, 0, 0" format is ambiguous
 * across tx types — see `percolatorTxParser.ts` removal of `extractPriceFromLogs`).
 *
 * Mirrors the parsing path in `StatsCollector.collect()` but narrowed to just mark_price.
 *
 * #221: pass the fill's `assetIndex` to price a FILL. On a v18 market that returns
 * the asset's `effective_price` — the price the engine booked the fill at — and only
 * falls back to the mark read below when that slot can't be read. Without it the
 * function keeps its original mark-only behaviour.
 */
export async function readMarkPriceE6(
  connection: Connection,
  slabAddress: string,
  assetIndex?: number,
): Promise<number | null> {
  try {
    const info = await withRetry(
      () => connection.getAccountInfo(new PublicKey(slabAddress)),
      {
        maxRetries: 3,
        baseDelayMs: 1000,
        label: `readMarkPriceE6(${slabAddress.slice(0, 8)})`,
      },
    );
    if (!info?.data) return null;

    const data = new Uint8Array(info.data);

    if (assetIndex !== undefined) {
      const effective = readAssetEffectivePriceE6(data, assetIndex);
      if (effective !== null) return effective;
    }

    // Desync fix 9: v17 account — detectSlabLayout returns null for v17 account sizes
    // (no v17 tier registered). Use parseWrapperConfigV17 to read mark_ewma_e6 directly.
    if (isV17Account(data)) {
      try {
        const cfg = parseWrapperConfigV17(data, V17_HEADER_LEN);
        const markEwmaE6 = cfg.markEwmaE6;
        if (markEwmaE6 > 0n && markEwmaE6 < 1_000_000_000_000n) {
          return Number(markEwmaE6);
        }
      } catch {
        // parseWrapperConfigV17 failed — return null
      }
      return null;
    }

    // Fast-path check before attempting parseEngine: the slab layout must have
    // a mark_price field (V0 does not). detectSlabLayout is cheap — avoids a
    // wasted parseEngine call on V0/V2 slabs.
    const layout = detectSlabLayout(data.length);
    if (!layout || layout.engineMarkPriceOff < 0) return null;

    const engine = parseEngine(data);
    const mp = engine.markPriceE6;
    if (mp <= 0n || mp >= 1_000_000_000_000n) return null; // sentinel / out-of-range
    return Number(mp);
  } catch (err) {
    logger.warn("readMarkPriceE6 failed", {
      slab: slabAddress.slice(0, 8),
      error: err instanceof Error ? err.message : String(err),
    });
    return null;
  }
}
