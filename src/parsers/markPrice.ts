import { Connection, PublicKey } from "@solana/web3.js";
import { parseEngine, detectSlabLayout, resolveMarketGeometry } from "@percolatorct/sdk";
import { hasWrapperMagic, readMarkEwmaE6, reportUnknownLayout } from "../layout/resolve.js";
import { createLogger, withRetry } from "@percolatorct/shared";

const logger = createLogger("indexer:mark-price");

/**
 * #221: the price the engine books a fill at — `asset[assetIndex].effective_price`
 * (raw e6) — read from a wrapper market account's bytes, or `null` when the account is
 * not a market of a VERSION the SDK knows (v2.1 = 18, v2.2 = 19), the slot is out of range, or the value is zero/out of range.
 *
 * Every fill settles at this price ("the position enters/settles at the asset mark
 * (effective_price), NOT at the caller-supplied exec_price" — percolator-prog
 * F-TRADENOCPI-FEE), and a trade never changes it: it only nudges the oracle
 * profile's `mark_ewma_e6` toward the matcher's quote. So unlike the mark EWMA,
 * the post-trade value of this field IS the fill's price.
 *
 * Slot layout comes from the SDK's per-VERSION table (`resolveMarketGeometry`): the engine's
 * AssetStateV16Account sits `wrapperSlotLen` into each slot and `effective_price` is `assetState.effectivePrice`
 * (25) into it, for v2.1 (VERSION 18) and v2.2 (VERSION 19) alike.
 */
export function readAssetEffectivePriceE6(data: Uint8Array, assetIndex: number): number | null {
  if (!Number.isInteger(assetIndex) || assetIndex < 0) return null;
  // VERSION-keyed: the slot stride and the asset-state offset come from the SDK layout table for the account's
  // VERSION (v2.1 = 18, v2.2 = 19), never from a literal. A VERSION the SDK does not know (or a non-wrapper
  // account) yields null so the caller falls back to its other price source instead of reading garbage.
  let off: number;
  try {
    const g = resolveMarketGeometry(data, { parser: "readAssetEffectivePriceE6", strictLength: false });
    if (assetIndex >= g.slotCount) return null;
    off = g.engineOff(assetIndex) + g.layout.assetState.effectivePrice;
  } catch {
    return null;
  }
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
    if (hasWrapperMagic(data)) {
      try {
        const markEwmaE6 = readMarkEwmaE6(data, "readMarkPriceE6");
        if (markEwmaE6 > 0n && markEwmaE6 < 1_000_000_000_000n) {
          return Number(markEwmaE6);
        }
      } catch (err) {
        // Unknown VERSION is loud (per market); any other parse failure — return null
        reportUnknownLayout(slabAddress, err, "readMarkPriceE6");
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
