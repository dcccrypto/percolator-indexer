import { Connection, PublicKey } from "@solana/web3.js";
import { parseEngine, detectSlabLayout, isV17Account, parseWrapperConfigV17, V17_HEADER_LEN } from "@percolatorct/sdk";
import { createLogger, withRetry } from "@percolatorct/shared";

const logger = createLogger("indexer:mark-price");

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
 */
export async function readMarkPriceE6(
  connection: Connection,
  slabAddress: string,
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
