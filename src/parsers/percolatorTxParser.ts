import { IX_TAG } from "@percolatorct/sdk";
import { decodeBase58, parseTradeSize } from "@percolatorct/shared";

/**
 * v17 single-fill trade tags (TradeNoCpi=6, TradeCpi=10).
 *
 * TradeCpiV2 (was tag 35/alias 105) is NOT in the v17 wrapper decoder — removed.
 * BatchTradeNoCpi (66) and BatchTradeCpi (67) are handled separately via
 * parseBatchFills because they embed N legs each with their own asset_index.
 */
const SINGLE_TRADE_TAGS = new Set<number>([
  IX_TAG.TradeNoCpi,   // 6
  IX_TAG.TradeCpi,     // 10
]);

const BATCH_TRADE_TAGS = new Set<number>([
  IX_TAG.BatchTradeNoCpi, // 66
  IX_TAG.BatchTradeCpi,   // 67
]);

const ALL_TRADE_TAGS = new Set<number>([
  ...SINGLE_TRADE_TAGS,
  ...BATCH_TRADE_TAGS,
]);

/** v17 single-fill wire: tag(1)+asset_index(2)+size_q(16)+exec_price(8)+fee_bps(8) = 35 bytes. */
const V17_SINGLE_MIN_LEN = 19; // only need tag+asset_index+size_q to parse fills
const V17_SINGLE_ASSET_IDX_OFF = 1; // u16 LE
const V17_SINGLE_SIZE_OFF = 3;      // i128 LE, 16 bytes

/** v17 BatchTrade leg wire: asset_index(2)+size_q(16)+exec_price(8)+fee_bps_or_limit(8) = 34 bytes/leg.
 *  (Matches v16_program.rs decode arms for tags 66/67 and the SDK encodeBatchTrade{NoCpi,Cpi}.) */
const V17_BATCH_HEADER_LEN = 2;    // tag(1)+n_legs(1)
const V17_BATCH_LEG_LEN = 34;      // asset_index(2)+size_q(16)+exec_price(8)+trailing u64(8)
const V17_BATCH_LEG_ASSET_OFF = 0; // u16 LE within leg
const V17_BATCH_LEG_SIZE_OFF = 2;  // i128 LE within leg

export interface ParsedFill {
  signature: string;
  trader: string;
  programId: string;
  /**
   * Asset/domain index within the market group (u16 LE from instruction data).
   * Always 0 for legacy v12 fills. Used to re-key stats by (slab, asset_index).
   */
  assetIndex: number;
  /** Absolute trade size (positive bigint). */
  sizeAbs: bigint;
  /** "long" = positive i128 size, "short" = negative — matches SDK parseTradeSize. */
  side: "long" | "short";
  /**
   * Mark price from program logs — intentionally always `undefined` post-refactor.
   *
   * The old log-derived parser read the `sol_log_64(idx, price, 0, 0, 0)` output
   * and picked the WRONG number on non-liquidation txs, writing bogus prices like
   * $13.15 for real $84 SOL trades. The caller is now responsible for resolving
   * price via slab state (see `readMarkPriceE6`). Kept in the shape for BC with
   * existing callers that branch on `priceE6 ?? 0`.
   */
  priceE6?: number;
}

/**
 * Parse fill events from a Percolator v17 transaction.
 *
 * Handles both single-fill instructions (TradeNoCpi=6, TradeCpi=10) and
 * batch-fill instructions (BatchTradeNoCpi=66, BatchTradeCpi=67) by expanding
 * each batch leg into a separate ParsedFill entry.
 *
 * v17 wire format changes vs v12.x:
 *   - Single fill: tag(1)+asset_index(2)+size_q(16)+... → size at [3:19], NOT [5:21]
 *   - Batch fill: tag(1)+n_legs(1)+[asset_index(2)+size_q(16)+8B]*n (26B/leg)
 *   - TradeCpiV2 (was tag 35 / alias TradeCpiV=105) is NOT a valid v17 instruction
 *
 * Input shape matches either `getParsedTransaction` or Helius Atlas WS
 * `transactionSubscribe` notifications — both produce ParsedTransactionWithMeta-shaped objects.
 *
 * After the 2026-04-20 parser overhaul, this function no longer attempts to extract
 * price from logs — callers MUST resolve price from the slab state at the tx slot
 * via `readMarkPriceE6`.
 */
export function parsePercolatorFills(
  tx: {
    transaction?: { message?: { instructions?: any[] } };
    meta?: { err?: unknown; logMessages?: string[] } | null;
  },
  signature: string,
  programIds: string[],
): ParsedFill[] {
  if (!tx.meta || tx.meta.err) return [];

  const ixs = tx.transaction?.message?.instructions ?? [];
  if (ixs.length === 0) return [];

  const programIdSet = new Set(programIds);
  const fills: ParsedFill[] = [];

  for (const ix of ixs) {
    // Skip parsed instructions (system, token, etc.) — same guard as TradeIndexer.
    if (ix && typeof ix === "object" && "parsed" in ix) continue;

    const programId = pubkeyToBase58(ix.programId);
    if (!programId || !programIdSet.has(programId)) continue;

    const data = decodeBase58(ix.data);
    if (!data || data.length < 1) continue;

    const tag = data[0];
    if (!ALL_TRADE_TAGS.has(tag)) continue;

    const trader = pubkeyToBase58(ix.accounts?.[0]);
    if (!trader) continue;

    if (SINGLE_TRADE_TAGS.has(tag)) {
      // v17 single-fill: tag(1)+asset_index(u16=2)+size_q(i128=16)+... min 19 bytes
      if (data.length < V17_SINGLE_MIN_LEN) continue;

      const assetIndex =
        (data[V17_SINGLE_ASSET_IDX_OFF] | (data[V17_SINGLE_ASSET_IDX_OFF + 1] << 8)) >>> 0;
      const { sizeValue, side } = parseTradeSize(
        data.slice(V17_SINGLE_SIZE_OFF, V17_SINGLE_SIZE_OFF + 16),
      );
      if (sizeValue === 0n) continue;

      fills.push({
        signature,
        trader,
        programId,
        assetIndex,
        sizeAbs: sizeValue,
        side,
        priceE6: undefined,
      });
    } else if (BATCH_TRADE_TAGS.has(tag)) {
      // v17 batch-fill: tag(1)+n_legs(u8=1)+[asset_index(u16=2)+size_q(i128=16)+8B]*n
      if (data.length < V17_BATCH_HEADER_LEN) continue;
      const nLegs = data[1];
      if (nLegs === 0) continue;

      for (let i = 0; i < nLegs; i++) {
        const legOff = V17_BATCH_HEADER_LEN + i * V17_BATCH_LEG_LEN;
        if (legOff + V17_BATCH_LEG_LEN > data.length) break;

        const assetIndex =
          (data[legOff + V17_BATCH_LEG_ASSET_OFF] |
            (data[legOff + V17_BATCH_LEG_ASSET_OFF + 1] << 8)) >>> 0;
        const { sizeValue, side } = parseTradeSize(
          data.slice(
            legOff + V17_BATCH_LEG_SIZE_OFF,
            legOff + V17_BATCH_LEG_SIZE_OFF + 16,
          ),
        );
        if (sizeValue === 0n) continue;

        fills.push({
          signature,
          trader,
          programId,
          assetIndex,
          sizeAbs: sizeValue,
          side,
          priceE6: undefined,
        });
      }
    }
  }

  return fills;
}

function pubkeyToBase58(key: unknown): string | undefined {
  if (!key) return undefined;
  if (typeof key === "string") return key;
  if (typeof key === "object" && key !== null) {
    const k = key as { toBase58?: () => string; pubkey?: { toBase58?: () => string } };
    if (typeof k.toBase58 === "function") return k.toBase58();
    // Some parsed tx shapes wrap { pubkey: PublicKey, isSigner, isWritable }.
    if (k.pubkey && typeof k.pubkey.toBase58 === "function") return k.pubkey.toBase58();
  }
  return undefined;
}
