import { IX_TAG } from "@percolatorct/sdk";
import { decodeBase58, parseTradeSize } from "@percolatorct/shared";

/**
 * Trade instruction tags we want to index — mirrors TradeIndexerPolling.
 *
 * v17 breaking changes vs v12.x:
 *   - TradeCpiV2(105): removed, not in v17 decoder. DROPPED.
 *   - BatchTradeNoCpi(66), BatchTradeCpi(67): new v17 multi-leg variants. ADDED.
 *   - size_q offset: v12 was bytes [5..21] (after lpIdx+userIdx). v17 is bytes [3..19]
 *     (after asset_index only; lpIdx/userIdx removed).
 */
const TRADE_TAGS = new Set<number>([
  IX_TAG.TradeNoCpi,
  IX_TAG.TradeCpi,
  IX_TAG.BatchTradeNoCpi,
  IX_TAG.BatchTradeCpi,
]);

export interface ParsedFill {
  signature: string;
  trader: string;
  programId: string;
  /** Absolute trade size (positive bigint). */
  sizeAbs: bigint;
  /** "long" = positive i128 size, "short" = negative — matches SDK parseTradeSize. */
  side: "long" | "short";
  /** Asset/domain index for this trade leg (u16). Added in v17. */
  assetIndex: number;
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
 * Parse fill events from a Percolator transaction.
 *
 * Input shape matches either `getParsedTransaction` or Helius Atlas WS
 * `transactionSubscribe` notifications — both produce ParsedTransactionWithMeta-shaped objects.
 *
 * Mirrors the trade-parsing logic of TradeIndexerPolling.processTransaction but is pure:
 * no DB writes, no RPC calls. After the 2026-04-20 parser overhaul, this function no
 * longer attempts to extract price from logs — callers MUST resolve price from the slab
 * state at the tx slot via `readMarkPriceE6`.
 *
 * v17 layout (single-trade, tags 6 and 10):
 *   tag(1) + asset_index(u16=2) + size_q(i128=16) + exec_price_or_fee(u64=8) + fee_or_limit(u64=8) = 35 bytes
 *   Minimum parseable: tag(1) + asset_index(2) + size_q(16) = 19 bytes.
 *
 * v17 layout (batch-trade, tags 66 and 67):
 *   tag(1) + n_legs(u8=1) + [asset_index(u16=2) + size_q(i128=16) + ...]×n
 *   We index the first leg only. Minimum parseable: 1+1+2+16 = 20 bytes.
 */
export function parsePercolatorFills(
  tx: {
    transaction?: { message?: { instructions?: unknown[] } };
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

    const ixObj = ix as { programId: unknown; data?: unknown; accounts?: unknown[] };
    const programId = pubkeyToBase58(ixObj.programId);
    if (!programId || !programIdSet.has(programId)) continue;

    if (typeof ixObj.data !== "string") continue;
    const data = decodeBase58(ixObj.data);
    if (!data || data.length < 1) continue;

    const tag = data[0];
    if (!TRADE_TAGS.has(tag)) continue;

    // BatchTrade tags have n_legs(u8) between the tag byte and the first leg.
    const isBatch = tag === IX_TAG.BatchTradeNoCpi || tag === IX_TAG.BatchTradeCpi;
    const legOffset = isBatch ? 1 : 0;

    // Minimum: tag(1) + [n_legs(1)] + asset_index(2) + size_q(16)
    if (data.length < 1 + legOffset + 2 + 16) continue;

    const assetIndexOffset = 1 + legOffset;
    const sizeOffset = assetIndexOffset + 2;

    // asset_index: u16 LE
    const assetIndex = data[assetIndexOffset] | (data[assetIndexOffset + 1] << 8);

    // size_q: i128 LE, 16 bytes (v17 offset 3 for single, 4 for batch first leg)
    const { sizeValue, side } = parseTradeSize(data.slice(sizeOffset, sizeOffset + 16));
    if (sizeValue === 0n) continue;

    const trader = pubkeyToBase58(ixObj.accounts?.[0]);
    if (!trader) continue;

    fills.push({
      signature,
      trader,
      programId,
      sizeAbs: sizeValue, // parseTradeSize already returns absolute value
      assetIndex,
      side,
      // priceE6 intentionally omitted — caller resolves via slab (readMarkPriceE6).
      // See the field docstring for the why; the old `mark_price=<n>` regex never
      // matched anything the program actually emitted.
      priceE6: undefined,
    });
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
