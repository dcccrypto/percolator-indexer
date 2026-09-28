import { IX_TAG } from "@percolatorct/sdk";
import { decodeBase58, parseTradeSize } from "@percolatorct/shared";
import { parseLiquidation, type LiquidationMarker } from "./liquidations.js";

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

/**
 * v18 single-fill wire (integration `a9318945`, byte-exact vs the SDK's
 * `encodeTradeNoCpi`/`encodeTradeCpi` and `app/lib/v18-wire.ts`).
 *
 * Unlike v17 — where TradeNoCpi and TradeCpi shared one layout — v18 gives
 * each its own layout because TradeCpi carries an extra
 * `accountBMatcherSequence` (u64) field TradeNoCpi does not:
 *
 *   TradeNoCpi (tag 6), 77 bytes:
 *     tag(1) + accountAPortfolioId(u64=8) + accountAPositionEpoch(u64=8)
 *     + accountBPortfolioId(u64=8) + accountBPositionEpoch(u64=8)
 *     + asset_index(u16=2) @33 + market_id(u64=8) + size_q(i128=16) @43
 *     + exec_price(u64=8) @59 + fee_bps(u64=8) + backing_fee_cap_bps(u16=2)
 *
 *   TradeCpi (tag 10), 85 bytes:
 *     tag(1) + accountAPortfolioId(u64=8) + accountAPositionEpoch(u64=8)
 *     + accountBPortfolioId(u64=8) + accountBPositionEpoch(u64=8)
 *     + accountBMatcherSequence(u64=8) + asset_index(u16=2) @41
 *     + market_id(u64=8) + size_q(i128=16) @51 + fee_bps(u64=8)
 *     + limit_price(u64=8) + backing_fee_cap_bps(u16=2)
 *
 * `minLen` is only the bytes needed to reach the end of size_q — the indexer
 * does not read the trailing fee/price fields (price is resolved from slab
 * state via readMarkPriceE6, not the instruction data).
 */
const V18_SINGLE_OFFSETS: Readonly<Record<number, { assetIdxOff: number; sizeOff: number; minLen: number }>> = {
  [IX_TAG.TradeNoCpi]: { assetIdxOff: 33, sizeOff: 43, minLen: 59 },
  [IX_TAG.TradeCpi]: { assetIdxOff: 41, sizeOff: 51, minLen: 67 },
};

/**
 * v18 BatchTrade leg wire (byte-exact vs the SDK's `encodeBatchTradeNoCpi`/
 * `encodeBatchTradeCpi`): asset_index(u16=2) + market_id(u64=8) + size_q(i128=16)
 * + 16 trailing bytes whose MEANING differs by variant but whose LENGTH does not
 * (NoCpi: exec_price(8)+fee_bps(8); Cpi: fee_bps(8)+limit_price(8)) = 42 bytes/leg.
 *
 * asset_index and size_q sit at the SAME leg-relative offsets in both variants,
 * so one leg decoder covers both BatchTradeNoCpi (66) and BatchTradeCpi (67) —
 * unlike the single-fill case above, where the two tags diverge.
 *
 * v17 legs were 34 bytes (no market_id field); v18 inserted market_id(u64=8)
 * between asset_index and size_q, growing every leg by 8 bytes.
 */
const V18_BATCH_HEADER_LEN = 2;    // tag(1)+n_legs(1)
const V18_BATCH_LEG_LEN = 42;      // asset_index(2)+market_id(8)+size_q(16)+16B trailer
const V18_BATCH_LEG_ASSET_OFF = 0; // u16 LE within leg
const V18_BATCH_LEG_SIZE_OFF = 10; // i128 LE within leg (after asset_index+market_id)

export interface DecodedFill {
  /** Asset/domain index within the market group (u16 LE). */
  assetIndex: number;
  /** Absolute trade size (positive bigint). */
  sizeValue: bigint;
  /** "long" = positive i128 size, "short" = negative. */
  side: "long" | "short";
}

/**
 * Decode a v18 single-fill instruction (TradeNoCpi=6 or TradeCpi=10).
 * Returns `null` for an unknown tag, undersized data, or a zero-size fill.
 *
 * Single source of truth for the single-fill offsets above — reused by
 * `parsePercolatorFills` (this file), `TradeIndexer.processTransaction`, and
 * `webhook.ts`'s `extractTradesFromEnhancedTx` (outer + inner instructions) so
 * all four ingestion paths decode identically instead of carrying their own
 * copies of the offset math (the pre-v18 duplication was itself a source of
 * drift risk across the poll/webhook/event-stream paths).
 */
export function decodeV18SingleFill(tag: number, data: Uint8Array): DecodedFill | null {
  const off = V18_SINGLE_OFFSETS[tag];
  if (!off || data.length < off.minLen) return null;
  const assetIndex = (data[off.assetIdxOff] | (data[off.assetIdxOff + 1] << 8)) >>> 0;
  const { sizeValue, side } = parseTradeSize(data.slice(off.sizeOff, off.sizeOff + 16));
  if (sizeValue === 0n) return null;
  return { assetIndex, sizeValue, side };
}

/**
 * Decode all legs of a v18 batch-fill instruction (BatchTradeNoCpi=66 or
 * BatchTradeCpi=67). Zero-size legs are skipped, not returned. See
 * {@link decodeV18SingleFill} for the shared-decoder rationale.
 */
export function decodeV18BatchLegs(data: Uint8Array): Array<DecodedFill & { legIndex: number }> {
  const out: Array<DecodedFill & { legIndex: number }> = [];
  if (data.length < V18_BATCH_HEADER_LEN) return out;
  const nLegs = data[1];
  for (let i = 0; i < nLegs; i++) {
    const legOff = V18_BATCH_HEADER_LEN + i * V18_BATCH_LEG_LEN;
    if (legOff + V18_BATCH_LEG_LEN > data.length) break;
    const assetIndex =
      (data[legOff + V18_BATCH_LEG_ASSET_OFF] | (data[legOff + V18_BATCH_LEG_ASSET_OFF + 1] << 8)) >>> 0;
    const { sizeValue, side } = parseTradeSize(
      data.slice(legOff + V18_BATCH_LEG_SIZE_OFF, legOff + V18_BATCH_LEG_SIZE_OFF + 16),
    );
    if (sizeValue === 0n) continue;
    out.push({ assetIndex, sizeValue, side, legIndex: i });
  }
  return out;
}

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
   * Slab (market) address derived from the instruction's account list.
   *
   * #148: Populated per-instruction (not per-tx) so that multi-slab transactions
   * correctly attribute each fill to its own slab rather than the first known slab
   * in the tx's accountKeys.
   *
   * Account layout (v17):
   *   TradeNoCpi (6) / BatchTradeNoCpi (66): accounts[2] = market (writable)
   *   TradeCpi  (10) / BatchTradeCpi  (67): accounts[1] = market (writable)
   *
   * May be undefined if the account list is shorter than expected (malformed ix).
   */
  slabAddress?: string;
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
 * Parse fill events from a Percolator v18 transaction.
 *
 * Handles both single-fill instructions (TradeNoCpi=6, TradeCpi=10) and
 * batch-fill instructions (BatchTradeNoCpi=66, BatchTradeCpi=67) by expanding
 * each batch leg into a separate ParsedFill entry, via {@link decodeV18SingleFill}
 * / {@link decodeV18BatchLegs} (see those for the exact v18 byte layout).
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

    // #148: Derive slab per-instruction from the account list.
    // TradeNoCpi (6) / BatchTradeNoCpi (66): market is at accounts[2].
    // TradeCpi  (10) / BatchTradeCpi  (67): market is at accounts[1].
    // This mirrors the TradeIndexer per-instruction guard so multi-slab txs
    // correctly attribute each fill to its own slab, not the first known slab.
    const isNoCpiTag = (tag === IX_TAG.TradeNoCpi || tag === IX_TAG.BatchTradeNoCpi);
    const marketAccountIdx = isNoCpiTag ? 2 : 1;
    const slabAddress = pubkeyToBase58(ix.accounts?.[marketAccountIdx]);

    if (SINGLE_TRADE_TAGS.has(tag)) {
      const decoded = decodeV18SingleFill(tag, data);
      if (!decoded) continue;

      fills.push({
        signature,
        trader,
        programId,
        assetIndex: decoded.assetIndex,
        sizeAbs: decoded.sizeValue,
        side: decoded.side,
        slabAddress,
        priceE6: undefined,
      });
    } else if (BATCH_TRADE_TAGS.has(tag)) {
      for (const leg of decodeV18BatchLegs(data)) {
        fills.push({
          signature,
          trader,
          programId,
          assetIndex: leg.assetIndex,
          sizeAbs: leg.sizeValue,
          side: leg.side,
          slabAddress,
          priceE6: undefined,
        });
      }
    }
  }

  return fills;
}

/**
 * Extract v17 liquidation markers (PermissionlessCrank action=1) from a tx. Mirrors
 * parsePercolatorFills' instruction iteration. Markers carry no size/price/side.
 */
export function parsePercolatorLiquidations(
  tx: {
    transaction?: { message?: { instructions?: any[] } };
    meta?: { err?: unknown } | null;
  },
  signature: string,
  programIds: string[],
): Array<LiquidationMarker & { signature: string }> {
  if (!tx.meta || tx.meta.err) return [];
  const ixs = tx.transaction?.message?.instructions ?? [];
  if (ixs.length === 0) return [];
  const programIdSet = new Set(programIds);
  const out: Array<LiquidationMarker & { signature: string }> = [];

  for (const ix of ixs) {
    if (ix && typeof ix === "object" && "parsed" in ix) continue;
    const programId = pubkeyToBase58(ix.programId);
    if (!programId || !programIdSet.has(programId)) continue;
    const data = decodeBase58(ix.data);
    if (!data || data.length < 1) continue;
    const accounts = (ix.accounts ?? []).map((a: unknown) => pubkeyToBase58(a));
    const marker = parseLiquidation(data[0], data, accounts);
    if (marker) out.push({ ...marker, signature });
  }
  return out;
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
