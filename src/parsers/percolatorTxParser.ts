import { IX_TAG } from "@percolatorct/sdk";
import { decodeBase58, parseTradeSize } from "@percolatorct/shared";

/** Trade instruction tags we want to index — mirrors TradeIndexerPolling. */
const TRADE_TAGS = new Set<number>([
  IX_TAG.TradeNoCpi,
  IX_TAG.TradeCpi,
  IX_TAG.TradeCpiV2,
]);

export interface ParsedFill {
  signature: string;
  trader: string;
  programId: string;
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
 * Parse fill events from a Percolator transaction.
 *
 * Input shape matches either `getParsedTransaction` or Helius Atlas WS
 * `transactionSubscribe` notifications — both produce ParsedTransactionWithMeta-shaped objects.
 *
 * Mirrors the trade-parsing logic of TradeIndexerPolling.processTransaction but is pure:
 * no DB writes, no RPC calls. After the 2026-04-20 parser overhaul, this function no
 * longer attempts to extract price from logs — callers MUST resolve price from the slab
 * state at the tx slot via `readMarkPriceE6`.
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
    // tag(1) + lpIdx(2) + userIdx(2) + size(16) = 21 bytes minimum.
    // TradeCpiV2 adds bump(u8) at byte 21 — size range unchanged.
    if (!data || data.length < 21) continue;

    const tag = data[0];
    if (!TRADE_TAGS.has(tag)) continue;

    const { sizeValue, side } = parseTradeSize(data.slice(5, 21));
    if (sizeValue === 0n) continue;

    const trader = pubkeyToBase58(ix.accounts?.[0]);
    if (!trader) continue;

    fills.push({
      signature,
      trader,
      programId,
      sizeAbs: sizeValue, // parseTradeSize already returns absolute value
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
