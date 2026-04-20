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
  /** Mark price in the slab-native integer representation (from logs, undefined if absent). */
  priceE6?: number;
}

/**
 * Parse fill events from a Percolator transaction.
 *
 * Input shape matches either `getParsedTransaction` or Helius Atlas WS
 * `transactionSubscribe` notifications — both produce ParsedTransactionWithMeta-shaped objects.
 *
 * Mirrors the trade-parsing logic of TradeIndexerPolling.processTransaction but is pure:
 * no DB writes, no RPC calls, no price fallback to the slab account. The caller is responsible
 * for persistence (Task 2.4) and for any additional price resolution.
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
  const priceE6 = extractPriceFromLogs(tx.meta.logMessages ?? []);
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
      priceE6,
    });
  }

  return fills;
}

/** Extract mark price in e6 units from `Program log: mark_price=<n>` style lines. */
function extractPriceFromLogs(logs: string[]): number | undefined {
  for (const line of logs) {
    const m = line.match(/mark_price=(\d+)/);
    if (m) return Number(m[1]);
  }
  return undefined;
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
