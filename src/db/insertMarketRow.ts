import { getSupabase, getNetwork, createLogger } from "@percolatorct/shared";

const logger = createLogger("indexer:markets");

/**
 * Indexer-local market writer.
 *
 * Shared's `insertMarket` (a published package) writes a fixed MarketRow that has
 * no `logo_url` and no `metadata_source`, so registrations could never carry a
 * logo and could never be marked as auto-resolved. Rather than fork the shared
 * package, the indexer writes markets directly here — the same approach already
 * taken for trades in insertTradeRow.ts.
 */
export interface IndexerMarketRow {
  slab_address: string;
  mint_address: string;
  symbol: string;
  name: string;
  decimals: number;
  deployer: string;
  oracle_authority: string | null;
  initial_price_e6: number | null;
  max_leverage: number;
  trading_fee_bps: number;
  lp_collateral: string | null;
  matcher_context: string | null;
  status: string;
  /** Resolved token image. NULL when the market's identity is unknown. */
  logo_url: string | null;
}

/**
 * Register a market. Everything the indexer writes is 'auto' by definition —
 * only PATCH /api/markets/[slab] produces 'manual' rows, and this never runs for
 * a slab that already exists (syncMarkets only inserts missing ones), so a
 * human's edits cannot be clobbered here.
 *
 * Swallows 23505 (already registered by a concurrent cycle).
 */
export async function insertMarketRow(row: IndexerMarketRow): Promise<void> {
  const { error } = await getSupabase().from("markets").insert({
    ...row,
    metadata_source: "auto",
    network: getNetwork(),
  });

  if (error && error.code !== "23505") {
    logger.warn("insertMarketRow failed", {
      slab: row.slab_address.slice(0, 8),
      code: error.code,
      error: error.message,
    });
    throw new Error(`insertMarketRow failed: ${error.message}`);
  }
}

/** Identity fields the metadata refresh pass may update. */
export interface MarketMetadataPatch {
  symbol?: string;
  name?: string;
  logo_url?: string | null;
}

/**
 * Update identity for an AUTO-sourced market.
 *
 * The `metadata_source = 'auto'` guard is in the WHERE clause rather than a
 * read-then-write: a human could flip the row to 'manual' between those two
 * steps and the write would silently clobber their edit. As one conditional
 * UPDATE, a row that has become 'manual' simply matches nothing.
 *
 * Returns true when a row was actually updated.
 */
export async function updateAutoMarketMetadata(
  slabAddress: string,
  patch: MarketMetadataPatch,
): Promise<boolean> {
  const { data, error } = await getSupabase()
    .from("markets")
    .update(patch)
    .eq("slab_address", slabAddress)
    .eq("network", getNetwork())
    .eq("metadata_source", "auto")
    .select("slab_address");

  if (error) {
    logger.warn("updateAutoMarketMetadata failed", {
      slab: slabAddress.slice(0, 8),
      code: error.code,
      error: error.message,
    });
    return false;
  }
  return (data?.length ?? 0) > 0;
}
