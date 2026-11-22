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
 * keeper_status for every indexer-inserted row.
 *
 * Not 'active': auto-discovery must never enroll a market for keeper pricing
 * (the oracle keeper and price-ws read only keeper_status='active'). Only the
 * authenticated registration path in the app sets 'active'.
 *
 * Not the column default 'retired' either. 'retired' is this indexer's own
 * stop-ingesting lever (blocklist.ts setDbRetiredSlabs), so a brand-new market
 * came back retired and its trades and events were dropped until someone
 * registered it. Measured 2026-10-01: 9EPm8nB8… was inserted at 01:21 and
 * registered at 01:55. 'pending' means discovered and not yet registered.
 * Migration 20261001020000 adds it to markets_keeper_status_check.
 */
export const AUTO_ROW_KEEPER_STATUS = "pending";

/**
 * What insertMarketRow found. "exists" is the harmless case: a concurrent cycle (or the
 * app's registration) already wrote this network's row and it is not retired.
 */
export type InsertMarketRowResult =
  | "inserted"
  | "exists"
  | "exists_retired"
  | "exists_other_network";

/**
 * Register a market. Everything the indexer writes is 'auto' by definition —
 * only PATCH /api/markets/[slab] produces 'manual' rows, and this never runs for
 * a slab that already exists (syncMarkets only inserts missing ones), so a
 * human's edits cannot be clobbered here.
 *
 * On 23505 the existing row is read back, because markets.slab_address is UNIQUE
 * across the whole table, whatever the row's network or keeper_status:
 *  - same network, not retired: a concurrent cycle got there first. Quiet.
 *  - another network: getMarketRows() only reads this network's rows, so the slab
 *    looks missing every sweep, every insert hits 23505, and this network never
 *    gets its row. Warned; it needs a human to fix the row's network.
 *  - same network, 'retired': blocklist.ts drops the slab's trades and events.
 *    Warned, NOT flipped: 'retired' is also the operator's deliberate lever, and
 *    a legacy column-default row cannot be told apart from it here.
 * The existing row is never written to. A failed read-back is warned and treated
 * as "exists", the same as before.
 */
export async function insertMarketRow(row: IndexerMarketRow): Promise<InsertMarketRowResult> {
  const network = getNetwork();
  const { error } = await getSupabase().from("markets").insert({
    ...row,
    metadata_source: "auto",
    keeper_status: AUTO_ROW_KEEPER_STATUS,
    network,
  });

  if (!error) return "inserted";
  if (error.code !== "23505") {
    logger.warn("insertMarketRow failed", {
      slab: row.slab_address.slice(0, 8),
      code: error.code,
      error: error.message,
    });
    throw new Error(`insertMarketRow failed: ${error.message}`);
  }
  return classifyExistingRow(row.slab_address, network);
}

async function classifyExistingRow(
  slabAddress: string,
  network: string,
): Promise<InsertMarketRowResult> {
  const slab = slabAddress.slice(0, 8);
  let existing: { network: string; keeper_status: string } | null;
  try {
    const { data, error } = await getSupabase()
      .from("markets")
      .select("network,keeper_status")
      .eq("slab_address", slabAddress)
      .maybeSingle();
    if (error) {
      logger.warn("insertMarketRow: 23505, existing row could not be read back", {
        slab,
        code: error.code,
        error: error.message,
      });
      return "exists";
    }
    existing = data as typeof existing;
  } catch (err) {
    logger.warn("insertMarketRow: 23505, existing row could not be read back", {
      slab,
      error: err instanceof Error ? err.message : String(err),
    });
    return "exists";
  }

  if (!existing) return "exists";
  if (existing.network !== network) {
    logger.warn(
      "insertMarketRow: slab already has a markets row under another network; this network's row cannot be created (slab_address is unique) and the market is re-tried every sweep",
      { slab, network, existingNetwork: existing.network, keeperStatus: existing.keeper_status },
    );
    return "exists_other_network";
  }
  if (existing.keeper_status === "retired") {
    logger.warn(
      "insertMarketRow: slab already has a 'retired' markets row; its trades and events are dropped until keeper_status is changed",
      { slab, network, keeperStatus: existing.keeper_status },
    );
    return "exists_retired";
  }
  return "exists";
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
