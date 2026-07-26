import { getSupabase, getNetwork, createLogger } from "@percolatorct/shared";

const logger = createLogger("indexer:trades");

/**
 * Indexer-local trade writer (H2/H3).
 *
 * Shared's `insertTrade` (a published package) writes a fixed TradeRow that has no
 * `asset_index` / `leg_index` / `is_liquidation`, so multi-fill batch legs collapse
 * under `UNIQUE(tx_signature)`. Rather than fork the shared package, the indexer
 * writes trades directly here with the full v17 shape and dedupes on the composite
 * key `(tx_signature, asset_index, leg_index)` via the partial unique index — a
 * duplicate leg raises 23505, which we swallow (idempotent re-index).
 */
export interface IndexerTradeRow {
  slab_address: string;
  trader: string;
  /** null for is_liquidation markers (v17 exposes no side/size/price for forced closes). */
  side: "long" | "short" | null;
  size: string | number | null; // i128 magnitude; string preserves full precision
  price: number | null;
  fee: number;
  tx_signature: string | null;
  /** u16 asset/domain index within the market group (0 for legacy single-asset). */
  asset_index: number;
  /** Position of this fill within its transaction (disambiguates batch legs). */
  leg_index: number;
  /** True for forced closes captured from the crank (markers; see baseline note). */
  is_liquidation?: boolean;
}

/**
 * Insert one trade fill. Swallows 23505 (duplicate leg already indexed); throws on
 * any other error so callers with retry/catch semantics behave as before.
 */
export async function insertTradeRow(row: IndexerTradeRow): Promise<void> {
  const { error } = await getSupabase().from("trades").insert({
    slab_address: row.slab_address,
    trader: row.trader,
    side: row.side,
    size: row.size,
    price: row.price,
    fee: row.fee,
    tx_signature: row.tx_signature,
    asset_index: row.asset_index,
    leg_index: row.leg_index,
    is_liquidation: row.is_liquidation ?? false,
    network: getNetwork(),
  });
  if (error && error.code !== "23505") {
    logger.warn("insertTradeRow failed", {
      slab: row.slab_address.slice(0, 8),
      sig: row.tx_signature?.slice(0, 12),
      code: error.code,
      error: error.message,
    });
    throw new Error(`insertTradeRow failed: ${error.message}`);
  }
}
