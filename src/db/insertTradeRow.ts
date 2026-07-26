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
  const { error } = await getSupabase().from("trades").insert(toDbRow(row));
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

/** Map the indexer-side shape to the `trades` column set. */
function toDbRow(row: IndexerTradeRow) {
  return {
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
  };
}

/**
 * Insert many fills in ONE round-trip.
 *
 * A Helius webhook delivery routinely carries several transactions, each of which
 * can contain several fill legs. Inserting them one at a time serializes that many
 * round-trips inside Helius's ~15s delivery window; batching keeps it at one.
 *
 * Duplicate handling differs from the single-row path by necessity: a plain batch
 * INSERT is all-or-nothing, so one already-indexed leg would 23505 the whole batch
 * and drop the new legs with it. This uses upsert(..., ignoreDuplicates) against the
 * composite key so known legs are skipped and the rest still land — same idempotent
 * re-index semantics as insertTradeRow, applied per row instead of per batch.
 *
 * Returns the dedupe key of each row actually inserted — skipped duplicates are
 * absent — so callers can fire side effects (WS events) only for genuinely new
 * trades rather than for every row they submitted.
 * Throws on any non-duplicate error so the caller's retry/Sentry path is unchanged.
 */
export async function insertTradeRows(rows: IndexerTradeRow[]): Promise<InsertedTradeKey[]> {
  if (rows.length === 0) return [];

  const { data, error } = await getSupabase()
    .from("trades")
    .upsert(rows.map(toDbRow), {
      onConflict: "tx_signature,asset_index,leg_index",
      ignoreDuplicates: true,
    })
    .select("tx_signature, asset_index, leg_index");

  if (error) {
    logger.warn("insertTradeRows failed", {
      count: rows.length,
      code: error.code,
      error: error.message,
    });
    throw new Error(`insertTradeRows failed: ${error.message}`);
  }

  return (data ?? []) as InsertedTradeKey[];
}

/** Identifies a written row via the (tx_signature, asset_index, leg_index) dedupe key. */
export interface InsertedTradeKey {
  tx_signature: string | null;
  asset_index: number;
  leg_index: number;
}

/** Stable string form of the dedupe key, for set membership. */
export function tradeKey(r: { tx_signature: string | null; asset_index: number; leg_index: number }): string {
  return `${r.tx_signature ?? ""}|${r.asset_index}|${r.leg_index}`;
}
