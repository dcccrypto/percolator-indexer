import { getSupabase, createLogger } from "@percolatorct/shared";
import { CURRENT_NETWORK } from "../network.js";

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
export type TradeSizeProvenance =
  | "matcher_exact"
  | "matcher_request_upper_bound"
  | "legacy_unverified";

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
  size_provenance?: TradeSizeProvenance | null;
  /**
   * The unit `price` is quoted in: per 10^price_lot_exp base tokens (0 = per token). Omitted = derived by the
   * writer: every price this indexer build writes is per TOKEN (all producers go through lotScale.perTokenPrice),
   * so a priced row is 0 and a priceless marker is NULL. NULL on old rows = unknown (rows written before the
   * indexer knew lot_exp hold the per-LOT price); readers (/api/candles) only guess for those.
   */
  price_lot_exp?: number | null;
}

/** price_lot_exp of every price written by this indexer build: per token. */
export const PRICE_LOT_EXP_PER_TOKEN = 0;

/**
 * Insert one trade fill. Swallows 23505 (duplicate leg already indexed); throws on
 * any other error so callers with retry/catch semantics behave as before.
 *
 * Returns true when this call wrote the row, false when the leg already existed
 * (23505), so callers can skip side effects (WS events) for duplicates.
 */
export async function insertTradeRow(row: IndexerTradeRow): Promise<boolean> {
  let { error } = await getSupabase().from("trades").insert(toDbRow(row));
  for (let i = 0; i < 2 && error && noteMissingOptionalColumn(error); i++) {
    ({ error } = await getSupabase().from("trades").insert(toDbRow(row)));
  }
  if (error && error.code !== "23505") {
    logger.warn("insertTradeRow failed", {
      slab: row.slab_address.slice(0, 8),
      sig: row.tx_signature?.slice(0, 12),
      code: error.code,
      error: error.message,
    });
    throw new Error(`insertTradeRow failed: ${error.message}`);
  }
  return !error;
}

/** Map the indexer-side shape to the `trades` column set. */
/**
 * GH#195: reject a missing `tx_signature` at the WRITER, not just at the index.
 *
 * The dedup key is `(tx_signature, asset_index, leg_index)`. The migration makes
 * that index NULLS NOT DISTINCT so NULL-signature rows are deduplicated rather
 * than exempt — but a NULL signature is still not a usable identity: every such
 * row from every different transaction collapses onto ONE key, so the second
 * genuine fill lacking a signature would be silently dropped as a duplicate.
 *
 * The index prevents unbounded duplication; this prevents silent LOSS. Both are
 * wanted, and neither substitutes for the other.
 *
 * The webhook path already validates signatures, which is why the exposure was
 * described as latent. This puts the guarantee in the single place every writer
 * goes through — backfill scripts and EventStreamService included — instead of
 * relying on each of them to have remembered.
 */
function assertHasSignature(row: IndexerTradeRow): void {
  if (typeof row.tx_signature !== "string" || row.tx_signature.length === 0) {
    throw new Error(
      "trade row is missing tx_signature — it is part of the dedupe key, so a row " +
        "without one cannot be deduplicated against anything and would collide " +
        "with every other signature-less row (GH#195)",
    );
  }
}

/**
 * #228: `size_provenance` needs migration 20261012090000 (applied by hand). Until it is applied,
 * PostgREST rejects every insert naming the column (PGRST204), which would stop ALL trade
 * indexing. So the first such rejection switches this process to writing without the column
 * (provenance is then simply unrecorded, i.e. NULL, as for historical rows) and logs once.
 */
let provenanceColumnMissingSince: number | null = null;

/** After this long without the column, send it again so an applied migration is picked up without a restart. */
const PROVENANCE_REPROBE_MS = 5 * 60_000;

function provenanceColumnMissing(): boolean {
  if (provenanceColumnMissingSince === null) return false;
  const age = Date.now() - provenanceColumnMissingSince;
  // A wall clock stepped back (NTP) makes the age negative; treat that as expired so the column is
  // re-probed instead of being omitted until the clock catches up.
  return age >= 0 && age < PROVENANCE_REPROBE_MS;
}

/** Same fallback for trades.price_lot_exp (migration 20261120210000): an environment without it keeps indexing. */
let priceLotExpColumnMissingSince: number | null = null;
function priceLotExpColumnMissing(): boolean {
  return priceLotExpColumnMissingSince !== null && Date.now() - priceLotExpColumnMissingSince < PROVENANCE_REPROBE_MS;
}
function noteMissingPriceLotExpColumn(error: { code?: string; message?: string }): boolean {
  if (error.code !== "PGRST204" || !/price_lot_exp/.test(error.message ?? "")) return false;
  const first = priceLotExpColumnMissingSince === null;
  priceLotExpColumnMissingSince = Date.now();
  if (first) logger.error("trades.price_lot_exp is missing (migration 20261120210000 not applied); writing trades without it", { code: error.code, error: error.message });
  return true;
}
/** Either optional column was missing: the write stored nothing and is retried without it. */
function noteMissingOptionalColumn(error: { code?: string; message?: string }): boolean {
  return noteMissingProvenanceColumn(error) || noteMissingPriceLotExpColumn(error);
}

/**
 * True when `error` is the missing-column rejection, i.e. the request wrote nothing and should be retried
 * without the column. Also true when another in-flight write already flagged the column (concurrent writers
 * that were sent with the column before the flag flipped), and for a failed re-probe.
 */
function noteMissingProvenanceColumn(error: { code?: string; message?: string }): boolean {
  if (error.code !== "PGRST204" || !/size_provenance/.test(error.message ?? "")) return false;
  const first = provenanceColumnMissingSince === null;
  provenanceColumnMissingSince = Date.now();
  if (first) {
    logger.error(
      "trades.size_provenance is missing (migration 20261012090000 not applied); writing trades without it",
      { code: error.code, error: error.message },
    );
  }
  return true;
}

/** Test hook: forget a previously detected missing column. */
export function resetProvenanceColumnProbe(): void {
  provenanceColumnMissingSince = null;
  priceLotExpColumnMissingSince = null;
}

/** The row's price_lot_exp: explicit when given (validated 0..15), else per token for a priced row, NULL for a marker. */
export function priceLotExpOf(row: Pick<IndexerTradeRow, "price" | "price_lot_exp">): number | null {
  if (row.price_lot_exp !== undefined) {
    const k = row.price_lot_exp;
    return k !== null && Number.isInteger(k) && k >= 0 && k <= 15 ? k : null;
  }
  return row.price != null ? PRICE_LOT_EXP_PER_TOKEN : null;
}

function toDbRow(row: IndexerTradeRow) {
  assertHasSignature(row);
  const provenance = provenanceColumnMissing() ? {} : { size_provenance: row.size_provenance ?? null };
  const lotScale = priceLotExpColumnMissing() ? {} : { price_lot_exp: priceLotExpOf(row) };
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
    ...provenance,
    ...lotScale,
    network: CURRENT_NETWORK,
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

  const write = () =>
    getSupabase()
      .from("trades")
      .upsert(rows.map(toDbRow), {
        onConflict: "tx_signature,asset_index,leg_index",
        ignoreDuplicates: true,
      })
      .select("tx_signature, asset_index, leg_index");
  let { data, error } = await write();
  for (let i = 0; i < 2 && error && noteMissingOptionalColumn(error); i++) ({ data, error } = await write());

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
