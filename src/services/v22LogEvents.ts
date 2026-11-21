/**
 * Records v2.2 wrapper LOG events (fills, reductions, value moves, and `events_unknown` markers) for one transaction.
 * The decoding and the trust rule are in parsers/v22FillEvents.ts; this is the glue the three ingestion paths share:
 * the Atlas event stream and the poll path read `meta.logMessages` of a jsonParsed transaction, the Helius webhook reads
 * `logMessages` only when the delivery carries them (the enhanced payload normally does not, and then nothing is attempted
 * and nothing is claimed).
 *
 * BEST EFFORT and ISOLATED: never throws (a failed DB write is reported as status "write_failed"), and the rows go through their own `insertV22Events` call, so a problem here can
 * never cost an instruction event or a trade row (a failed DB write does make the webhook answer 5xx, deliberately). VERSION-keyed: only markets known to be wrapper
 * VERSION 19 (or not yet read) get events, and only a market known to be VERSION 19 gets an unknown marker.
 */
import { createLogger } from "@percolatorct/shared";
import { isBlockedSlab } from "../blocklist.js";
import { assetGenerationOf, marketVersionOf } from "../layout/marketVersions.js";
import { decodeV22LogEvents, getLogEventStats, type EventGate, type UnknownReason } from "../parsers/v22FillEvents.js";
import type { RawInstruction } from "../parsers/v22Events.js";
import { insertV22Events, V22EventWriteError } from "../db/insertV22Events.js";
import { insertTradeRows, type IndexerTradeRow } from "../db/insertTradeRow.js";
import type { V22EventRow } from "../parsers/v22Events.js";

/** REDUCE reason 3 (`REDUCE_REASONS` in parsers/v22FillEvents.ts): the crank's forced close. */
const REDUCE_REASON_LIQUIDATION = 3;

/**
 * `parseLiquidation` is a no-op (no v2.2 instruction declares a liquidation), so the liquidation REDUCE log event is the only
 * signal. Turn each current-generation one into the size-less `is_liquidation` marker the trades readers expect
 * (liquidation counts and trader history by portfolio; volume rollups already exclude markers). NOTE: traderNetPosition's guard is keyed by owner wallet, so it does not see these portfolio-keyed markers. leg_index 1000+ keeps clear of fill legs; it is 1000 + 100 * ix_index + the
 * row's position among this transaction's liquidation events for the same (instruction, slab, asset), so every path and a re-delivery map to the same key (insert ignores duplicates).
 */
export function liquidationMarkers(rows: readonly V22EventRow[]): IndexerTradeRow[] {
  const out: IndexerTradeRow[] = [];
  // Ordinal per (instruction, slab, asset), counted over EVERY reason-3 event before the generation filter, so it does not depend on which
  // markets a path's registry accepts or on whether the generation is known yet: poll, stream and webhook agree on the key.
  const ordinals = new Map<string, number>();
  for (const r of rows) {
    if (r.kind !== "reduce_event" || r.detail?.reason !== REDUCE_REASON_LIQUIDATION) continue;
    if (!r.slab_address || !r.actor || r.asset_index == null) continue;
    const k = `${r.ix_index}|${r.slab_address}|${r.asset_index}`;
    const ord = ordinals.get(k) ?? 0;
    ordinals.set(k, ord + 1);
    if (r.detail?.gen_current === false) continue;
    out.push({
      slab_address: r.slab_address,
      // The event names the liquidated PORTFOLIO, not its owner wallet (not derivable from the logs): the marker is keyed by it.
      trader: r.actor,
      side: null,
      size: null,
      price: null,
      fee: 0,
      tx_signature: r.signature,
      asset_index: r.asset_index,
      // The trades key is (tx_signature, asset_index, leg_index) with NO slab, so two markets' liquidations of the same asset in
      // one sweep must not share a leg. The instruction index separates them (one crank names one market) and, unlike a slab-wide
      // ordinal, is the same on every path whatever its market gate.
      leg_index: 1000 + r.ix_index * 100 + Math.min(ord, 99),
      is_liquidation: true,
    });
  }
  return out;
}

const logger = createLogger("indexer:v22-log-events");

/** Log an unknown-events transaction at most this often (a systematically log-less RPC would otherwise flood). */
const UNKNOWN_RELOG_MS = 60_000;
let lastUnknownLog = 0;

export interface RecordLogEventsInput {
  signature: string;
  /** `meta.err`. */
  err: unknown;
  /** `meta.logMessages` (null / undefined when the node did not keep them). */
  logMessages: unknown;
  /** The transaction's instructions of the pinned wrapper program ids (top-level and inner). */
  wrapperInstructions: readonly RawInstruction[];
  wrapperIds: ReadonlySet<string>;
  slot?: number | null;
  blockTimeSec?: number | null;
  /** The registry: true when `slab` is a market this path indexes. */
  isKnownMarket: (slab: string) => boolean;
}

export interface RecordLogEventsResult {
  status: "not_applicable" | "failed" | "ok" | "unknown" | "write_failed";
  rows: number;
  unknownReason?: UnknownReason;
}

/** Decode and write one transaction's log events. Never throws. */
export async function recordV22LogEvents(input: RecordLogEventsInput): Promise<RecordLogEventsResult> {
  try {
    const gate: EventGate = {
      isKnownMarket: (s) => !isBlockedSlab(s) && input.isKnownMarket(s),
      versionOf: marketVersionOf,
      generationOf: assetGenerationOf,
    };
    const res = decodeV22LogEvents(
      { err: input.err, logMessages: input.logMessages },
      input.wrapperInstructions,
      input.wrapperIds,
      gate,
      { signature: input.signature, slot: input.slot ?? null, blockTimeSec: input.blockTimeSec ?? null },
    );
    if (res.status === "unknown") {
      const now = Date.now();
      if (now - lastUnknownLog >= UNKNOWN_RELOG_MS) {
        lastUnknownLog = now;
        logger.warn("v2.2 events UNKNOWN for a transaction (not 'no fill'): its positions need reconciling from account state", {
          metric: "indexer_v22_events_unknown_total",
          signature: input.signature.slice(0, 16),
          reason: res.reason,
          totals: getLogEventStats().unknownByReason,
        });
      }
    }
    if (res.rows.length > 0) {
      try {
        await insertV22Events(res.rows);
      } catch (err) {
        if (!(err instanceof V22EventWriteError)) throw err;
        // The DB write failed (already counted + alerted in insertV22Events). Report it instead of pretending it was recorded,
        // so a caller that can be redelivered (the webhook) answers 5xx.
        return { status: "write_failed", rows: res.rows.length };
      }
    }
    const markers = liquidationMarkers(res.rows);
    if (markers.length > 0) {
      try {
        await insertTradeRows(markers);
      } catch (err) {
        logger.warn("v2.2 liquidation marker write failed (event row kept)", { signature: input.signature.slice(0, 16), error: err instanceof Error ? err.message : String(err) });
      }
    }
    return { status: res.status, rows: res.rows.length, ...(res.status === "unknown" ? { unknownReason: res.reason } : {}) };
  } catch (err) {
    logger.warn("v22 log-event recording failed (trades unaffected)", { signature: input.signature.slice(0, 16), error: err instanceof Error ? err.message : String(err) });
    return { status: "not_applicable", rows: 0 };
  }
}
