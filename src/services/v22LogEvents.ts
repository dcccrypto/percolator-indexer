/**
 * Records v2.2 wrapper LOG events (fills, reductions, value moves, and `events_unknown` markers) for one transaction.
 * The decoding and the trust rule are in parsers/v22FillEvents.ts; this is the glue the three ingestion paths share:
 * the Atlas event stream and the poll path read `meta.logMessages` of a jsonParsed transaction, the Helius webhook reads
 * `logMessages` only when the delivery carries them (the enhanced payload normally does not, and then nothing is attempted
 * and nothing is claimed).
 *
 * BEST EFFORT and ISOLATED: never throws, and the rows go through their own `insertV22Events` call, so a problem here can
 * never cost an instruction event, a trade row or a webhook delivery. VERSION-keyed: only markets known to be wrapper
 * VERSION 19 (or not yet read) get events, and only a market known to be VERSION 19 gets an unknown marker.
 */
import { createLogger } from "@percolatorct/shared";
import { isBlockedSlab } from "../blocklist.js";
import { assetGenerationOf, marketVersionOf } from "../layout/marketVersions.js";
import { decodeV22LogEvents, getLogEventStats, type EventGate, type UnknownReason } from "../parsers/v22FillEvents.js";
import type { RawInstruction } from "../parsers/v22Events.js";
import { insertV22Events } from "../db/insertV22Events.js";

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
  status: "not_applicable" | "failed" | "ok" | "unknown";
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
    if (res.rows.length > 0) await insertV22Events(res.rows);
    return { status: res.status, rows: res.rows.length, ...(res.status === "unknown" ? { unknownReason: res.reason } : {}) };
  } catch (err) {
    logger.warn("v22 log-event recording failed (trades unaffected)", { signature: input.signature.slice(0, 16), error: err instanceof Error ? err.message : String(err) });
    return { status: "not_applicable", rows: 0 };
  }
}
