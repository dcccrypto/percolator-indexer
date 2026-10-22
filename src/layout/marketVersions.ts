/**
 * What the indexer last saw of each market's wrapper VERSION and per-asset generation (`market_id`).
 *
 * Two consumers, both about v2.2 events (parsers/v22FillEvents.ts):
 *   - the VERSION gate: a market known to be VERSION 18 (v2.1) never gets v2.2 events or `events_unknown` markers;
 *   - the generation flag: an event carries the asset's `asset_gen` (= the asset's `market_id`); comparing it with the
 *     generation last read tells whether the event is about the asset as it is now. The cache is refreshed by the
 *     collector's sweep (about a minute) and by every discovery pass, so a freshly activated asset can be one sweep
 *     behind: a mismatch is therefore FLAGGED on the row, never a reason to drop it.
 *
 * Fed from account bytes the indexer already reads (StatsCollector.sweep, discovery); nothing here reads the chain.
 */
import { resolveMarketGeometry } from "@percolatorct/sdk";

/** `AssetStateV16Account` begins with `market_id` u64: the asset's generation sits at the start of its engine slot. */
export const ASSET_STATE_MARKET_ID_OFF = 0;

interface MarketLayoutNote {
  version: number;
  /** asset index -> market_id, for the slots whose bytes the buffer held. */
  generations: Map<number, bigint>;
}

const notes = new Map<string, MarketLayoutNote>();
/** A slab is a few KB of notes; the cap is a backstop against an unbounded set of garbage accounts. */
const MAX_NOTES = 20_000;

/**
 * Record a market account's VERSION and the generations of the asset slots present in `data`. Best effort: bytes that
 * are not a market (wrong magic / kind / unknown VERSION) record nothing. Never throws.
 */
export function noteMarketLayout(slab: string, data: Uint8Array): void {
  try {
    const g = resolveMarketGeometry(data, { parser: "noteMarketLayout", strictLength: false });
    const generations = new Map<number, bigint>();
    const dv = new DataView(data.buffer, data.byteOffset, data.byteLength);
    for (let i = 0; i < g.slotCount; i++) {
      const off = g.engineOff(i) + ASSET_STATE_MARKET_ID_OFF;
      if (off + 8 > data.length) break;
      generations.set(i, dv.getBigUint64(off, true));
    }
    if (notes.size >= MAX_NOTES && !notes.has(slab)) return;
    const prev = notes.get(slab);
    // A light (sliced) read carries the version but no slots: keep the generations an earlier full read recorded.
    notes.set(slab, { version: g.layout.version, generations: generations.size > 0 || !prev ? generations : prev.generations });
  } catch {
    /* not a market this build understands: nothing to note */
  }
}

/** The wrapper VERSION last seen for `slab`, or null when it has not been read yet. */
export function marketVersionOf(slab: string): number | null {
  return notes.get(slab)?.version ?? null;
}

/** The generation (`market_id`) last read for the asset, or null when unknown. */
export function assetGenerationOf(slab: string, assetIndex: number): bigint | null {
  return notes.get(slab)?.generations.get(assetIndex) ?? null;
}

/** Test hook. */
export function resetMarketLayoutNotes(): void {
  notes.clear();
}
