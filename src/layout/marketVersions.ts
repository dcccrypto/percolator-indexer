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
/** A slab is a few KB of notes; the cap is a backstop; at the cap the least-recently-noted slab is evicted (counted) so new markets are still gated. */
export const MAX_MARKET_NOTES = 20_000;
let evicted = 0;

/** Notes dropped to make room at the cap (oldest first). A non-zero value means the cap is being hit. */
export function marketNotesEvicted(): number {
  return evicted;
}

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
    const prev = notes.get(slab);
    // A partial read (light slice, or a buffer cut inside a later slot) carries only some slots: merge over the
    // generations an earlier read of the SAME layout recorded. A different VERSION drops the old layout's slots.
    const merged = prev && prev.version === g.layout.version ? new Map([...prev.generations, ...generations]) : generations;
    if (prev) notes.delete(slab); // re-insert so Map order is least-recently-noted first
    else if (notes.size >= MAX_MARKET_NOTES) {
      const oldest = notes.keys().next().value;
      if (oldest !== undefined) notes.delete(oldest);
      evicted++;
    }
    notes.set(slab, { version: g.layout.version, generations: merged });
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
  evicted = 0;
}
