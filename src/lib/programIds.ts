import { PublicKey } from "@solana/web3.js";
import { config } from "@percolatorct/shared";

/**
 * Normalise a raw program-id list (e.g. `config.allProgramIds`, which @percolatorct/shared builds with
 * `split(",").filter(Boolean)` — no trim, no validation). Trims each entry, drops empties and duplicates, and
 * requires every remaining entry to be a base58 32-byte public key. A bad entry throws an error that names it,
 * so a typo fails fast instead of silently dropping every fill for that program.
 */
export function normalizeProgramIds(raw: readonly string[] | undefined | null): string[] {
  const out: string[] = [];
  for (const entry of raw ?? []) {
    const id = String(entry).trim();
    if (!id) continue;
    let ok = false;
    try {
      ok = new PublicKey(id).toBytes().length === 32;
    } catch {
      ok = false;
    }
    if (!ok) {
      throw new Error(`Invalid program id in ALL_PROGRAM_IDS: ${JSON.stringify(entry)} (expected base58 32-byte public key)`);
    }
    if (!out.includes(id)) out.push(id);
  }
  return out;
}

/** The validated, trimmed `config.allProgramIds`. Use this instead of reading `config.allProgramIds` directly. */
export function getProgramIds(): string[] {
  return normalizeProgramIds(config.allProgramIds);
}

/**
 * Parse MARKETS_FILTER (comma-separated slab addresses). Entries are trimmed; an invalid entry is skipped
 * and reported through `onBad` instead of throwing, so one typo does not stop discovery of the others.
 */
export function parseMarketsFilter(raw: string | undefined, onBad?: (entry: string, err: string) => void): PublicKey[] {
  const out: PublicKey[] = [];
  for (const part of (raw ?? "").split(",")) {
    const a = part.trim();
    if (!a) continue;
    try {
      out.push(new PublicKey(a));
    } catch (e) {
      onBad?.(a, e instanceof Error ? e.message : String(e));
    }
  }
  return out;
}
