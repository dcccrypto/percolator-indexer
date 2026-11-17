import { createLogger, getSupabase, getNetwork } from "@percolatorct/shared";

const logger = createLogger("indexer:trade-cursor");

/**
 * Durable per-market cursor of the trade poller (supabase/migrations/20261114120000_trade_indexer_cursor.sql, applied by
 * hand). Best effort on purpose and never throws: without the table the poller is back to its in-memory cursor, which
 * costs a re-fetch after a restart but must never stop indexing.
 */
const warned = new Set<string>();
function unavailable(op: "load" | "save", message: string): void {
  if (warned.has(op)) return;
  warned.add(op);
  logger.error("trade_indexer_cursor unavailable: using the in-memory cursor (apply supabase/migrations/20261114120000_trade_indexer_cursor.sql)", { op, error: message });
}

/** Test hook. */
export function resetTradeCursorWarnings(): void {
  warned.clear();
}

function network(): string | null {
  try {
    return getNetwork();
  } catch {
    return null; // unset in some test/dev setups: no durable cursor
  }
}

/** Rows per read. Must be <= the project's PostgREST max_rows (1000 in supabase/config.toml), which caps a response whatever .limit asks for. */
export const TRADE_CURSOR_PAGE = 1000;

/**
 * slab -> newest fully processed signature, paged with .range over a stable order (as traderNetPosition.ts does).
 * Never throws: on a failed page it returns the cursors read so far. A missing cursor only makes that slab re-scan its
 * newest window (RPC cost; trade writes are deduped by the unique leg index), so a partial map is safe.
 */
export async function loadTradeCursors(): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  const net = network();
  if (!net) return out;
  try {
    for (let from = 0; ; from += TRADE_CURSOR_PAGE) {
      const { data, error } = await getSupabase()
        .from("trade_indexer_cursor")
        .select("slab_address,signature")
        .eq("network", net)
        .order("slab_address", { ascending: true })
        .range(from, from + TRADE_CURSOR_PAGE - 1);
      if (error) throw new Error(error.message);
      const rows = (data ?? []) as { slab_address: string; signature: string }[];
      for (const r of rows) out.set(r.slab_address, r.signature);
      if (rows.length < TRADE_CURSOR_PAGE) break;
    }
  } catch (err) {
    unavailable("load", err instanceof Error ? err.message : String(err));
  }
  return out;
}

export async function saveTradeCursor(slab: string, signature: string): Promise<void> {
  const net = network();
  if (!net) return;
  try {
    const { error } = await getSupabase()
      .from("trade_indexer_cursor")
      .upsert({ network: net, slab_address: slab, signature, updated_at: new Date().toISOString() }, { onConflict: "network,slab_address" });
    if (error) throw new Error(error.message);
  } catch (err) {
    unavailable("save", err instanceof Error ? err.message : String(err));
  }
}
