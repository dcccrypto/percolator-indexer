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

/** slab -> newest fully processed signature. Empty on any failure. */
export async function loadTradeCursors(): Promise<Map<string, string>> {
  const out = new Map<string, string>();
  const net = network();
  if (!net) return out;
  try {
    const { data, error } = await getSupabase().from("trade_indexer_cursor").select("slab_address,signature").eq("network", net).limit(10_000);
    if (error) throw new Error(error.message);
    for (const r of (data ?? []) as { slab_address: string; signature: string }[]) out.set(r.slab_address, r.signature);
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
