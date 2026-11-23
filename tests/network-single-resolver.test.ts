import { describe, it, expect, afterEach, vi } from "vitest";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join } from "node:path";

/**
 * NET-SPLIT: one cluster tag per process.
 *
 * src/network.ts CURRENT_NETWORK tags the LP-vault rows (src/index.ts LpVaultIndexer) and the webhook IS_MAINNET.
 * Until this fix the trades, markets, v22_events, trade cursor and skipped_signatures sites called
 * @percolatorct/shared getNetwork(), which is `raw === "mainnet" ? "mainnet" : "devnet"` (no case fold, no trim,
 * no testnet). Shared's own boot validator (validateNetworkConfig) lower-cases, trims and accepts every value below,
 * so production booted with NETWORK=testnet and wrote trades/markets/v22_events under "devnet" while LP-vault rows
 * got "testnet". Every row family must carry the same cluster the boot validator resolved.
 *
 * The fake Supabase records every .eq("network", x) filter and every network field in an insert/upsert payload.
 * Shared's real getNetwork stays in place, so a src site that still calls it shows up as a "devnet" tag.
 */
const PROGRAM_ID = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD"; // the example id from shared's own error text

type Seen = { table: string; how: string; network: unknown };

function recordingSupabase(seen: Seen[]) {
  const builder = (table: string): unknown => {
    const result = { data: [], error: null, count: 0 };
    const b: Record<string, unknown> = {};
    const chain = (name: string) => (...args: unknown[]) => {
      if (name === "eq" && args[0] === "network") seen.push({ table, how: "eq", network: args[1] });
      if (name === "upsert" || name === "insert") {
        const rows = Array.isArray(args[0]) ? args[0] : [args[0]];
        for (const r of rows as Record<string, unknown>[]) if ("network" in r) seen.push({ table, how: name, network: r.network });
      }
      return b;
    };
    for (const m of ["select", "insert", "upsert", "update", "delete", "eq", "neq", "in", "is", "not", "gt", "gte", "lt", "lte", "order", "range", "limit", "single", "maybeSingle"]) b[m] = chain(m);
    b.then = (ok: (v: unknown) => unknown, bad: (e: unknown) => unknown) => Promise.resolve(result).then(ok, bad);
    return b;
  };
  return { from: (t: string) => builder(t), rpc: (_fn: string, args: Record<string, unknown>) => { seen.push({ table: "rpc", how: "rpc", network: args?.p_network }); return Promise.resolve({ data: [], error: null }); } };
}

describe("NET-SPLIT: every row family is tagged with the cluster the indexer runs on", () => {
  afterEach(() => {
    vi.doUnmock("@percolatorct/shared");
    vi.unstubAllEnvs();
    vi.resetModules();
  });

  it.each(["devnet", "testnet", "Testnet", "MAINNET", " mainnet", "mainnet\n", "DEVNET"])(
    "NETWORK=%j: trades, markets, v22_events, cursor and skipped rows use CURRENT_NETWORK",
    async (value) => {
      vi.resetModules();
      vi.stubEnv("NETWORK", value);
      const { validateNetworkConfig } = await import("@percolatorct/shared/dist/networkValidation.js" as string);
      // Precondition: a production boot accepts this value (no throw) and resolves it to cfg.network.
      const cfg = validateNetworkConfig({ NETWORK: value, PROGRAM_ID, RPC_URL: "http://127.0.0.1:1", FORCE_MAINNET: "1" });

      const seen: Seen[] = [];
      const fake = recordingSupabase(seen);
      vi.doMock("@percolatorct/shared", async (importOriginal) => ({
        ...(await importOriginal<Record<string, unknown>>()),
        getSupabase: () => fake,
        captureException: () => undefined,
        createLogger: () => ({ info() {}, warn() {}, error() {}, debug() {} }),
      }));

      const { CURRENT_NETWORK } = await import("../src/network.js");
      expect(CURRENT_NETWORK).toBe(cfg.network); // LP-vault rows and webhook IS_MAINNET use this

      const { insertTradeRows } = await import("../src/db/insertTradeRow.js");
      const { fetchStoredLegs } = await import("../src/db/storedLegs.js");
      const { insertV22Events } = await import("../src/db/insertV22Events.js");
      const { getMarketRows } = await import("../src/db/marketRows.js");
      const { saveTradeCursor, loadTradeCursors } = await import("../src/db/tradeCursor.js");
      const { recordSkippedSignatures } = await import("../src/lib/skippedSignatures.js");

      const slab = "SLAB1111111111111111111111111111111111111111";
      await insertTradeRows([{ slab_address: slab, trader: "T", side: "long", size: "1", price: 1, fee: 0, tx_signature: "sig-1", asset_index: 0, leg_index: 0 }]);
      await fetchStoredLegs("sig-1");
      await insertV22Events([{ signature: "sig-1", ix_index: 0, inner_index: -1, kind: "deposit", slab_address: slab, asset_index: 0, actor: null, subject: null, amount: null } as never]);
      await getMarketRows();
      await saveTradeCursor(slab, "sig-1");
      await loadTradeCursors();
      await recordSkippedSignatures([{ signature: `sig-skip-${JSON.stringify(value)}`, source: "trade-indexer", slab, error: "x" }]);

      const tables = new Set(seen.map((s) => s.table));
      for (const t of ["trades", "v22_events", "markets", "trade_indexer_cursor", "skipped_signatures"]) expect(tables).toContain(t);
      // Before the fix: "testnet", "Testnet", "MAINNET", " mainnet", "mainnet\n" tagged every one of these "devnet".
      expect(seen.filter((s) => s.network !== CURRENT_NETWORK)).toEqual([]);
    },
  );

  it("no file under src imports shared's getNetwork (the only resolver is src/network.ts)", () => {
    const root = join(__dirname, "..", "src");
    const importsShared = /import\s*\{[^}]*\bgetNetwork\b[^}]*\}\s*from\s*["']@percolatorct\/shared["']/;
    const offenders: string[] = [];
    const walk = (dir: string) => {
      for (const name of readdirSync(dir)) {
        const p = join(dir, name);
        if (statSync(p).isDirectory()) walk(p);
        else if (p.endsWith(".ts") && importsShared.test(readFileSync(p, "utf8"))) offenders.push(p.slice(root.length + 1));
      }
    };
    walk(root);
    expect(offenders).toEqual([]);
  });
});
