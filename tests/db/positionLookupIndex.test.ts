import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const strip = (s: string) => s.split("\n").filter((l) => !l.trim().startsWith("--")).join("\n");
const read = (p: string) => readFileSync(new URL(p, import.meta.url), "utf8");
// The CONCURRENTLY build is run by hand; supabase/migrations holds the transaction-safe stand-in.
const manual = strip(read("../../db/manual/20261114130000_trades_position_lookup_index.sql"));
const migration = strip(read("../../supabase/migrations/20261114130000_trades_position_lookup_index.sql"));
const src = read("../../src/db/traderNetPosition.ts");
const KEY = /idx_trades_position_lookup\s+ON (?:public\.)?trades \(trader, slab_address, asset_index, network, id\)/;

describe("trades position lookup index", () => {
  it("db/manual build is concurrent, re-runnable, and matches the filters then ORDER BY id", () => {
    expect(manual).toMatch(/CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_position_lookup\s+ON trades \(trader, slab_address, asset_index, network, id\)/);
  });
  it("supabase migration is transaction-safe: no CONCURRENTLY, same key, built only on an empty trades table", () => {
    expect(migration.replace(/'(?:[^']|'')*'/g, "''")).not.toMatch(/CONCURRENTLY/i);
    expect(migration).toMatch(new RegExp(`CREATE INDEX IF NOT EXISTS ${KEY.source}`));
    expect(migration).toMatch(/ELSIF EXISTS \(SELECT 1 FROM public\.trades LIMIT 1\) THEN\s+RAISE WARNING/);
    // skipped when the superseding cover index (20261115120000) is already there
    expect(migration).toMatch(/to_regclass\('public\.idx_trades_position_lookup_cover'\) IS NOT NULL/);
  });
  it("matches the query in fetchTraderPositionEvidence", () => {
    for (const col of ["trader", "slab_address", "asset_index", "network"]) expect(src).toContain(`.eq("${col}"`);
    expect(src).toContain('.order("id", { ascending: true })');
  });
});
