import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const sql = readFileSync(new URL("../../supabase/migrations/20261114130000_trades_position_lookup_index.sql", import.meta.url), "utf8");
const live = sql.split("\n").filter((l) => !l.trim().startsWith("--")).join("\n");
const src = readFileSync(new URL("../../src/db/traderNetPosition.ts", import.meta.url), "utf8");

describe("trades position lookup index", () => {
  it("is concurrent, and matches the filters then ORDER BY id", () => {
    expect(live).toMatch(/CREATE INDEX CONCURRENTLY idx_trades_position_lookup\s+ON trades \(trader, slab_address, asset_index, network, id\)/);
  });
  it("matches the query in fetchTraderPositionEvidence", () => {
    for (const col of ["trader", "slab_address", "asset_index", "network"]) expect(src).toContain(`.eq("${col}"`);
    expect(src).toContain('.order("id", { ascending: true })');
  });
});
