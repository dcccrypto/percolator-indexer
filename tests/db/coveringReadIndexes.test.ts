import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const read = (p: string) => readFileSync(new URL(p, import.meta.url), "utf8");
const live = read("../../supabase/migrations/20261115120000_trades_covering_read_indexes.sql")
  .split("\n").filter((l) => !l.trim().startsWith("--")).join("\n");

describe("covering read indexes migration", () => {
  it("is concurrent", () => {
    expect(live.match(/CREATE INDEX CONCURRENTLY/g)).toHaveLength(2);
    expect(live).not.toMatch(/DROP INDEX/); // the old-index drop is a manual, commented step
  });

  it("volume index carries the rollup function's own filter and columns", () => {
    const fn = read("../../supabase/migrations/20260726180000_volume_rollup_fn.sql");
    expect(fn).toMatch(/t\.is_liquidation = false/);
    expect(fn).toMatch(/t\.size\s+IS NOT NULL/);
    expect(live).toMatch(
      /idx_trades_volume_24h\s+ON trades \(network, created_at\) INCLUDE \(slab_address, size\)\s+WHERE is_liquidation = false AND size IS NOT NULL;/,
    );
  });

  it("position index keeps the lookup key and includes every column the reader selects", () => {
    const src = read("../../src/db/traderNetPosition.ts");
    expect(src).toContain('.select("side, size, is_liquidation, created_at, tx_signature")');
    expect(live).toMatch(
      /idx_trades_position_lookup_cover\s+ON trades \(trader, slab_address, asset_index, network, id\)\s+INCLUDE \(side, size, is_liquidation, created_at, tx_signature\);/,
    );
  });
});
