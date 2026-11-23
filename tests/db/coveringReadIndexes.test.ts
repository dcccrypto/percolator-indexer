import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const read = (p: string) => readFileSync(new URL(p, import.meta.url), "utf8");
const strip = (s: string) => s.split("\n").filter((l) => !l.trim().startsWith("--")).join("\n");
// The CONCURRENTLY build is run by hand; supabase/migrations holds the transaction-safe stand-in.
const MANUAL = "../../db/manual/20261115120000_trades_covering_read_indexes.sql";
const live = strip(read(MANUAL));
const migration = strip(read("../../supabase/migrations/20261115120000_trades_covering_read_indexes.sql"));

const VOLUME = /idx_trades_volume_24h\s+ON (?:public\.)?trades \(network, created_at\) INCLUDE \(slab_address, size\)\s+WHERE is_liquidation = false AND size IS NOT NULL;/;
const COVER = /idx_trades_position_lookup_cover\s+ON (?:public\.)?trades \(trader, slab_address, asset_index, network, id\)\s+INCLUDE \(side, size, is_liquidation, created_at, tx_signature\);/;

describe("covering read indexes (db/manual, run by hand)", () => {
  it("is concurrent", () => {
    expect(live.match(/CREATE INDEX CONCURRENTLY/g)).toHaveLength(2);
  });

  it("drops the superseded idx_trades_position_lookup as an active statement after the cover CREATE", () => {
    const drop = /DROP INDEX CONCURRENTLY IF EXISTS idx_trades_position_lookup;/;
    expect(live).toMatch(drop);
    expect(live.match(/DROP INDEX/g)).toHaveLength(1);
    expect(live.search(drop)).toBeGreaterThan(live.indexOf("idx_trades_position_lookup_cover"));
    expect(live.search(drop)).toBeGreaterThan(live.indexOf("idx_trades_volume_24h"));
    // the cover index must not be the one dropped (name is a prefix of the cover's)
    expect(live).not.toMatch(/DROP INDEX[^;]*idx_trades_position_lookup_cover/);
  });

  it("documents ON_ERROR_STOP and the rollback in the header", () => {
    const raw = read(MANUAL);
    expect(raw).toMatch(/psql -v ON_ERROR_STOP=1/);
    expect(raw).toMatch(/NOT APPLIED/);
    expect(raw).toMatch(/re-create idx_trades_position_lookup from db\/manual\/20261114130000/);
  });

  it("volume index carries the rollup function's own filter and columns", () => {
    const fn = read("../../supabase/migrations/20260726180000_volume_rollup_fn.sql");
    expect(fn).toMatch(/t\.is_liquidation = false/);
    expect(fn).toMatch(/t\.size\s+IS NOT NULL/);
    expect(live).toMatch(VOLUME);
  });

  it("position index keeps the lookup key and includes every column the reader selects", () => {
    const src = read("../../src/db/traderNetPosition.ts");
    expect(src).toContain('.select("side, size, is_liquidation, created_at, tx_signature")');
    expect(live).toMatch(COVER);
  });
});

describe("covering read indexes (supabase/migrations stand-in)", () => {
  it("is transaction-safe: no CONCURRENTLY", () => {
    expect(migration.replace(/'(?:[^']|'')*'/g, "''")).not.toMatch(/CONCURRENTLY/i);
  });

  it("builds the same two indexes as the db/manual file, then drops the superseded one, only on an empty trades table", () => {
    const empty = migration.indexOf("ELSE");
    expect(migration).toMatch(/IF EXISTS \(SELECT 1 FROM public\.trades LIMIT 1\) THEN/);
    expect(migration.search(new RegExp(`CREATE INDEX IF NOT EXISTS ${VOLUME.source}`))).toBeGreaterThan(empty);
    expect(migration.search(new RegExp(`CREATE INDEX IF NOT EXISTS ${COVER.source}`))).toBeGreaterThan(empty);
    const drop = migration.search(/DROP INDEX IF EXISTS public\.idx_trades_position_lookup;/);
    expect(drop).toBeGreaterThan(migration.indexOf("idx_trades_position_lookup_cover"));
    expect(migration.match(/DROP INDEX/g)).toHaveLength(1);
  });

  it("on a populated trades table it only warns (no build, no drop before the branch)", () => {
    const populated = migration.slice(0, migration.indexOf("ELSE"));
    expect(populated).toMatch(/RAISE WARNING/);
    expect(populated).not.toMatch(/CREATE INDEX|DROP INDEX/);
  });
});
