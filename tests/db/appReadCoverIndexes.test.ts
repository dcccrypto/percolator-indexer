import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

// Static guards for the three app-read covering indexes (J-20261120-IDX-PERF-COVERINGINDEXES).
// The scratch-Postgres run in bench/app-read-covers/README.md is the real proof.
const read = (p: string) => readFileSync(new URL(p, import.meta.url), "utf8");
const strip = (s: string) => s.split("\n").filter((l) => !l.trim().startsWith("--")).join("\n");
const MANUAL = "../../db/manual/20261120120000_trades_app_read_cover_indexes.sql";
const STAND_IN = "../../supabase/migrations/20261120120000_trades_app_read_cover_indexes.sql";
const live = strip(read(MANUAL));
const migration = strip(read(STAND_IN));

const DEFS = {
  idx_trades_net_created_cover: /idx_trades_net_created_cover\s+ON (?:public\.)?trades \(network, created_at\) INCLUDE \(trader, slab_address, size, price, is_liquidation\);/,
  idx_trades_slab_net_created_cover: /idx_trades_slab_net_created_cover\s+ON (?:public\.)?trades \(network, slab_address, created_at DESC\) INCLUDE \(price, size, id\);/,
  idx_trades_trader_net_created_cover: /idx_trades_trader_net_created_cover\s+ON (?:public\.)?trades \(trader, network, created_at DESC\) INCLUDE \(slab_address, side, size, price, fee\);/,
};

describe("app read cover indexes (db/manual, run by hand)", () => {
  it("builds exactly the three indexes CONCURRENTLY IF NOT EXISTS and drops nothing", () => {
    expect(live.match(/CREATE INDEX/g)).toHaveLength(3);
    for (const re of Object.values(DEFS)) expect(live).toMatch(new RegExp(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${re.source}`));
    expect(live).not.toMatch(/DROP INDEX/);
  });

  it("documents ON_ERROR_STOP, NOT APPLIED and the rollback in the header", () => {
    const raw = read(MANUAL);
    expect(raw).toMatch(/psql -v ON_ERROR_STOP=1 -f db\/manual\/20261120120000_trades_app_read_cover_indexes\.sql/);
    expect(raw).toMatch(/NOT APPLIED/);
    for (const name of Object.keys(DEFS)) expect(raw).toMatch(new RegExp(`Rollback:[\\s\\S]*${name}`));
  });
});

describe("app read cover indexes (supabase/migrations stand-in)", () => {
  it("is transaction-safe: no CONCURRENTLY outside string literals", () => {
    expect(migration.replace(/'(?:[^']|'')*'/g, "''")).not.toMatch(/CONCURRENTLY/i);
  });

  it("builds the same three indexes as the db/manual file, only on an empty trades table", () => {
    const empty = migration.indexOf("ELSE");
    expect(empty).toBeGreaterThan(0);
    expect(migration).toMatch(/IF EXISTS \(SELECT 1 FROM public\.trades LIMIT 1\) THEN/);
    expect(migration.match(/CREATE INDEX/g)).toHaveLength(3);
    for (const re of Object.values(DEFS)) {
      expect(migration.search(new RegExp(`CREATE INDEX IF NOT EXISTS ${re.source}`))).toBeGreaterThan(empty);
    }
    expect(migration).not.toMatch(/DROP INDEX/);
  });

  it("on a populated trades table it only warns, and checks every one of the three names", () => {
    const populated = migration.slice(0, migration.indexOf("ELSE"));
    expect(populated).toMatch(/RAISE WARNING/);
    expect(populated).not.toMatch(/CREATE INDEX|DROP INDEX/);
    for (const name of Object.keys(DEFS)) expect(populated).toContain(`to_regclass('public.${name}') IS NULL`);
  });
});
