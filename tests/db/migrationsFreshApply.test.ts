import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

// Static guards for "a fresh database builds from supabase/migrations and a second apply is a
// no-op" (J-20261119-IDX-FIX-MIGRATIONSFRESHAPPLY). The scratch-Postgres run is the real proof.
const MIGRATIONS = join(__dirname, "..", "..", "supabase", "migrations");
const files = readdirSync(MIGRATIONS).filter((f) => f.endsWith(".sql")).sort();
const live = (f: string) =>
  readFileSync(join(MIGRATIONS, f), "utf8").split("\n").filter((l) => !l.trim().startsWith("--")).join("\n");
/** Drop single-quoted literals (e.g. RAISE WARNING texts that mention CONCURRENTLY). */
const noStrings = (sql: string) => sql.replace(/'(?:[^']|'')*'/g, "''");

describe("supabase/migrations fresh apply", () => {
  it("no file runs CONCURRENTLY (the Supabase CLI applies each file in one transaction)", () => {
    for (const f of files) expect(noStrings(live(f)), f).not.toMatch(/\bCONCURRENTLY\b/i);
  });

  it("markets.keeper_status is created (IF NOT EXISTS) before the markets_with_stats rebuild and before its CHECK", () => {
    const adds = files.filter((f) => /ADD COLUMN\s+(?:IF NOT EXISTS\s+)?keeper_status\b/i.test(live(f)));
    expect(adds).toEqual(["20260726191500_markets_keeper_status_column.sql"]);
    expect(live(adds[0])).toMatch(/ALTER TABLE markets ADD COLUMN IF NOT EXISTS keeper_status TEXT NOT NULL DEFAULT 'retired';/);
    // 20260726192000 recreates markets_with_stats from m.*; the column must exist by then, or the
    // baseline's CREATE OR REPLACE VIEW fails on a second apply ("cannot change name of view column").
    const view = files.indexOf("20260726192000_view_metadata_source.sql");
    const check = files.indexOf("20261001020000_markets_keeper_status_pending.sql");
    expect(live(files[view])).toMatch(/DROP VIEW IF EXISTS markets_with_stats;\s+CREATE VIEW markets_with_stats AS\s+SELECT\s+m\.\*/);
    expect(live(files[check])).toMatch(/ADD CONSTRAINT markets_keeper_status_check/);
    expect(files.indexOf(adds[0])).toBeLessThan(view);
    expect(files.indexOf(adds[0])).toBeLessThan(check);
  });

  it("20261012090000 adds size_provenance and its CHECK idempotently", () => {
    const sql = live("20261012090000_trade_size_provenance.sql");
    expect(sql).toMatch(/ADD COLUMN IF NOT EXISTS size_provenance TEXT;/);
    expect(sql).toMatch(/IF NOT EXISTS \(\s*SELECT 1 FROM pg_constraint\s+WHERE conrelid = 'public\.trades'::regclass AND conname = 'trades_size_provenance_check'\s*\) THEN\s+ALTER TABLE public\.trades\s+ADD CONSTRAINT trades_size_provenance_check/);
  });
});
