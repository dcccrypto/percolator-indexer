import { readFileSync, readdirSync } from "node:fs";
import { describe, expect, it } from "vitest";

// market_stats.volume_24h above 2^53 must not be stored rounded. PostgREST sends a NUMERIC
// function column as a bare JSON number (supabase-js parses it into a double), so the rollup
// must return volume_24h as text. Live proof (real PostgREST 16.6 + PG16, before/after) is in the PR.
const migDir = new URL("../../supabase/migrations/", import.meta.url);
const files = readdirSync(migDir).filter((f) => f.endsWith(".sql")).sort();
const strip = (sql: string) => sql.replace(/--[^\n]*/g, "");
const CREATE = /CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:public\.)?volume_24h_by_slab\s*\([^)]*\)\s*RETURNS\s+TABLE\s*\(([^)]*)\)/i;

/** The migration that defines volume_24h_by_slab last, and its RETURNS TABLE column list. */
function lastDefinition(): { file: string; sql: string; columns: string } {
  let found: { file: string; sql: string; columns: string } | null = null;
  for (const f of files) {
    const sql = strip(readFileSync(new URL(f, migDir), "utf8"));
    const m = CREATE.exec(sql);
    if (m) found = { file: f, sql, columns: m[1] };
  }
  if (!found) throw new Error("volume_24h_by_slab is not defined in supabase/migrations");
  return found;
}

describe("volume_24h_by_slab returns a volume that survives JSON", () => {
  it("the effective definition returns volume_24h as text (not numeric)", () => {
    const { file, columns } = lastDefinition();
    const type = /\bvolume_24h\s+(\w+)/i.exec(columns)?.[1]?.toLowerCase();
    expect(type, `${file} RETURNS TABLE(${columns.trim()})`).toBe("text");
  });

  it("the file that re-creates it restores the hardened grants (service_role only)", () => {
    const { file, sql } = lastDefinition();
    const flat = sql.replace(/\s+/g, " ");
    const create = flat.search(/CREATE (OR REPLACE )?FUNCTION (public\.)?volume_24h_by_slab/i);
    const revoke = flat.indexOf("REVOKE ALL ON FUNCTION public.volume_24h_by_slab(text) FROM PUBLIC, anon, authenticated;");
    const grant = flat.indexOf("GRANT EXECUTE ON FUNCTION public.volume_24h_by_slab(text) TO service_role;");
    expect(revoke, `${file} revokes PUBLIC/anon/authenticated`).toBeGreaterThan(create);
    expect(grant, `${file} grants service_role`).toBeGreaterThan(create);
  });
});
