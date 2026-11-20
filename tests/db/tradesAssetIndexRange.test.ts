import { readFileSync, readdirSync } from "node:fs";
import { describe, expect, it } from "vitest";

const migDir = new URL("../../supabase/migrations/", import.meta.url);
const sqls = readdirSync(migDir)
  .filter((f) => f.endsWith(".sql"))
  .sort()
  .map((f) => readFileSync(new URL(f, migDir), "utf8").replace(/--[^\n]*/g, ""));

/** Last declared type of <table>.<col>: CREATE TABLE body, then any ALTER COLUMN ... TYPE. */
function columnType(table: string, col: string): string {
  let t: string | null = null;
  for (const sql of sqls) {
    const ct = new RegExp(`CREATE TABLE (?:IF NOT EXISTS )?(?:public\\.)?${table}\\s*\\(([\\s\\S]*?)\\n\\);`, "i").exec(sql);
    if (ct) {
      const m = new RegExp(`^\\s*${col}\\s+(\\w+)`, "im").exec(ct[1]);
      if (m) t = m[1].toLowerCase();
    }
    const add = new RegExp(`ALTER TABLE (?:IF EXISTS )?(?:public\\.)?${table}[\\s\\S]*?ADD COLUMN(?: IF NOT EXISTS)?\\s+${col}\\s+(\\w+)`, "i").exec(sql);
    if (add) t = add[1].toLowerCase();
    for (const m of sql.matchAll(new RegExp(`ALTER COLUMN\\s+${col}\\s+(?:SET DATA )?TYPE\\s+(\\w+)`, "gi"))) t = m[1].toLowerCase();
  }
  if (!t) throw new Error(`${table}.${col} not found`);
  return t;
}

const U16_MAX = 65535;
const MAX_BY_TYPE: Record<string, number> = { smallint: 32767, int2: 32767, integer: 2 ** 31 - 1, int: 2 ** 31 - 1, int4: 2 ** 31 - 1, bigint: Number.MAX_SAFE_INTEGER, int8: Number.MAX_SAFE_INTEGER };

describe("trades.asset_index holds every u16 the trade writer is typed to send", () => {
  it("column range >= u16 max (same as v22_events.asset_index)", () => {
    const t = columnType("trades", "asset_index");
    expect({ type: t, max: MAX_BY_TYPE[t] ?? 0 }).toEqual({ type: t, max: expect.any(Number) });
    expect(MAX_BY_TYPE[t]).toBeGreaterThanOrEqual(U16_MAX);
  });
});
