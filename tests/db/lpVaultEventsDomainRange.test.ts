import { readFileSync, readdirSync } from "node:fs";
import { describe, expect, it } from "vitest";

// lp_vault_events.domain stores the u16 `domain` the decoder reads from DepositToLpVault (tag 75) and
// ExecuteRedemption (tag 77) data (tests/lpVault/decoder.test.ts pins domain 0xffff decoding exactly). A domain
// is 2*asset (+1 for the odd side), so any market asset >= 16384 gives a domain >= 32768. A SMALLINT column (max 32767) rejects that row with 22003, SupabaseLpVaultStore.upsertEvents
// throws, and LpVaultIndexer re-reads and fails the same transaction every poll (the vault's cursor never moves).
const migDir = new URL("../../supabase/migrations/", import.meta.url);
const sqls = readdirSync(migDir)
  .filter((f) => f.endsWith(".sql"))
  .sort()
  .map((f) => readFileSync(new URL(f, migDir), "utf8").replace(/--[^\n]*/g, ""));

/** Last declared type of <table>.<col>: CREATE TABLE body, ADD COLUMN, then any ALTER COLUMN ... TYPE. */
function columnType(table: string, col: string): string {
  let t: string | null = null;
  for (const sql of sqls) {
    const ct = new RegExp(`CREATE TABLE (?:IF NOT EXISTS )?(?:public\\.)?${table}\\s*\\(([\\s\\S]*?)\\n\\);`, "i").exec(sql);
    if (ct) {
      const m = new RegExp(`^\\s*${col}\\s+(\\w+)`, "im").exec(ct[1]);
      if (m) t = m[1].toLowerCase();
    }
    const add = new RegExp(`ALTER TABLE (?:IF EXISTS )?(?:public\\.)?${table}\\s[^;]*?ADD COLUMN(?: IF NOT EXISTS)?\\s+${col}\\s+(\\w+)`, "i").exec(sql);
    if (add) t = add[1].toLowerCase();
    for (const stmt of sql.matchAll(new RegExp(`ALTER TABLE (?:IF EXISTS )?(?:ONLY )?(?:public\\.)?${table}\\s[^;]*`, "gi"))) {
      for (const m of stmt[0].matchAll(new RegExp(`ALTER COLUMN\\s+${col}\\s+(?:SET DATA )?TYPE\\s+(\\w+)`, "gi"))) t = m[1].toLowerCase();
    }
  }
  if (!t) throw new Error(`${table}.${col} not found`);
  return t;
}

const U16_MAX = 65535;
const MAX_BY_TYPE: Record<string, number> = { smallint: 32767, int2: 32767, integer: 2 ** 31 - 1, int: 2 ** 31 - 1, int4: 2 ** 31 - 1, bigint: Number.MAX_SAFE_INTEGER, int8: Number.MAX_SAFE_INTEGER };

describe("lp_vault_events.domain holds every u16 the LP-vault decoder emits", () => {
  it("column range >= u16 max (same as trades.asset_index / v22_events.asset_index)", () => {
    const t = columnType("lp_vault_events", "domain");
    expect(MAX_BY_TYPE).toHaveProperty(t);
    expect(MAX_BY_TYPE[t]).toBeGreaterThanOrEqual(U16_MAX);
  });
});
