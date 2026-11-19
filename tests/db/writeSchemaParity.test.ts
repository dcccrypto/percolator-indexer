import { readFileSync, readdirSync } from "node:fs";
import { describe, expect, it } from "vitest";

/**
 * Differential test: what the indexer WRITES vs what the hand-applied migrations DEFINE.
 * Parses the SQL text only (no database). A drift here means every write fails at runtime
 * while the mocked-Supabase unit tests keep passing.
 */
const root = new URL("../../", import.meta.url);
const read = (p: string) => readFileSync(new URL(p, root), "utf8");
const stripComments = (s: string) => s.replace(/--[^\n]*/g, "");

const migDir = "supabase/migrations/";
const allMigrations = readdirSync(new URL(migDir, root))
  .filter((f) => f.endsWith(".sql"))
  .sort()
  .map((f) => ({ file: f, sql: stripComments(read(migDir + f)) }));

/** Body of CREATE TABLE <name> ( ... ); split at top-level commas. */
function createTableItems(sql: string, table: string): string[] | null {
  const m = new RegExp(`CREATE TABLE IF NOT EXISTS ${table}\\s*\\(`, "i").exec(sql);
  if (!m) return null;
  let depth = 1;
  let i = m.index + m[0].length;
  const start = i;
  for (; i < sql.length && depth > 0; i++) {
    if (sql[i] === "(") depth++;
    else if (sql[i] === ")") depth--;
  }
  const body = sql.slice(start, i - 1);
  const items: string[] = [];
  let d = 0;
  let cur = "";
  for (const ch of body) {
    if (ch === "(") d++;
    if (ch === ")") d--;
    if (ch === "," && d === 0) {
      items.push(cur.trim());
      cur = "";
    } else cur += ch;
  }
  if (cur.trim()) items.push(cur.trim());
  return items;
}

interface Schema {
  columns: Set<string>;
  /** each entry: sorted column list of a UNIQUE constraint / unique index */
  uniques: string[][];
  items: string[];
}

function tableSchema(table: string): Schema {
  const columns = new Set<string>();
  const uniques: string[][] = [];
  const items: string[] = [];
  for (const { sql } of allMigrations) {
    const created = createTableItems(sql, table);
    if (created) {
      for (const it of created) {
        items.push(it);
        const uq = /^UNIQUE\s*\(([^)]*)\)/i.exec(it);
        if (uq) uniques.push(uq[1].split(",").map((c) => c.trim()));
        else if (!/^(CONSTRAINT|PRIMARY|CHECK|FOREIGN)\b/i.test(it)) {
          const name = it.split(/\s+/)[0];
          columns.add(name);
          if (/\bUNIQUE\b/i.test(it) || /\bPRIMARY KEY\b/i.test(it)) uniques.push([name]);
        }
      }
    }
    const t = `(?:public\\.)?${table}`;
    for (const m of sql.matchAll(new RegExp(`ALTER TABLE (?:IF EXISTS )?${t}\\s+ADD COLUMN(?: IF NOT EXISTS)?\\s+(\\w+)`, "gi"))) columns.add(m[1]);
    // partial unique indexes (WHERE ...) cannot be PostgREST on_conflict targets, so they are skipped
    for (const m of sql.matchAll(new RegExp(`CREATE UNIQUE INDEX[^;]*?ON ${t}\\s*(?:USING \\w+\\s*)?\\(([^)]*)\\)(\\s*WHERE)?`, "gi")))
      if (!m[2]) uniques.push(m[1].split(",").map((c) => c.trim()));
    for (const m of sql.matchAll(new RegExp(`ALTER TABLE (?:IF EXISTS )?${t}\\s+ADD CONSTRAINT\\s+\\w+\\s+UNIQUE\\s*\\(([^)]*)\\)`, "gi")))
      uniques.push(m[1].split(",").map((c) => c.trim()));
  }
  return { columns, uniques, items };
}

/** Allowed values of the LAST migration that defines CHECK (col IN (...)). */
function lastCheck(col: string): string[] {
  let found: string[] | null = null;
  for (const { sql } of allMigrations)
    for (const m of sql.matchAll(new RegExp(`CHECK\\s*\\(\\s*${col}\\s+IN\\s*\\(([^)]*)\\)\\s*\\)`, "gi")))
      found = m[1].split(",").map((x) => x.trim().replace(/'/g, ""));
  if (!found) throw new Error(`no CHECK on ${col}`);
  return found;
}

const sameCols =(a: string[], b: string[]) => [...a].sort().join() === [...b].sort().join();

/** Keys of the object literal passed to a call, e.g. `.insert({ ...row, a: 1, b })`. */
function literalKeys(src: string, anchor: RegExp): string[] {
  const m = anchor.exec(src);
  if (!m) throw new Error(`anchor not found: ${anchor}`);
  let i = src.indexOf("{", m.index + m[0].length - 1);
  let depth = 0;
  const start = i;
  for (; i < src.length; i++) {
    if (src[i] === "{") depth++;
    else if (src[i] === "}" && --depth === 0) break;
  }
  const body = src.slice(start + 1, i);
  const keys: string[] = [];
  let d = 0;
  let cur = "";
  const parts: string[] = [];
  for (const ch of body) {
    if ("{([".includes(ch)) d++;
    if ("})]".includes(ch)) d--;
    if (ch === "," && d === 0) {
      parts.push(cur);
      cur = "";
    } else cur += ch;
  }
  parts.push(cur);
  for (const p of parts) {
    const t = p.replace(/\/\/[^\n]*/g, "").trim();
    if (!t || t.startsWith("...")) continue;
    keys.push(/^(\w+)/.exec(t)![1]);
  }
  return keys;
}

describe("skipped_signatures write parity (src/lib/skippedSignatures.ts vs 20261005120000)", () => {
  const src = read("src/lib/skippedSignatures.ts");
  const schema = tableSchema("skipped_signatures");

  it("every written key is a column", () => {
    // `.upsert(rows.map((r) => ({ ... })), {`
    const keys = literalKeys(src, /\.upsert\(rows\.map\(\(r\) => \(\{/);
    expect(keys.sort()).toEqual(["error", "network", "signature", "slab", "source"]);
    for (const k of keys) expect(schema.columns, `column ${k}`).toContain(k);
  });

  it("NOT NULL columns without default are all written (signature, source)", () => {
    const required = schema.items
      .filter((i) => /NOT NULL/i.test(i) && !/DEFAULT|PRIMARY KEY/i.test(i))
      .map((i) => i.split(/\s+/)[0])
      .sort();
    expect(required).toEqual(["signature", "source"]);
  });

  it("onConflict 'signature,source' matches a UNIQUE constraint", () => {
    const m = /onConflict:\s*"([^"]+)"/.exec(src);
    expect(m).not.toBeNull();
    const cols = m![1].split(",").map((c) => c.trim());
    expect(schema.uniques.some((u) => sameCols(u, cols))).toBe(true);
  });

  it("uses ignoreDuplicates", () => {
    expect(src).toMatch(/ignoreDuplicates:\s*true/);
  });
});

describe("markets write parity (src/db/insertMarketRow.ts vs markets migrations)", () => {
  const src = read("src/db/insertMarketRow.ts");
  const schema = tableSchema("markets");

  const iface = /export interface IndexerMarketRow \{([\s\S]*?)\n\}/.exec(src)![1];
  const rowKeys = [...iface.matchAll(/^\s{2}(\w+)\??:/gm)].map((m) => m[1]);
  const extraKeys = literalKeys(src, /\.from\("markets"\)\.insert\(\{/);

  it("parsed the expected write keys", () => {
    expect(rowKeys).toContain("slab_address");
    expect(rowKeys).toContain("logo_url");
    expect(extraKeys.sort()).toEqual(["keeper_status", "metadata_source", "network"]);
  });

  it("every IndexerMarketRow key and extra key is a column of markets", () => {
    const missing = [...rowKeys, ...extraKeys].filter((k) => !schema.columns.has(k));
    // keeper_status is only ever referenced by 20261001020000's CHECK; no migration in this
    // repo creates the column (it pre-exists on the live DB). Tracked explicitly below so this
    // test fails loudly if that changes in either direction.
    expect(missing).toEqual(["keeper_status"]);
  });

  it("keeper_status column is only constrained (not created) in repo migrations — known gap", () => {
    const creators = allMigrations.filter((m) => /ADD COLUMN(?: IF NOT EXISTS)?\s+keeper_status/i.test(m.sql));
    expect(creators).toHaveLength(0);
    const chk = allMigrations.find((m) => m.file.startsWith("20261001020000"))!;
    expect(chk.sql).toMatch(/ALTER TABLE markets ADD CONSTRAINT markets_keeper_status_check/);
  });

  it("keeper_status 'pending' satisfies the latest keeper_status CHECK and is what is written", () => {
    expect(lastCheck("keeper_status")).toContain("pending");
    expect(src).toMatch(/AUTO_ROW_KEEPER_STATUS = "pending"/);
    expect(src).toMatch(/keeper_status:\s*AUTO_ROW_KEEPER_STATUS/);
  });

  it("metadata_source 'auto' satisfies the latest CHECK and is what is written", () => {
    expect(lastCheck("metadata_source")).toContain("auto");
    expect(src).toMatch(/metadata_source:\s*"auto"/);
  });

  it("network CHECK allows the two networks", () => {
    expect(lastCheck("network")).toEqual(["devnet", "main" + "net"]);
  });

  it("no migration drops/renames columns or drops constraints/tables of the written tables", () => {
    for (const { file, sql } of allMigrations)
      expect(sql, file).not.toMatch(
        /DROP COLUMN|RENAME COLUMN|DROP TABLE(?: IF EXISTS)?\s+(?:public\.)?(?:markets|skipped_signatures)\b|ALTER TABLE[^;]*(?:markets|skipped_signatures)[^;]*DROP CONSTRAINT(?! IF EXISTS markets_keeper_status_check)/i,
      );
  });

  it("unique key: slab_address alone — does NOT cover network (reported, pinned)", () => {
    // MISMATCH REPORT: the only unique key on markets is slab_address. network is a plain
    // column (NOT NULL DEFAULT 'devnet'), so the same slab cannot exist on devnet and mainnet
    // in one database. Fine while each network has its own DB; a 23505 (swallowed by
    // insertMarketRow) would silently drop a cross-network duplicate if they ever share one.
    expect(schema.uniques).toEqual([["id"], ["slab_address"]]);
    expect(schema.uniques.some((u) => u.includes("network"))).toBe(false);
  });
});
