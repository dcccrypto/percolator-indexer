import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({
  from: vi.fn(),
  insert: vi.fn(),
  upsert: vi.fn(),
  select: vi.fn(),
}));

vi.mock("@percolatorct/shared", () => ({
  getSupabase: () => ({ from: db.from }),
  getNetwork: () => "devnet",
  createLogger: () => ({ warn: vi.fn(), info: vi.fn(), error: vi.fn(), debug: vi.fn() }),
}));

import * as writer from "../../src/db/insertTradeRow.js";
import type { IndexerTradeRow } from "../../src/db/insertTradeRow.js";

const ROOT = join(__dirname, "..", "..");
const MIGRATIONS = join(ROOT, "supabase", "migrations");

/** Columns anon may SELECT on trades, per a SQL text (column grants only). */
function anonTradeColumns(sql: string): Set<string> {
  const cols = new Set<string>();
  const re = /GRANT\s+SELECT\s*\(([^)]*)\)\s*ON\s+(?:public\.)?trades\s+TO\s+([^;]+);/gi;
  for (const m of sql.matchAll(re)) {
    if (!/\banon\b/i.test(m[2])) continue;
    for (const c of m[1].split(",")) cols.add(c.trim().toLowerCase());
  }
  return cols;
}

describe("#228 follow-up: anon can read size_provenance (Codex P2)", () => {
  it("a migration grants anon SELECT on trades.size_provenance, ordered after the column is added", () => {
    const files = readdirSync(MIGRATIONS).filter((f) => f.endsWith(".sql")).sort();
    const addIdx = files.findIndex((f) => /ADD COLUMN\s+size_provenance/i.test(readFileSync(join(MIGRATIONS, f), "utf8")));
    const grantIdx = files.findIndex((f) => anonTradeColumns(readFileSync(join(MIGRATIONS, f), "utf8")).has("size_provenance"));
    expect(addIdx).toBeGreaterThanOrEqual(0);
    expect(grantIdx).toBeGreaterThan(addIdx);
  });

  it("the fresh baseline grants anon the same trades columns plus size_provenance", () => {
    const cols = anonTradeColumns(readFileSync(join(ROOT, "db", "v17_baseline.sql"), "utf8"));
    expect([...cols].sort()).toEqual(["created_at", "size", "size_provenance", "trader"]);
  });
});

function row(o: Partial<IndexerTradeRow> = {}): IndexerTradeRow {
  return {
    slab_address: "slab", trader: "t", side: "long", size: "1", price: 1, fee: 0,
    tx_signature: "sig", asset_index: 0, leg_index: 0, size_provenance: "matcher_exact", ...o,
  };
}

const MISSING = { code: "PGRST204", message: "Could not find the 'size_provenance' column of 'trades' in the schema cache" };

describe("#228 follow-up: an unapplied migration does not stop trade indexing", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    // resetProvenanceColumnProbe only exists with the follow-up; the tests must fail, not crash the file, without it.
    (writer as { resetProvenanceColumnProbe?: () => void }).resetProvenanceColumnProbe?.();
    db.upsert.mockImplementation(() => ({ select: db.select }));
    db.from.mockImplementation(() => ({ insert: db.insert, upsert: db.upsert }));
  });

  it("single insert: retries without the column and stays without it", async () => {
    db.insert.mockResolvedValueOnce({ error: MISSING }).mockResolvedValue({ error: null });
    await expect(writer.insertTradeRow(row())).resolves.toBe(true);
    expect(db.insert).toHaveBeenCalledTimes(2);
    expect(db.insert.mock.calls[1][0]).not.toHaveProperty("size_provenance");
    await writer.insertTradeRow(row({ leg_index: 1 }));
    expect(db.insert).toHaveBeenCalledTimes(3);
    expect(db.insert.mock.calls[2][0]).not.toHaveProperty("size_provenance");
  });

  it("batch insert: retries without the column", async () => {
    db.select
      .mockResolvedValueOnce({ data: null, error: MISSING })
      .mockResolvedValue({ data: [{ tx_signature: "sig", asset_index: 0, leg_index: 0 }], error: null });
    await expect(writer.insertTradeRows([row()])).resolves.toHaveLength(1);
    expect(db.upsert).toHaveBeenCalledTimes(2);
    expect(db.upsert.mock.calls[1][0][0]).not.toHaveProperty("size_provenance");
  });

  it("any other error still throws, with no retry", async () => {
    db.insert.mockResolvedValue({ error: { code: "PGRST204", message: "Could not find the 'bogus' column" } });
    await expect(writer.insertTradeRow(row())).rejects.toThrow(/insertTradeRow failed/);
    expect(db.insert).toHaveBeenCalledTimes(1);
  });
});
