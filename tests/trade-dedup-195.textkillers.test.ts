// GH#195 pins that tests/trade-dedup-195.test.ts misses: the exact index and backfill key, the backfill
// order/threshold, tradeKey case-sensitivity and byte-for-byte signature storage. K1-K3 read SQL like
// trade-dedup-195.test.ts; K4-K6 drive the real insertTradeRow module with the same Supabase mock as
// tests/db/insertTradeRow.paths.test.ts. Fast fallback for trade-dedup-195.realpg.test.ts (needs no Postgres).
import { beforeEach, describe, expect, it, vi } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

const h = vi.hoisted(() => ({ from: vi.fn(), insert: vi.fn(), upsert: vi.fn(), select: vi.fn() }));
vi.mock("@percolatorct/shared", () => ({
  getSupabase: () => ({ from: h.from }),
  getNetwork: () => "devnet",
  createLogger: () => ({ warn: vi.fn(), error: vi.fn(), info: vi.fn(), debug: vi.fn() }),
}));
import { insertTradeRow, insertTradeRows, tradeKey, type IndexerTradeRow } from "../src/db/insertTradeRow.js";

const KEY = ["tx_signature", "asset_index", "leg_index"];
const MIG = join(__dirname, "..", "supabase", "migrations");
const lastBuilder = () =>
  readdirSync(MIG).filter((f) => f.endsWith(".sql")).sort()
    .map((f) => readFileSync(join(MIG, f), "utf8"))
    .filter((s) => /CREATE\s+UNIQUE\s+INDEX[\s\S]*uq_trades_sig_asset_leg/i.test(s)).pop()!
    .split("\n").filter((l) => !l.trimStart().startsWith("--")).join("\n");
const cols = (s: string) => s.split(",").map((c) => c.trim());

describe("K1-K3: migration SQL pins the exact key", () => {
  it("K1 the unique index is on exactly (tx_signature, asset_index, leg_index), bare columns", () => {
    const m = lastBuilder().match(/CREATE\s+UNIQUE\s+INDEX\s+uq_trades_sig_asset_leg\s+ON\s+trades\s*\(([^)]*)\)/i);
    expect(m).toBeTruthy();
    expect(cols(m![1])).toEqual(KEY); // index column list: drop a column, add one, lower()
  });
  it("K2 the backfill partitions by exactly the index key, no extra column", () => {
    const m = lastBuilder().match(/PARTITION\s+BY\s+([\s\S]*?)\s+ORDER\s+BY/i);
    expect(cols(m![1])).toEqual(KEY); // backfill key: drop or add a column
  });
  it("K3 ORDER BY id ascending and rn > 1 exactly", () => {
    const sql = lastBuilder();
    expect(sql).toMatch(/ORDER\s+BY\s+id\s*(ASC\s*)?\)/i); // keeps the lowest id, not the highest
    expect(sql).toMatch(/rn\s*>\s*1\s*\)/i); // deletes every copy after the first
  });
});

const row = (o: Partial<IndexerTradeRow> = {}): IndexerTradeRow => ({
  slab_address: "SlabAddress1111", trader: "Trader", side: "long", size: "10", price: 1.5, fee: 0.01,
  tx_signature: "5AbCdEfGh", asset_index: 0, leg_index: 0, ...o,
});
beforeEach(() => {
  vi.resetAllMocks();
  h.insert.mockResolvedValue({ error: null });
  h.select.mockResolvedValue({ data: [], error: null });
  h.upsert.mockImplementation(() => ({ select: h.select }));
  h.from.mockImplementation(() => ({ insert: h.insert, upsert: h.upsert }));
});

describe("K4-K6: writer", () => {
  it("K4 tradeKey is case-sensitive (base58 signatures differing only in case are different fills)", () => {
    const k = (s: string) => tradeKey({ tx_signature: s, asset_index: 0, leg_index: 0 });
    expect(k("5AbC")).not.toBe(k("5abc")); // no case-folding in the in-process key
  });
  it("K5 the signature is written byte-for-byte (single and batch)", async () => {
    await insertTradeRow(row());
    await insertTradeRows([row({ leg_index: 1 })]);
    expect(h.insert.mock.calls[0][0].tx_signature).toBe("5AbCdEfGh");
    expect(h.upsert.mock.calls[0][0][0].tx_signature).toBe("5AbCdEfGh"); // no case-folding in the stored value
  });
  it.each(["57014", "23505", "40001", "08006", undefined])("K6 insertTradeRows throws on ANY error (code %s), never returns []", async (code) => {
    h.select.mockResolvedValue({ data: null, error: { code, message: "boom" } });
    await expect(insertTradeRows([row()])).rejects.toThrow(/insertTradeRows failed/); // a failed batch is never reported as zero new rows
  });
});
