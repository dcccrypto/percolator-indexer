import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { existsSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const upsert = vi.fn();
const logError = vi.hoisted(() => vi.fn());
vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: logError, debug: vi.fn() }),
  getNetwork: () => "mainnet",
  captureException: vi.fn(),
  getSupabase: () => ({ from: (t: string) => ({ upsert: (...a: unknown[]) => upsert(t, ...a) }) }),
}));

import { getSkippedSignatureCount, recordSkippedSignatures, resetSkippedSignatureCount } from "../src/lib/skippedSignatures.js";

const FULL = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW";
const FILE = join(tmpdir(), `skipped-unit-${process.pid}.jsonl`);
const row = { signature: FULL, source: "trade-indexer" as const, slab: "SLABfull111111111111111111111111111111111111", error: "Transaction version (2) is not supported" };

describe("(c) durable skip record", () => {
  beforeEach(() => { process.env.SKIPPED_SIGNATURES_FILE = FILE; rmSync(FILE, { force: true }); resetSkippedSignatureCount(); upsert.mockReset(); logError.mockClear(); });
  afterEach(() => { delete process.env.SKIPPED_SIGNATURES_FILE; rmSync(FILE, { force: true }); });

  it("upserts the FULL signature and slab into skipped_signatures and bumps the counter", async () => {
    upsert.mockResolvedValue({ error: null });
    await recordSkippedSignatures([row]);
    expect(getSkippedSignatureCount()).toBe(1);
    expect(upsert).toHaveBeenCalledTimes(1);
    const [table, rows, opts] = upsert.mock.calls[0]!;
    expect(table).toBe("skipped_signatures");
    expect(rows[0]).toMatchObject({ signature: FULL, slab: row.slab, source: "trade-indexer", network: "mainnet" });
    expect(opts).toMatchObject({ onConflict: "signature,source" });
    expect(existsSync(FILE)).toBe(false);
  });
  it("falls back to the JSONL file (full signature + slab) when the table write fails, and never throws", async () => {
    upsert.mockResolvedValue({ error: { message: 'relation "skipped_signatures" does not exist' } });
    await expect(recordSkippedSignatures([row])).resolves.toBeUndefined();
    const line = JSON.parse(readFileSync(FILE, "utf8").trim());
    expect(line).toMatchObject({ signature: FULL, slab: row.slab });
    expect(getSkippedSignatureCount()).toBe(1);
    upsert.mockRejectedValue(new Error("db down"));
    await expect(recordSkippedSignatures([{ ...row, signature: FULL + "x" }])).resolves.toBeUndefined();
    expect(readFileSync(FILE, "utf8").trim().split("\n")).toHaveLength(2);
  });
  it("the error log line carries the FULL signature, the slab and the metric name (alertable, re-indexable)", async () => {
    upsert.mockResolvedValue({ error: null });
    await recordSkippedSignatures([row]);
    const ctx = logError.mock.calls.find((c) => String(c[0]).startsWith("SKIPPED"))![1];
    expect(ctx).toMatchObject({ signature: FULL, slab: row.slab, metric: "indexer_skipped_signatures_total" });
  });
  it("nothing to record is a no-op", async () => {
    await recordSkippedSignatures([]);
    expect(getSkippedSignatureCount()).toBe(0);
    expect(upsert).not.toHaveBeenCalled();
  });
  it("the migration file exists, is not applied by code, and defines the table", () => {
    const sql = readFileSync(join(__dirname, "..", "migrations", "20261005120000_skipped_signatures.sql"), "utf8");
    expect(sql).toMatch(/CREATE TABLE IF NOT EXISTS skipped_signatures/);
    expect(sql).toMatch(/UNIQUE \(signature, source\)/);
    expect(sql).toMatch(/NOT APPLIED/);
  });
});
