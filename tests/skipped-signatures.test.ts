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

import { assertSkippedSignatureSinkReady, getSkippedSignatureCount, recordSkippedSignatures, resetSkippedSignatureCount, resetSkippedSignatureDedupe } from "../src/lib/skippedSignatures.js";

const FULL = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW";
const FILE = join(tmpdir(), `skipped-unit-${process.pid}.jsonl`);
const row = { signature: FULL, source: "trade-indexer" as const, slab: "SLABfull111111111111111111111111111111111111", error: "Transaction version (2) is not supported" };

describe("(c) durable skip record", () => {
  beforeEach(() => { process.env.SKIPPED_SIGNATURES_FILE = FILE; rmSync(FILE, { force: true }); resetSkippedSignatureCount(); resetSkippedSignatureDedupe(); upsert.mockReset(); logError.mockClear(); });
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
  it("B: there is NO default file; without SKIPPED_SIGNATURES_FILE a failed table write only logs (the log + Sentry are the record)", async () => {
    delete process.env.SKIPPED_SIGNATURES_FILE;
    upsert.mockResolvedValue({ error: { message: "no table" } });
    const unique = `${FULL}-nodefault-${process.pid}-${Date.now()}`;
    await expect(recordSkippedSignatures([{ ...row, signature: unique }])).resolves.toBeUndefined();
    expect(existsSync("/tmp/indexer-skipped-signatures.jsonl") && readFileSync("/tmp/indexer-skipped-signatures.jsonl", "utf8").includes(unique)).toBe(false);
    const msgs = logError.mock.calls.map((c) => String(c[0]));
    expect(msgs.some((m) => m.includes("no SKIPPED_SIGNATURES_FILE is set"))).toBe(true);
    const last = logError.mock.calls.find((c) => String(c[0]).includes("no SKIPPED_SIGNATURES_FILE"))![1];
    expect(last.signatures).toEqual([unique]);
  });
  it("B: an explicitly configured file is checked writable at startup and fails loudly otherwise; unset is fine", () => {
    expect(() => assertSkippedSignatureSinkReady({})).not.toThrow();
    expect(() => assertSkippedSignatureSinkReady({ SKIPPED_SIGNATURES_FILE: FILE })).not.toThrow();
    expect(() => assertSkippedSignatureSinkReady({ SKIPPED_SIGNATURES_FILE: "/nonexistent-dir-xyz/skipped.jsonl" })).toThrow(/not writable/);
  });
  it("D: the same (source, signature) is recorded once per process (no repeated log, Sentry or table writes); a different source is separate", async () => {
    upsert.mockResolvedValue({ error: null });
    await recordSkippedSignatures([row]);
    await recordSkippedSignatures([row, row]);
    expect(getSkippedSignatureCount()).toBe(1);
    expect(upsert).toHaveBeenCalledTimes(1);
    expect(logError.mock.calls.filter((c) => String(c[0]).startsWith("SKIPPED"))).toHaveLength(1);
    await recordSkippedSignatures([{ ...row, source: "lp-vault" }]);
    expect(getSkippedSignatureCount()).toBe(2);
  });
  it("nothing to record is a no-op", async () => {
    await recordSkippedSignatures([]);
    expect(getSkippedSignatureCount()).toBe(0);
    expect(upsert).not.toHaveBeenCalled();
  });
  it("the migration file exists, is not applied by code, and defines the table", () => {
    const sql = readFileSync(join(__dirname, "..", "supabase", "migrations", "20261005120000_skipped_signatures.sql"), "utf8");
    expect(sql).toMatch(/CREATE TABLE IF NOT EXISTS skipped_signatures/);
    expect(sql).toMatch(/UNIQUE \(signature, source\)/);
    expect(sql).toMatch(/NOT APPLIED/);
    // the table must not be reachable by client roles
    expect(sql).toMatch(/ALTER TABLE skipped_signatures ENABLE ROW LEVEL SECURITY/);
    expect(sql).toMatch(/REVOKE ALL ON TABLE skipped_signatures FROM anon, authenticated/);
    expect(sql).not.toMatch(/CREATE POLICY/);
    expect(sql).not.toMatch(/GRANT /);
  });
});
