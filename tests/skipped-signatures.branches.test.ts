import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const upsert = vi.hoisted(() => vi.fn());
const logError = vi.hoisted(() => vi.fn());
const logWarn = vi.hoisted(() => vi.fn());
const captureException = vi.hoisted(() => vi.fn());
const getNetwork = vi.hoisted(() => vi.fn());
vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: logWarn, error: logError, debug: vi.fn() }),
  getNetwork,
  captureException,
  getSupabase: () => ({ from: (t: string) => ({ upsert: (...a: unknown[]) => upsert(t, ...a) }) }),
}));

import {
  assertSkippedSignatureSinkReady,
  getSkippedSignatureCount,
  recordSkippedSignatures,
  resetSkippedSignatureCount,
  resetSkippedSignatureDedupe,
} from "../src/lib/skippedSignatures.js";

const mk = (n: number, error = "boom") => ({ signature: `sig-${n}`, source: "creator-lookup" as const, slab: `slab-${n}`, error });

describe("skippedSignatures: error and fallback branches", () => {
  let dir: string;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "skipsig-"));
    delete process.env.SKIPPED_SIGNATURES_FILE;
    resetSkippedSignatureCount();
    resetSkippedSignatureDedupe();
    upsert.mockReset().mockResolvedValue({ error: null });
    logError.mockClear();
    logWarn.mockClear();
    captureException.mockReset();
    getNetwork.mockReset().mockReturnValue("devnet");
  });
  afterEach(() => {
    delete process.env.SKIPPED_SIGNATURES_FILE;
    rmSync(dir, { recursive: true, force: true });
  });

  it("a throwing Sentry capture does not stop the table write", async () => {
    captureException.mockImplementation(() => { throw new Error("sentry down"); });
    await expect(recordSkippedSignatures([mk(1)])).resolves.toBe(true);
    expect(upsert).toHaveBeenCalledTimes(1);
    expect(getSkippedSignatureCount()).toBe(1);
  });

  it("reports distinct slabs to Sentry once for several rows on one slab", async () => {
    await recordSkippedSignatures([mk(1), { ...mk(2), slab: "slab-1" }]);
    const extra = captureException.mock.calls[0]![1].extra;
    expect(extra.signatures).toEqual(["sig-1", "sig-2"]);
    expect(extra.slabs).toEqual(["slab-1"]);
  });

  it("an unset network writes network: null and still records", async () => {
    getNetwork.mockImplementation(() => { throw new Error("no network"); });
    await recordSkippedSignatures([mk(1)]);
    expect(upsert.mock.calls[0]![1][0].network).toBeNull();
  });

  it("truncates the error to 200 chars in the log and 500 in the table", async () => {
    await recordSkippedSignatures([mk(1, "e".repeat(900))]);
    const ctx = logError.mock.calls.find((c) => String(c[0]).startsWith("SKIPPED"))![1];
    expect(ctx.error).toHaveLength(200);
    expect(upsert.mock.calls[0]![1][0].error).toHaveLength(500);
  });

  it("a non-Error rejection is stringified in the no-file log", async () => {
    upsert.mockRejectedValue("plain string failure");
    await expect(recordSkippedSignatures([mk(1)])).resolves.toBe(false);
    const ctx = logError.mock.calls.find((c) => String(c[0]).includes("no SKIPPED_SIGNATURES_FILE"))![1];
    expect(ctx.error).toBe("plain string failure");
  });

  it("a whitespace-only SKIPPED_SIGNATURES_FILE counts as unset", async () => {
    process.env.SKIPPED_SIGNATURES_FILE = "   ";
    upsert.mockResolvedValue({ error: { message: "nope" } });
    await recordSkippedSignatures([mk(1)]);
    expect(logError.mock.calls.some((c) => String(c[0]).includes("no SKIPPED_SIGNATURES_FILE"))).toBe(true);
    expect(() => assertSkippedSignatureSinkReady({ SKIPPED_SIGNATURES_FILE: "  " })).not.toThrow();
  });

  it("the file fallback records network and a timestamp, and warns with a non-Error cause", async () => {
    const file = join(dir, "out.jsonl");
    process.env.SKIPPED_SIGNATURES_FILE = file;
    upsert.mockRejectedValue(42);
    await recordSkippedSignatures([mk(1), mk(2)]);
    const lines = readFileSync(file, "utf8").trim().split("\n").map((l) => JSON.parse(l));
    expect(lines).toHaveLength(2);
    expect(lines[0]).toMatchObject({ signature: "sig-1", network: "devnet" });
    expect(typeof lines[0].at).toBe("string");
    expect(logWarn.mock.calls[0]![1]).toMatchObject({ file, error: "42" });
  });

  it("when the file append also fails it logs both and never throws (Error and non-Error causes)", async () => {
    // a path inside a directory that does not exist: appendFileSync throws ENOENT
    process.env.SKIPPED_SIGNATURES_FILE = join(dir, "missing", "out.jsonl");
    upsert.mockResolvedValue({ error: { message: "no table" } });
    await expect(recordSkippedSignatures([mk(1)])).resolves.toBe(false);
    const call = logError.mock.calls.find((c) => String(c[0]).includes("could not persist"))!;
    expect(call[1].error).toMatch(/ENOENT/);
    expect(call[1].signatures).toEqual(["sig-1"]);
  });

  it("the startup check accepts an existing writable file and a new file in a writable dir", () => {
    expect(() => assertSkippedSignatureSinkReady({ SKIPPED_SIGNATURES_FILE: join(dir, "new.jsonl") })).not.toThrow();
    expect(() => assertSkippedSignatureSinkReady({ SKIPPED_SIGNATURES_FILE: dir })).not.toThrow();
  });

  it("assertSkippedSignatureSinkReady defaults to process.env", () => {
    process.env.SKIPPED_SIGNATURES_FILE = "/nonexistent-dir-xyz/skipped.jsonl";
    expect(() => assertSkippedSignatureSinkReady()).toThrow(/not writable/);
  });
});
