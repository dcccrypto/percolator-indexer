import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const upsert = vi.hoisted(() => vi.fn());
const logError = vi.hoisted(() => vi.fn());
const appendFileSync = vi.hoisted(() => vi.fn());
vi.mock("node:fs", async (orig) => ({ ...(await orig<typeof import("node:fs")>()), appendFileSync }));
vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: logError, debug: vi.fn() }),
  getNetwork: () => "devnet",
  captureException: vi.fn(),
  getSupabase: () => ({ from: () => ({ upsert }) }),
}));

import { recordSkippedSignatures, resetSkippedSignatureCount, resetSkippedSignatureDedupe } from "../src/lib/skippedSignatures.js";

describe("skippedSignatures: non-Error file failure", () => {
  beforeEach(() => {
    resetSkippedSignatureCount();
    resetSkippedSignatureDedupe();
    logError.mockClear();
    upsert.mockReset().mockResolvedValue({ error: { message: "db down" } });
    process.env.SKIPPED_SIGNATURES_FILE = "/tmp/never-written.jsonl";
  });
  afterEach(() => { delete process.env.SKIPPED_SIGNATURES_FILE; });

  it("stringifies a non-Error thrown by the file fallback and still logs the signatures", async () => {
    appendFileSync.mockImplementation(() => { throw "disk gone"; });
    await expect(recordSkippedSignatures([{ signature: "s1", source: "creator-lookup", slab: "sl", error: "x" }])).resolves.toBe(false);
    const call = logError.mock.calls.find((c) => String(c[0]).includes("could not persist"))!;
    expect(call[1]).toMatchObject({ error: "disk gone", signatures: ["s1"] });
  });
});
