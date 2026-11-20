// DB fault replay L2 (J-20261120-IDX-REPORT-DBFAULTINJECTREPLAY; runs {timeout,kill,check,503}-skipped_signatures: the
// poison trade was lost with NO skipped_signatures row, 4 of 4): the signature was marked seen BEFORE the upsert, so after a
// failed write (no SKIPPED_SIGNATURES_FILE, the default) it was never written again. It is marked seen only once durable now,
// and recordSkippedSignatures says whether the record landed so the caller can hold its cursor.
import { describe, it, expect, vi, beforeEach } from "vitest";

const upsert = vi.fn();
const captureException = vi.fn();
vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
  getNetwork: () => "devnet",
  captureException: (...a: unknown[]) => captureException(...a),
  getSupabase: () => ({ from: (t: string) => ({ upsert: (...a: unknown[]) => upsert(t, ...a) }) }),
}));

import { recordSkippedSignatures, resetSkippedSignatureDedupe, getSkippedSignatureCount, resetSkippedSignatureCount } from "../src/lib/skippedSignatures.js";

const row = { signature: "2JwXepwQBJaqc29d8gP84auH9NEbd2kVxidiygB85ahNkWvAwgeaSqDqgSSsCR7aLAgxhtEHeANUJa6TgAqKTCVr", source: "trade-indexer" as const, slab: "A8XsQeovaevpHLgkWQx3Yg2Q3DuCfjiRvmtagmr53kew", error: "fill write failed 5+ polls (poison row)" };

describe("skipped_signatures: a record whose table write failed is not forgotten", () => {
  beforeEach(() => { delete process.env.SKIPPED_SIGNATURES_FILE; resetSkippedSignatureDedupe(); resetSkippedSignatureCount(); upsert.mockReset(); captureException.mockReset(); });

  it("re-recording the same signature after a failed write (no file sink) upserts it again", async () => {
    // what real PostgREST 16.6 answered in the replay while the table was locked past statement_timeout
    upsert.mockResolvedValueOnce({ error: { code: "57014", message: "canceling statement due to statement timeout" } });
    expect(await recordSkippedSignatures([row])).toBe(false);
    upsert.mockResolvedValueOnce({ error: null });
    expect(await recordSkippedSignatures([row])).toBe(true);
    expect(upsert).toHaveBeenCalledTimes(2);
  });

  it("once durable it is deduped (no third upsert), and the alert/counter fired once across the retry", async () => {
    upsert.mockResolvedValueOnce({ error: { code: "PGRST001", message: "no connection to the server" } });
    await recordSkippedSignatures([row]);
    upsert.mockResolvedValue({ error: null });
    await recordSkippedSignatures([row]);
    expect(await recordSkippedSignatures([row])).toBe(true);
    expect(upsert).toHaveBeenCalledTimes(2);
    expect(getSkippedSignatureCount()).toBe(1);
    expect(captureException).toHaveBeenCalledTimes(1);
  });

  it("a throwing client counts as not durable", async () => {
    upsert.mockRejectedValueOnce(new Error("fetch failed"));
    expect(await recordSkippedSignatures([row])).toBe(false);
  });
});
