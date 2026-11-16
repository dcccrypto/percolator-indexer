import { describe, expect, it } from "vitest";
import { MAX_SKIPS_ABSOLUTE, fetchParsedTxsTolerant, skipsAcceptable, type TxFetcher } from "../../src/lib/tolerantTxFetch.js";

// Pins the per-signature fan-out of fetchParsedTxsTolerant (the batch path threw a -32015 version error) and the
// caller-facing contract for the three leads of J-20261116-IDX-BUG-TOLERANTFANOUT.
const retry = <R>(fn: () => Promise<R>): Promise<R> => fn();
const V2 = Object.assign(new Error("Transaction version (2) is not supported by the requesting client"), { code: -32015 });
const RATE = new Error("429 Too Many Requests");

type Single = (sig: string) => Promise<{ sig: string } | null>;
function conn(single: Single): TxFetcher<{ sig: string }> & { singles: string[] } {
  const singles: string[] = [];
  return {
    singles,
    async getParsedTransactions() { throw V2; },
    async getParsedTransaction(sig) { singles.push(sig); return single(sig); },
  };
}

describe("fetchParsedTxsTolerant fan-out", () => {
  it("lead 1: a transient error mid fan-out holds the cursor (failed) and stops fetching; earlier poison is still listed in skipped", async () => {
    const c = conn(async (s) => { if (s === "a") throw V2; if (s === "c") throw RATE; return { sig: s }; });
    const r = await fetchParsedTxsTolerant(c, ["a", "b", "c", "d"], retry);
    expect(r.failed).toBe(true);
    expect(r.error).toBe(RATE);
    expect(r.txs).toEqual([null, { sig: "b" }, null, null]); // fetched siblings are kept; the caller breaks on failed and never reads them
    expect(c.singles).toEqual(["a", "b", "c"]); // d never fetched
    // The caller (TradeIndexer) checks `failed` before moving the cursor, so these are only recorded, never skipped past.
    expect(r.skipped.map((x) => x.signature)).toEqual(["a"]);
  });

  it("a non-version, non-transient error mid fan-out also holds (unknown error is never a poison pill)", async () => {
    const boom = new Error("bad params");
    const r = await fetchParsedTxsTolerant(conn(async (s) => { if (s === "b") throw boom; return { sig: s }; }), ["a", "b"], retry);
    expect(r).toMatchObject({ failed: true, error: boom });
  });

  it("lead 2: every per-signature fetch returns null (no throw): not failed, nothing skipped; skipsAcceptable(0,0) is true", async () => {
    const r = await fetchParsedTxsTolerant(conn(async () => null), ["a", "b"], retry);
    expect(r).toMatchObject({ failed: false, skipped: [], txs: [null, null] });
    expect(skipsAcceptable(0, 0)).toBe(true);
    // Same outcome the plain batch path gives for not-found txs; TradeIndexer marks pass.incomplete for every null tx.
  });

  it("lead 3: the batch path passes through whatever the RPC returned, including a short array", async () => {
    const short: TxFetcher<{ sig: string }> = {
      async getParsedTransactions() { return [{ sig: "a" }]; },
      async getParsedTransaction() { return null; },
    };
    const r = await fetchParsedTxsTolerant(short, ["a", "b"], retry);
    expect(r.failed).toBe(false);
    expect(r.txs).toHaveLength(1); // callers index txs[j] for j < batch.length and treat undefined as missing
  });

  it("one poison with a good sibling is skipped; exactly MAX_SKIPS_ABSOLUTE skips pass, one more is refused", async () => {
    const sigs = Array.from({ length: MAX_SKIPS_ABSOLUTE + 2 }, (_, i) => `s${i}`);
    const ok = await fetchParsedTxsTolerant(conn(async (s) => { if (s !== "s0" && s !== "s6") throw V2; return { sig: s }; }), sigs, retry);
    expect(ok.failed).toBe(false);
    expect(ok.skipped).toHaveLength(MAX_SKIPS_ABSOLUTE);
    const bad = await fetchParsedTxsTolerant(conn(async (s) => { if (s !== "s0") throw V2; return { sig: s }; }), sigs, retry);
    expect(bad).toMatchObject({ failed: true, skipped: [], massSkip: { skipped: MAX_SKIPS_ABSOLUTE + 1, total: MAX_SKIPS_ABSOLUTE + 2 } });
  });
});
