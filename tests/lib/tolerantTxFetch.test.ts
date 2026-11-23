import { describe, expect, it } from "vitest";
import { MAX_SKIPS_ABSOLUTE, fetchParsedTxsTolerant, skipsAcceptable, type TxFetcher } from "../../src/lib/tolerantTxFetch.js";

// Pins the per-signature fan-out of fetchParsedTxsTolerant (the batch path threw a -32015 version error) and the
// caller-facing contract for the three leads of J-20261116-IDX-BUG-TOLERANTFANOUT.
const retry = <R>(fn: () => Promise<R>): Promise<R> => fn();
const V2 = Object.assign(new Error("Transaction version (2) is not supported by the requesting client"), { code: -32015 });
const RATE = new Error("429 Too Many Requests");
type Tx = { transaction: { signatures: string[] } };
const tx = (s: string): Tx => ({ transaction: { signatures: [s] } });

type Single = (sig: string) => Promise<Tx | null>;
type Batch = () => Promise<(Tx | null)[]>;
// The batch call throws the -32015 version error by default, so the per-signature fan-out runs.
function conn(single: Single, batch: Batch = async () => { throw V2; }): TxFetcher<Tx> & { singles: string[] } {
  const singles: string[] = [];
  return {
    singles,
    getParsedTransactions: batch,
    async getParsedTransaction(sig) { singles.push(sig); return single(sig); },
  };
}

describe("fetchParsedTxsTolerant fan-out", () => {
  it("lead 1: a transient error mid fan-out holds the cursor (failed) and stops fetching; earlier poison is still listed in skipped", async () => {
    const c = conn(async (s) => { if (s === "a") throw V2; if (s === "c") throw RATE; return tx(s); });
    const r = await fetchParsedTxsTolerant(c, ["a", "b", "c", "d"], retry);
    expect(r.failed).toBe(true);
    expect(r.error).toBe(RATE);
    expect(r.txs).toEqual([null, tx("b"), null, null]); // fetched siblings are kept; the caller breaks on failed and never reads them
    expect(c.singles).toEqual(["a", "b", "c"]); // d never fetched
    // The caller (TradeIndexer) checks `failed` before moving the cursor, so these are only recorded, never skipped past.
    expect(r.skipped.map((x) => x.signature)).toEqual(["a"]);
  });

  it("a non-version, non-transient error mid fan-out also holds (unknown error is never a poison pill)", async () => {
    const boom = new Error("bad params");
    const r = await fetchParsedTxsTolerant(conn(async (s) => { if (s === "b") throw boom; return tx(s); }), ["a", "b"], retry);
    expect(r).toMatchObject({ failed: true, error: boom });
  });

  it("lead 2: every per-signature fetch returns null (no throw): not failed, nothing skipped; skipsAcceptable(0,0) is true", async () => {
    const r = await fetchParsedTxsTolerant(conn(async () => null), ["a", "b"], retry);
    expect(r).toMatchObject({ failed: false, skipped: [], txs: [null, null] });
    expect(skipsAcceptable(0, 0)).toBe(true);
    // Same outcome the plain batch path gives for not-found txs; TradeIndexer marks pass.incomplete for every null tx.
  });

  it("lead 3: a short batch answer cannot be paired with the requested signatures: held (failed), never passed through", async () => {
    const short: TxFetcher<Tx> = {
      async getParsedTransactions() { return [tx("a")]; },
      async getParsedTransaction() { return null; },
    };
    const r = await fetchParsedTxsTolerant(short, ["a", "b"], retry);
    expect(r).toMatchObject({ failed: true, skipped: [], txs: [null, null] }); // BATCH-ORDER-1: pairing is by signature, a length mismatch is a failed read
  });

  it("up to MAX_SKIPS_ABSOLUTE skips pass with good siblings, one more is refused", async () => {
    const sigs = Array.from({ length: MAX_SKIPS_ABSOLUTE + 2 }, (_, i) => `s${i}`);
    const ok = await fetchParsedTxsTolerant(conn(async (s) => { if (s !== "s0" && s !== "s6") throw V2; return tx(s); }), sigs, retry);
    expect(ok.failed).toBe(false);
    expect(ok.skipped).toHaveLength(MAX_SKIPS_ABSOLUTE);
    const bad = await fetchParsedTxsTolerant(conn(async (s) => { if (s !== "s0") throw V2; return tx(s); }), sigs, retry);
    expect(bad).toMatchObject({ failed: true, skipped: [], massSkip: { skipped: MAX_SKIPS_ABSOLUTE + 1, total: MAX_SKIPS_ABSOLUTE + 2 } });
  });
});

// Mutation killers from J-20261123-IDX-REPORT-TOLERANTFANOUTTESTMUT: each test names the mutant(s) of
// src/lib/tolerantTxFetch.ts it kills that the tests above miss (transient veto, sibling rule, literal limit, pairing).
describe("fetchParsedTxsTolerant fan-out: mutation killers", () => {
  it("M06: a per-signature 502 gateway page that also carries -32015 / 'version (2)' holds the cursor, it is not skipped", async () => {
    const gw = Object.assign(new Error("502 Bad Gateway: Transaction version (2) is not supported"), { code: -32015 });
    const r = await fetchParsedTxsTolerant(conn(async (s) => { if (s === "b") throw gw; return tx(s); }), ["a", "b", "c"], retry);
    expect(r).toMatchObject({ failed: true, error: gw, skipped: [] });
  });

  it("M07: a batch error that is both -32015 and a 429 holds at once and never fans out", async () => {
    const rl = Object.assign(new Error("429 Too Many Requests"), { code: -32015 });
    const c = conn(async (s) => tx(s), async () => { throw rl; });
    const r = await fetchParsedTxsTolerant(c, ["a", "b"], retry);
    expect(r).toMatchObject({ failed: true, error: rl, skipped: [], txs: [null, null] });
    expect(c.singles).toEqual([]);
  });

  it("M11: the per-batch skip limit is exactly 5 (literal), 6 unreadable with a good sibling is refused", async () => {
    expect(MAX_SKIPS_ABSOLUTE).toBe(5);
    const sigs = ["g", "p1", "p2", "p3", "p4", "p5", "p6"];
    const r = await fetchParsedTxsTolerant(conn(async (s) => { if (s !== "g") throw V2; return tx(s); }), sigs, retry);
    expect(r).toMatchObject({ failed: true, skipped: [], massSkip: { skipped: 6, total: 7 } });
  });

  it("M12/M14/M16: an all-unreadable batch (1 or 3 signatures, under the limit) is refused: no sibling proves the reader works", async () => {
    for (const sigs of [["p1"], ["p1", "p2", "p3"]]) {
      const r = await fetchParsedTxsTolerant(conn(async () => { throw V2; }), sigs, retry);
      expect(r).toMatchObject({ failed: true, skipped: [], txs: sigs.map(() => null), massSkip: { skipped: sigs.length, total: sigs.length } });
    }
    expect(skipsAcceptable(1, 0)).toBe(false);
    expect(skipsAcceptable(MAX_SKIPS_ABSOLUTE, 0)).toBe(false);
    expect(skipsAcceptable(1, 1)).toBe(true);
  });

  it("M13/M15: a not-found (null) sibling is not a successful read; one unreadable + nulls is refused", async () => {
    const r = await fetchParsedTxsTolerant(conn(async (s) => { if (s === "p") throw V2; return null; }), ["p", "n1", "n2"], retry);
    expect(r).toMatchObject({ failed: true, skipped: [], massSkip: { skipped: 1, total: 3 } });
  });

  it("M21: a per-signature answer carrying ANOTHER transaction is held (TxPairingError), never filed under the asked signature", async () => {
    const r = await fetchParsedTxsTolerant(conn(async (s) => tx(s === "b" ? "z" : s)), ["a", "b", "c"], retry);
    expect(r.failed).toBe(true);
    expect((r.error as Error).name).toBe("TxPairingError");
    expect(r.skipped).toEqual([]);
  });

  it("M22: a shuffled batch answer (with a null) is re-paired by signature, so txs[i] is the tx of sigs[i]", async () => {
    const c = conn(async () => null, async () => [tx("c"), null, tx("a")]);
    const r = await fetchParsedTxsTolerant(c, ["a", "b", "c"], retry);
    expect(r).toMatchObject({ failed: false, skipped: [] });
    expect(r.txs).toEqual([tx("a"), null, tx("c")]);
  });

  it("M32: an all-null plain batch answer is not a failure (same as the fan-out all-null case)", async () => {
    const c = conn(async () => null, async () => [null, null]);
    const r = await fetchParsedTxsTolerant(c, ["a", "b"], retry);
    expect(r).toEqual({ txs: [null, null], skipped: [], failed: false });
    expect(c.singles).toEqual([]);
  });

  it("M26: a mass-skip refusal returns all-null txs (no good sibling leaks out of a held batch)", async () => {
    const sigs = ["g", "p1", "p2", "p3", "p4", "p5", "p6"];
    const r = await fetchParsedTxsTolerant(conn(async (s) => { if (s !== "g") throw V2; return tx(s); }), sigs, retry);
    expect(r.failed).toBe(true);
    expect(r.txs).toEqual(sigs.map(() => null));
  });
});
