import { describe, expect, it } from "vitest";
import {
  MAX_REQUESTED_TX_VERSION,
  breakerAlertPolls,
  createBreakerTracker,
  fetchParsedTxsTolerant,
  isPoisonTxError,
  isTransientRpcError,
} from "../../src/lib/tolerantTxFetch.js";

describe("lead 1: real -32015 texts are not classed transient", () => {
  const texts = [
    'Transaction version (2) is not supported by the requesting client. Please try the request again with the following configuration parameter: "maxSupportedTransactionVersion": 1',
    'failed to get transaction: Transaction version (2) is not supported by the requesting client. Please try the request again with the following configuration parameter: "maxSupportedTransactionVersion": 1',
    'failed to get transactions: Transaction version (3) is not supported by the requesting client. Please try the request again with the following configuration parameter: "maxSupportedTransactionVersion": 1',
  ];
  for (const t of texts) {
    it(t.slice(0, 60), () => {
      const err = Object.assign(new Error(t), { code: -32015 });
      expect(isTransientRpcError(err)).toBe(false);
      expect(isPoisonTxError(err)).toBe(true);
      expect(isPoisonTxError(new Error(t))).toBe(true);
    });
  }
});

describe("lead 2: breakerAlertPolls is decimal-only", () => {
  for (const v of ["0x10", "2e0", " 5 ", "5.0", "+5", "", "0", "-3", "abc"]) {
    it(`rejects ${JSON.stringify(v)} -> default 10`, () => {
      expect(breakerAlertPolls({ INDEXER_BREAKER_ALERT_POLLS: v })).toBe(10);
    });
  }
  it("accepts plain decimal", () => {
    expect(breakerAlertPolls({ INDEXER_BREAKER_ALERT_POLLS: "3" })).toBe(3);
    expect(breakerAlertPolls({})).toBe(10);
  });
});

describe("lead 3: createBreakerTracker with k < 1", () => {
  for (const k of [0, -2, NaN, 0.5]) {
    it(`k=${k} still alerts (treated as 1)`, () => {
      const fired: number[] = [];
      const t = createBreakerTracker(k, (_key, n) => fired.push(n));
      t.held("s");
      t.held("s");
      expect(fired).toEqual([1, 2]);
    });
  }
  it("k=3 unchanged", () => {
    const fired: number[] = [];
    const t = createBreakerTracker(3, (_k, n) => fired.push(n));
    for (let i = 0; i < 6; i++) t.held("s");
    expect(fired).toEqual([3, 6]);
  });
});

describe("lead 4: version option", () => {
  it("passes MAX_REQUESTED_TX_VERSION", async () => {
    let seen: unknown;
    await fetchParsedTxsTolerant(
      { getParsedTransactions: async (_s, o) => ((seen = o), []), getParsedTransaction: async () => null },
      [],
      (fn) => fn(),
    );
    expect(seen).toEqual({ maxSupportedTransactionVersion: MAX_REQUESTED_TX_VERSION });
  });
});
