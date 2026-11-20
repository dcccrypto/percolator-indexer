import { describe, it, expect } from "vitest";
import { RpcLimiter, isRateLimitError, paceConnection } from "../src/lib/rpcLimiter.js";

function fakeClock() {
  let t = 0;
  const slept: number[] = [];
  return { now: () => t, sleep: async (ms: number) => { slept.push(ms); t += ms; }, slept };
}

describe("isRateLimitError forms", () => {
  it("code and status, numeric only", () => {
    expect(isRateLimitError({ code: 429 })).toBe(true);
    expect(isRateLimitError({ status: 429 })).toBe(true);
    expect(isRateLimitError({ code: -32429 })).toBe(true);
    expect(isRateLimitError({ status: -32429 })).toBe(true);
    expect(isRateLimitError({ code: 500 })).toBe(false);
    // a string code is not a numeric code; falls to the message, which is "[object Object]"
    expect(isRateLimitError({ code: "429" })).toBe(false);
    // numeric code wins over status
    expect(isRateLimitError({ code: 500, status: 429 })).toBe(false);
  });

  it("message forms: Error, plain object, other values", () => {
    expect(isRateLimitError(new Error("429 : Too many"))).toBe(true);
    expect(isRateLimitError(new Error("  429"))).toBe(true);
    expect(isRateLimitError(new Error("Server responded with 429"))).toBe(true);
    expect(isRateLimitError({ message: "rate limit exceeded" })).toBe(true);
    expect(isRateLimitError({ message: "Rate-Limited" })).toBe(true);
    expect(isRateLimitError({ message: "ratelimit" })).toBe(true);
    expect(isRateLimitError({ message: 5 })).toBe(false);
    expect(isRateLimitError("Too Many Requests")).toBe(true);
    expect(isRateLimitError("fine")).toBe(false);
    expect(isRateLimitError(null)).toBe(false);
    expect(isRateLimitError(undefined)).toBe(false);
  });

  it("a 429 that is just a number stays false", () => {
    expect(isRateLimitError(new Error("Slot 429 was skipped"))).toBe(false);
    expect(isRateLimitError(new Error("block 1429 missing"))).toBe(false);
    expect(isRateLimitError(new Error("error -32429x"))).toBe(false);
  });
});

describe("RpcLimiter branches", () => {
  it("gives up after maxRetries, per-call override beats the default of 1, and reports each retry", async () => {
    const c = fakeClock();
    const seen: Array<{ attempt: number; backoffMs: number }> = [];
    const l = new RpcLimiter({ minIntervalMs: 0, baseBackoffMs: 100, now: c.now, sleep: c.sleep, onRateLimited: (i) => seen.push(i) });
    let calls = 0;
    await expect(l.run(async () => { calls++; throw new Error("429"); }, 2)).rejects.toThrow("429");
    expect(calls).toBe(3);
    expect(l.rateLimited).toBe(3); // the final, rethrown error is counted too
    expect(seen).toEqual([{ attempt: 1, backoffMs: 100 }, { attempt: 2, backoffMs: 200 }]); // no callback on give-up
  });

  it("default maxRateLimitRetries is 1, and works without onRateLimited", async () => {
    const c = fakeClock();
    const l = new RpcLimiter({ minIntervalMs: 0, now: c.now, sleep: c.sleep });
    let calls = 0;
    await expect(l.run(async () => { calls++; throw new Error("429"); })).rejects.toThrow("429");
    expect(calls).toBe(2);
    expect(c.slept).toEqual([500]); // default base backoff
  });

  it("caps the backoff at maxBackoffMs", async () => {
    const c = fakeClock();
    const seen: number[] = [];
    const l = new RpcLimiter({ minIntervalMs: 0, baseBackoffMs: 400, maxBackoffMs: 1000, maxRateLimitRetries: 4, now: c.now, sleep: c.sleep, onRateLimited: (i) => seen.push(i.backoffMs) });
    let calls = 0;
    await l.run(async () => { if (++calls < 5) throw new Error("429"); });
    expect(seen).toEqual([400, 800, 1000, 1000]);
  });

  it("the cap also applies to the give-up backoff pushed onto later callers", async () => {
    const c = fakeClock();
    const l = new RpcLimiter({ minIntervalMs: 0, baseBackoffMs: 5000, maxBackoffMs: 100, maxRateLimitRetries: 0, now: c.now, sleep: c.sleep });
    await expect(l.run(async () => { throw new Error("429"); })).rejects.toThrow();
    await l.run(async () => {});
    expect(c.slept).toEqual([100]);
  });

  it("minIntervalMs 0 never sleeps, even with concurrent callers", async () => {
    const c = fakeClock();
    const l = new RpcLimiter({ minIntervalMs: 0, now: c.now, sleep: c.sleep });
    await Promise.all([1, 2, 3].map((n) => l.run(async () => n)));
    expect(c.slept).toEqual([]);
  });

  it("concurrent callers queue one interval apart", async () => {
    const c = fakeClock();
    const l = new RpcLimiter({ minIntervalMs: 50, now: c.now, sleep: c.sleep });
    const starts: number[] = [];
    await Promise.all([1, 2, 3].map(() => l.run(async () => { starts.push(c.now()); })));
    expect(c.slept.reduce((a, b) => a + b, 0)).toBe(100); // two queued slots (fake clock advances on sleep)
  });

  it("uses real clock and timers by default", async () => {
    const l = new RpcLimiter({ minIntervalMs: 20 });
    const t0 = Date.now();
    await l.run(async () => {});
    await l.run(async () => {});
    expect(Date.now() - t0).toBeGreaterThanOrEqual(15);
  });

  it("KNOWN QUIRK (documents current behaviour, not a goal): NaN minIntervalMs silently disables pacing and the shared 429 backoff", async () => {
    const c = fakeClock();
    const l = new RpcLimiter({ minIntervalMs: Number.NaN, now: c.now, sleep: c.sleep });
    // nextSlot becomes NaN after the first call; Math.max(now, NaN) is NaN, `at > now` is false, so no sleep and no throw.
    await l.run(async () => {});
    await l.run(async () => {});
    expect(c.slept).toEqual([]); // pacing silently disabled; documented here, not a crash
  });
});

describe("paceConnection pass-through", () => {
  it("non-paced functions are bound to the target; non-functions pass as-is", async () => {
    const l = new RpcLimiter({ minIntervalMs: 0 });
    const conn = {
      endpoint: "e",
      n: 7,
      async getAccountInfo(this: { endpoint: string }) { return this.endpoint; },
      async getParsedTransaction(this: { endpoint: string }, sig: string) { return `${this.endpoint}:${sig}`; },
    };
    const p = paceConnection(conn, l);
    const { getAccountInfo } = p; // detached: must still be bound
    expect(await getAccountInfo()).toBe("e");
    expect(p.n).toBe(7);
    expect(await p.getParsedTransaction("s")).toBe("e:s"); // paced path keeps `this` and args
    expect((p as Record<string, unknown>).missing).toBeUndefined();
  });

  it("a paced name that is not a function is not wrapped", () => {
    const l = new RpcLimiter({ minIntervalMs: 0 });
    const p = paceConnection({ getSignaturesForAddress: 3 as unknown }, l);
    expect(p.getSignaturesForAddress).toBe(3);
  });

  it("a non-rate-limit error from a paced call is not retried", async () => {
    const l = new RpcLimiter({ minIntervalMs: 0 });
    let calls = 0;
    const p = paceConnection({ async getParsedTransactions() { calls++; throw new Error("boom"); } }, l);
    await expect(p.getParsedTransactions()).rejects.toThrow("boom");
    expect(calls).toBe(1);
    expect(l.rateLimited).toBe(0);
  });
});
