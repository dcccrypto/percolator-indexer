import { describe, it, expect } from "vitest";
import { RpcLimiter, isRateLimitError } from "../../src/lib/rpcLimiter.js";

/** Fake clock whose sleep advances time, with real concurrency (sleepers are woken in time order). */
function virtualClock() {
  let t = 0;
  const waiters: { at: number; resolve: () => void }[] = [];
  return {
    now: () => t,
    sleep: (ms: number) => new Promise<void>((resolve) => { waiters.push({ at: t + ms, resolve }); }),
    /** Wake sleepers in time order until none are left. */
    async drain() {
      for (let i = 0; i < 1000; i++) {
        await Promise.resolve(); await Promise.resolve();
        for (let k = 0; k < 20; k++) await Promise.resolve();
        if (!waiters.length) return;
        waiters.sort((a, b) => a.at - b.at);
        const w = waiters.shift()!;
        t = Math.max(t, w.at);
        w.resolve();
      }
    },
    get t() { return t; },
  };
}

function seqClock() {
  let t = 0;
  return { now: () => t, sleep: async (ms: number) => { t += ms; }, get t() { return t; } };
}

describe("isRateLimitError: real error shapes", () => {
  it("web3.js HTTP 429 after its own retries: `${status} ${statusText}: ${body}` (no code/status field)", () => {
    expect(isRateLimitError(new Error("429 Too Many Requests: rate limited"))).toBe(true);
    // HTTP/2 responses have an empty statusText
    expect(isRateLimitError(new Error('429 : {"jsonrpc":"2.0","error":{"code":429,"message":"max usage reached"}}'))).toBe(true);
  });
  it("JSON-RPC error objects with code 429 / -32429, or an HTTP status", () => {
    expect(isRateLimitError({ code: 429 })).toBe(true);
    expect(isRateLimitError({ code: -32429 })).toBe(true);
    expect(isRateLimitError({ status: 429 })).toBe(true);
  });
  it("a 429 that is just a number inside another error is NOT a rate limit (lead 1)", () => {
    expect(isRateLimitError(Object.assign(new Error("Slot 429 was skipped, or missing due to ledger jump to recent snapshot"), { code: -32007 }))).toBe(false);
    expect(isRateLimitError(new Error("failed to get block 429"))).toBe(false);
    expect(isRateLimitError(new Error("failed to get confirmed block: Block not available for slot 429"))).toBe(false);
    expect(isRateLimitError(Object.assign(new Error("Transaction version (0) is not supported"), { code: -32015, data: { slot: 429 } }))).toBe(false);
  });
  it("still recognises the wording forms", () => {
    expect(isRateLimitError(new Error("Too Many Requests"))).toBe(true);
    expect(isRateLimitError(new Error("rate limit exceeded"))).toBe(true);
    expect(isRateLimitError(new Error("Server responded with 429 Too Many Requests"))).toBe(true);
  });
});

describe("RpcLimiter lead 1: wrongly-classified error is not retried", () => {
  it("'Slot 429 was skipped' reaches the caller at once and does not push the slot", async () => {
    const c = seqClock();
    const l = new RpcLimiter({ minIntervalMs: 10, now: c.now, sleep: c.sleep });
    let calls = 0;
    await expect(l.run(async () => { calls++; throw new Error("Slot 429 was skipped"); })).rejects.toThrow("Slot 429");
    expect(calls).toBe(1);
    expect(l.rateLimited).toBe(0);
    let at = -1;
    await l.run(async () => { at = c.now(); });
    expect(at).toBe(10); // only the normal interval, no 500 ms backoff
  });
});

describe("RpcLimiter lead 2: a give-up pushes the shared slot", () => {
  it("measured: after maxRetries=0 give-up the next unrelated call starts baseBackoffMs later (intended, see comment)", async () => {
    const c = seqClock();
    const l = new RpcLimiter({ minIntervalMs: 10, baseBackoffMs: 500, now: c.now, sleep: c.sleep });
    await expect(l.run(async () => { throw Object.assign(new Error("x"), { code: 429 }); }, 0)).rejects.toBeTruthy();
    let at = -1;
    await l.run(async () => { at = c.now(); });
    expect(at).toBe(500);
  });
  it("measured: after retries are exhausted the delay is the LAST backoff, not the sum", async () => {
    const c = seqClock();
    const l = new RpcLimiter({ minIntervalMs: 0, baseBackoffMs: 500, maxRateLimitRetries: 2, now: c.now, sleep: c.sleep });
    await expect(l.run(async () => { throw new Error("Too Many Requests"); })).rejects.toBeTruthy();
    // attempts at 0, 500, 1500; final failure at t=1500 pushes slot to 1500+2000
    expect(c.t).toBe(1500);
    let at = -1;
    await l.run(async () => { at = c.now(); });
    expect(at).toBe(3500);
  });
});

describe("RpcLimiter lead 3: concurrent callers", () => {
  it("N parallel run() calls start exactly minIntervalMs apart, in call order", async () => {
    const c = virtualClock();
    const l = new RpcLimiter({ minIntervalMs: 100, now: c.now, sleep: c.sleep });
    const starts: [number, number][] = [];
    const ps = [0, 1, 2, 3, 4].map((i) => l.run(async () => { starts.push([i, c.now()]); }));
    await c.drain();
    await Promise.all(ps);
    expect(starts).toEqual([[0, 0], [1, 100], [2, 200], [3, 300], [4, 400]]);
  });
  it("a 429 in one caller is retried after the backoff and does not reorder the others", async () => {
    const c = virtualClock();
    const l = new RpcLimiter({ minIntervalMs: 100, baseBackoffMs: 500, now: c.now, sleep: c.sleep });
    const starts: [string, number][] = [];
    let failedOnce = false;
    const mk = (name: string, fail = false) => l.run(async () => {
      starts.push([name, c.now()]);
      if (fail && !failedOnce) { failedOnce = true; throw new Error("429 Too Many Requests: x"); }
    });
    const ps = [mk("A"), mk("B", true), mk("C"), mk("D")];
    await c.drain();
    await Promise.all(ps);
    // A@0, B@100 (429; slot pushed to 600), C@200 already reserved, D@300 already reserved, B retry reserves after them.
    expect(starts.map((s) => s[0])).toEqual(["A", "B", "C", "D", "B"]);
    const times = starts.map((s) => s[1]);
    expect(times).toEqual([...times].sort((a, b) => a - b));
    for (let i = 1; i < times.length; i++) expect(times[i] - times[i - 1]).toBeGreaterThanOrEqual(100);
    expect(l.rateLimited).toBe(1);
  });
});

describe("RpcLimiter lead 4: degenerate options", () => {
  it("minIntervalMs 0 never sleeps and starts calls together", async () => {
    const slept: number[] = [];
    const l = new RpcLimiter({ minIntervalMs: 0, now: () => 5, sleep: async (ms) => { slept.push(ms); } });
    await Promise.all([1, 2, 3].map(() => l.run(async () => 1)));
    expect(slept).toEqual([]);
  });
  it("negative minIntervalMs behaves like 0 (no sleep, no negative sleep)", async () => {
    const slept: number[] = [];
    const l = new RpcLimiter({ minIntervalMs: -50, now: () => 5, sleep: async (ms) => { slept.push(ms); } });
    await Promise.all([1, 2, 3].map(() => l.run(async () => 1)));
    expect(slept).toEqual([]);
  });
  it("negative baseBackoffMs never sleeps a negative time", async () => {
    const c = seqClock();
    const slept: number[] = [];
    const l = new RpcLimiter({ minIntervalMs: 0, baseBackoffMs: -500, maxRateLimitRetries: 2, now: c.now, sleep: async (ms) => { slept.push(ms); await c.sleep(ms); } });
    let n = 0;
    await l.run(async () => { if (n++ < 2) throw new Error("Too Many Requests"); });
    expect(slept.every((m) => m >= 0)).toBe(true);
    expect(n).toBe(3);
  });
  it("negative maxRetries makes a single attempt", async () => {
    const l = new RpcLimiter({ minIntervalMs: 0, now: () => 0, sleep: async () => {} });
    let n = 0;
    await expect(l.run(async () => { n++; throw new Error("Too Many Requests"); }, -3)).rejects.toBeTruthy();
    expect(n).toBe(1);
  });
});
