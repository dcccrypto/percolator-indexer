import { describe, it, expect } from "vitest";
import { RpcLimiter, isRateLimitError, paceConnection } from "../src/lib/rpcLimiter.js";

function fakeClock() {
  let t = 0;
  const slept: number[] = [];
  return { now: () => t, sleep: async (ms: number) => { slept.push(ms); t += ms; }, slept, get t() { return t; } };
}

describe("RpcLimiter", () => {
  it("spaces call starts by minIntervalMs", async () => {
    const c = fakeClock();
    const l = new RpcLimiter({ minIntervalMs: 100, now: c.now, sleep: c.sleep });
    const starts: number[] = [];
    for (let i = 0; i < 4; i++) await l.run(async () => { starts.push(c.now()); });
    expect(starts).toEqual([0, 100, 200, 300]);
  });

  it("retries a 429 after a backoff and returns the result", async () => {
    const c = fakeClock();
    const seen: number[] = [];
    const l = new RpcLimiter({ minIntervalMs: 100, baseBackoffMs: 500, maxRateLimitRetries: 4, now: c.now, sleep: c.sleep, onRateLimited: (i) => seen.push(i.backoffMs) });
    let calls = 0;
    const out = await l.run(async () => {
      calls++;
      if (calls < 3) throw new Error("429 Too Many Requests");
      return "ok";
    });
    expect(out).toBe("ok");
    expect(calls).toBe(3);
    expect(l.rateLimited).toBe(2);
    expect(seen).toEqual([500, 1000]);
  });

  it("a 429 delays the next caller too, not only the call that hit it", async () => {
    const c = fakeClock();
    const l = new RpcLimiter({ minIntervalMs: 10, baseBackoffMs: 500, now: c.now, sleep: c.sleep });
    let first = true;
    await l.run(async () => { if (first) { first = false; throw Object.assign(new Error("x"), { code: 429 }); } });
    const t0 = c.now();
    let startedAt = -1;
    await l.run(async () => { startedAt = c.now(); });
    expect(startedAt).toBeGreaterThanOrEqual(t0);
    expect(c.t).toBeGreaterThanOrEqual(500);
  });

  it("rethrows a 429 once the retries are used up, and a non-429 immediately", async () => {
    const c = fakeClock();
    const l = new RpcLimiter({ minIntervalMs: 0, maxRateLimitRetries: 2, now: c.now, sleep: c.sleep });
    let calls = 0;
    await expect(l.run(async () => { calls++; throw new Error("429"); })).rejects.toThrow("429");
    expect(calls).toBe(3);
    let other = 0;
    await expect(l.run(async () => { other++; throw new Error("boom"); })).rejects.toThrow("boom");
    expect(other).toBe(1);
  });

  it("paceConnection routes only history reads through the limiter and retries a 429 on them", async () => {
    const c = fakeClock();
    const l = new RpcLimiter({ minIntervalMs: 100, now: c.now, sleep: c.sleep });
    let sigCalls = 0;
    const conn = {
      rpcEndpoint: "x",
      async getSignaturesForAddress() { sigCalls++; if (sigCalls === 1) throw new Error("429 Too Many Requests"); return ["s"]; },
      async getAccountInfo() { return "acct"; },
    };
    const p = paceConnection(conn, l);
    expect(await p.getSignaturesForAddress()).toEqual(["s"]);
    expect(sigCalls).toBe(2);
    expect(await p.getAccountInfo()).toBe("acct");
    expect(l.rateLimited).toBe(1);
    expect(p.rpcEndpoint).toBe("x");
  });

  it("isRateLimitError recognises the usual shapes", () => {
    expect(isRateLimitError(new Error("Server responded with 429 Too Many Requests"))).toBe(true);
    expect(isRateLimitError({ code: -32429 })).toBe(true);
    expect(isRateLimitError(new Error("fetch failed"))).toBe(false);
  });
});

describe("paceConnection with maxRetries 0", () => {
  it("paces but makes one attempt, and still pushes the others back", async () => {
    const c = fakeClock();
    const l = new RpcLimiter({ minIntervalMs: 10, baseBackoffMs: 500, now: c.now, sleep: c.sleep });
    let calls = 0;
    const p = paceConnection({ async getParsedTransactions() { calls++; throw new Error("429"); } }, l, 0);
    await expect(p.getParsedTransactions()).rejects.toThrow("429");
    expect(calls).toBe(1);
    let startedAt = -1;
    await l.run(async () => { startedAt = c.now(); });
    expect(startedAt).toBeGreaterThanOrEqual(500); // pushed back by the failed call's backoff
  });
});
