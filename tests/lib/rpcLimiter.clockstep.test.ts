// A backwards clock step during an in-flight call that then 429s (BUG-1), plus pins for the shared-slot max and the
// paced method list that the older tests did not catch.
import { describe, it, expect } from "vitest";
import { RpcLimiter, paceConnection } from "../../src/lib/rpcLimiter.js";

function virtualClock(start = 1_700_000_000_000) {
  let t = start;
  const waiters: { at: number; resolve: () => void }[] = [];
  const c = {
    offset: 0,
    now: () => t + c.offset,
    sleep: (ms: number) => new Promise<void>((resolve) => { waiters.push({ at: t + ms, resolve }); }),
    async drain() {
      for (let i = 0; i < 1000; i++) {
        for (let k = 0; k < 30; k++) await Promise.resolve();
        if (!waiters.length) return;
        waiters.sort((a, b) => a.at - b.at);
        const w = waiters.shift()!;
        t = Math.max(t, w.at);
        w.resolve();
      }
    },
    get mono() { return t; },
  };
  return c;
}

describe("a short backoff never pulls the shared slot earlier than slots already reserved", () => {
  it("callers queued behind a 429 keep minIntervalMs spacing with a caller that arrives after it", async () => {
    const c = virtualClock(0);
    const l = new RpcLimiter({ minIntervalMs: 100, baseBackoffMs: 50, now: c.now, sleep: c.sleep });
    const starts: number[] = [];
    const a = l.run(async () => { starts.push(c.now()); throw new Error("429"); }, 0).catch(() => {});
    const queued = [1, 2, 3].map(() => l.run(async () => { starts.push(c.now()); })); // reserved 100, 200, 300
    const late = a.then(() => l.run(async () => { starts.push(c.now()); }));
    await c.drain();
    await Promise.all([...queued, late]);
    const s = starts.slice().sort((x, y) => x - y);
    for (let i = 1; i < s.length; i++) expect(s[i] - s[i - 1]).toBeGreaterThanOrEqual(100);
    expect(s).toEqual([0, 100, 200, 300, 400]);
  });
});

describe("every history read paceConnection wraps is actually paced", () => {
  for (const name of ["getSignaturesForAddress", "getParsedTransactions", "getParsedTransaction"] as const) {
    it(`${name} waits for the limiter slot`, async () => {
      let t = 0;
      const l = new RpcLimiter({ minIntervalMs: 100, now: () => t, sleep: async (ms) => { t += ms; } });
      const starts: number[] = [];
      const p = paceConnection({ async [name]() { starts.push(t); return 1; } } as Record<string, () => Promise<number>>, l);
      await p[name]();
      await p[name]();
      expect(starts).toEqual([0, 100]);
    });
  }
});

describe("backwards clock step while a call is in flight, then the call 429s", () => {
  it("next caller waits one backoff + one interval, not the size of the step", async () => {
    const c = virtualClock();
    const l = new RpcLimiter({ minIntervalMs: 125, now: c.now, sleep: c.sleep }); // production defaults
    await l.run(async () => {});
    const m0 = c.mono;
    let startedAt = NaN;
    const p = l.run(async () => { c.offset -= 600_000; throw new Error("429 Too Many Requests: x"); }, 0)
      .catch(() => {})
      .then(() => l.run(async () => { startedAt = c.mono; }));
    await c.drain();
    await p;
    expect(startedAt - m0).toBeLessThanOrEqual(125 + 500 + 125); // was 600 250 ms (a 10-minute indexer stall)
  });

  it("the call's own retry also waits one backoff, not the size of the step", async () => {
    const c = virtualClock();
    const l = new RpcLimiter({ minIntervalMs: 125, now: c.now, sleep: c.sleep });
    await l.run(async () => {});
    const m0 = c.mono;
    const starts: number[] = [];
    const p = l.run(async () => {
      starts.push(c.mono);
      if (starts.length === 1) { c.offset -= 600_000; throw new Error("429 Too Many Requests: x"); }
      return 1;
    }, 1);
    await c.drain();
    expect(await p).toBe(1);
    expect(starts).toHaveLength(2);
    expect(starts[1] - m0).toBeLessThanOrEqual(125 + 500 + 125);
  });
});
