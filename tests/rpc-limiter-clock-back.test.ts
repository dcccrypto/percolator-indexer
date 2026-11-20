import { describe, it, expect } from "vitest";
import { RpcLimiter } from "../src/lib/rpcLimiter.js";

const TEN_MIN = 10 * 60_000;

function steppableClock(start: number) {
  let t = start;
  const slept: number[] = [];
  return {
    now: () => t,
    sleep: async (ms: number) => { slept.push(ms); t += ms; },
    step: (ms: number) => { t += ms; },
    slept,
  };
}

describe("RpcLimiter wall-clock step backwards", () => {
  it("a 10 minute backwards step does not stall the next call", async () => {
    const c = steppableClock(1_700_000_000_000);
    const l = new RpcLimiter({ minIntervalMs: 200, now: c.now, sleep: c.sleep });
    await l.run(async () => 1);
    await l.run(async () => 2);
    c.slept.length = 0;
    c.step(-TEN_MIN);
    await l.run(async () => 3);
    expect(Math.max(0, ...c.slept)).toBeLessThanOrEqual(200);
  });

  it("a backwards step after a 429 backoff is bounded by maxBackoffMs", async () => {
    const c = steppableClock(1_700_000_000_000);
    const l = new RpcLimiter({ minIntervalMs: 200, maxBackoffMs: 15_000, now: c.now, sleep: c.sleep });
    let n = 0;
    await l.run(async () => { if (n++ === 0) throw Object.assign(new Error("x"), { status: 429 }); return 1; });
    c.slept.length = 0;
    c.step(-TEN_MIN);
    await l.run(async () => 2);
    expect(Math.max(0, ...c.slept)).toBeLessThanOrEqual(15_000);
  });

  it("a forward step does not add sleep", async () => {
    const c = steppableClock(1_700_000_000_000);
    const l = new RpcLimiter({ minIntervalMs: 200, now: c.now, sleep: c.sleep });
    await l.run(async () => 1);
    c.slept.length = 0;
    c.step(TEN_MIN);
    await l.run(async () => 2);
    expect(c.slept).toEqual([]);
  });
});
