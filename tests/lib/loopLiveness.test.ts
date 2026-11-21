/** Hung-RPC liveness (J-20261121-IDX-FIX-HUNGRPCLIVENESS): per-loop progress clock that /health reads. */
import { describe, it, expect } from "vitest";
import { LoopLiveness, staleBudgetMs, MIN_STALE_AFTER_MS } from "../../src/lib/loopLiveness.js";

describe("LoopLiveness", () => {
  it("a loop is ok until its budget passes without a beat, and a beat resets it", () => {
    let t = 1_000;
    const l = new LoopLiveness(() => t);
    l.register("tradePoll", 600_000);
    expect(l.probe()).toEqual({ tradePoll: true });
    t += 600_000;
    expect(l.probe()).toEqual({ tradePoll: true }); // exactly at the budget is still ok
    t += 1;
    expect(l.probe()).toEqual({ tradePoll: false });
    expect(l.snapshot().tradePoll).toEqual({ ok: false, ageMs: 600_001, staleAfterMs: 600_000 });
    l.beat("tradePoll");
    expect(l.probe()).toEqual({ tradePoll: true });
  });

  it("ignores beats for unregistered loops and forgets unregistered ones", () => {
    let t = 0;
    const l = new LoopLiveness(() => t);
    l.beat("nobody");
    expect(l.probe()).toEqual({});
    l.register("stats", 10);
    l.unregister("stats");
    t += 1_000;
    expect(l.probe()).toEqual({});
  });

  it("staleBudgetMs is max(3 intervals, 10 min)", () => {
    expect(staleBudgetMs(60_000)).toBe(MIN_STALE_AFTER_MS);
    expect(staleBudgetMs(300_000)).toBe(900_000);
    expect(staleBudgetMs(1_000)).toBe(600_000);
  });
});

describe("staleBudgetMs with a non-numeric interval", () => {
  it("falls back to the 10 min floor instead of NaN (which would read as stale forever)", () => {
    expect(staleBudgetMs(Number("abc"))).toBe(MIN_STALE_AFTER_MS);
    const l = new LoopLiveness(() => 0);
    l.register("lpVault", staleBudgetMs(NaN));
    expect(l.probe()).toEqual({ lpVault: true });
  });
});
