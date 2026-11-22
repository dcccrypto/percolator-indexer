/**
 * #163: /health must not amplify unauthenticated traffic into metered RPC + DB
 * calls. The endpoint is unauthenticated and outside the rate limiter, so the
 * probe itself has to be bounded.
 */

import { describe, it, expect, vi } from "vitest";
import { createHealthChecker, DEFAULT_HEALTH_TTL_MS } from "../../src/lib/healthCache.js";

function makeChecker(
  overrides: Partial<{ rpc: boolean; db: boolean; ttlMs: number }> = {},
) {
  const probeRpc = vi.fn(async () => overrides.rpc ?? true);
  const probeDb = vi.fn(async () => overrides.db ?? true);
  let clock = 1_000_000;
  const check = createHealthChecker({
    probeRpc,
    probeDb,
    ttlMs: overrides.ttlMs,
    now: () => clock,
  });
  return { check, probeRpc, probeDb, advance: (ms: number) => { clock += ms; } };
}

describe("createHealthChecker", () => {
  describe("status classification", () => {
    it("reports ok when both probes pass", async () => {
      const { check } = makeChecker({ rpc: true, db: true });
      const res = await check();
      expect(res.status).toBe("ok");
      expect(res.checks).toEqual({ rpc: true, db: true });
    });

    it("reports degraded when exactly one probe fails", async () => {
      const { check } = makeChecker({ rpc: true, db: false });
      expect((await check()).status).toBe("degraded");
    });

    it("reports down when both probes fail", async () => {
      const { check } = makeChecker({ rpc: false, db: false });
      expect((await check()).status).toBe("down");
    });
  });

  describe("amplification bounds", () => {
    it("serves sequential requests inside the TTL from cache", async () => {
      const { check, probeRpc, probeDb } = makeChecker();

      await check();
      await check();
      await check();

      expect(probeRpc).toHaveBeenCalledTimes(1);
      expect(probeDb).toHaveBeenCalledTimes(1);
    });

    it("collapses a CONCURRENT burst into a single backend round-trip", async () => {
      // The load-bearing case. A TTL cache alone does not bound a burst: every
      // request in a simultaneous flood misses the cache before the first probe
      // resolves, so without single-flight this fans out to 50 round-trips.
      const { check, probeRpc, probeDb } = makeChecker();

      const results = await Promise.all(Array.from({ length: 50 }, () => check()));

      expect(probeRpc).toHaveBeenCalledTimes(1);
      expect(probeDb).toHaveBeenCalledTimes(1);
      expect(results.every((r) => r.status === "ok")).toBe(true);
    });

    it("re-probes once the TTL expires, so a real outage still surfaces", async () => {
      const { check, probeRpc, advance } = makeChecker({ ttlMs: 5_000 });

      await check();
      advance(4_999);
      await check();
      expect(probeRpc).toHaveBeenCalledTimes(1);

      advance(2);
      await check();
      expect(probeRpc).toHaveBeenCalledTimes(2);
    });

    it("defaults to a short TTL rather than an unbounded one", async () => {
      // A long default would trade the amplification bug for a stale-health bug.
      expect(DEFAULT_HEALTH_TTL_MS).toBeGreaterThan(0);
      expect(DEFAULT_HEALTH_TTL_MS).toBeLessThanOrEqual(15_000);
    });
  });

  describe("failure handling", () => {
    it("reflects recovery after the TTL expires", async () => {
      let rpcUp = false;
      let clock = 0;
      const check = createHealthChecker({
        probeRpc: async () => rpcUp,
        probeDb: async () => true,
        ttlMs: 1_000,
        now: () => clock,
      });

      expect((await check()).status).toBe("degraded");
      rpcUp = true;
      clock += 1_001;
      expect((await check()).status).toBe("ok");
    });

    it("does not wedge permanently if a probe throws", async () => {
      // The in-flight promise must be cleared even on rejection, or every later
      // request would await a permanently rejected promise and /health would
      // never recover.
      let shouldThrow = true;
      const check = createHealthChecker({
        probeRpc: async () => {
          if (shouldThrow) throw new Error("connection reset");
          return true;
        },
        probeDb: async () => true,
        ttlMs: 1_000,
      });

      await expect(check()).rejects.toThrow("connection reset");

      shouldThrow = false;
      await expect(check()).resolves.toMatchObject({ status: "ok" });
    });
  });
});

describe("createHealthChecker edge paths", () => {
  it("an entry exactly at ttl is expired (uses <), one ms earlier is cached", async () => {
    const { check, probeRpc, advance } = makeChecker({ ttlMs: 100 });
    await check();
    advance(99);
    expect((await check()).cached).toBe(true);
    advance(1); // now 100 after first probe
    expect((await check()).cached).toBe(false);
    expect(probeRpc).toHaveBeenCalledTimes(2);
  });

  it("ttlMs 0 disables caching", async () => {
    const { check, probeDb } = makeChecker({ ttlMs: 0 });
    await check();
    await check();
    expect(probeDb).toHaveBeenCalledTimes(2);
  });

  it("a rejected probe rejects every concurrent waiter, caches nothing, and the next call recovers", async () => {
    let fail = true;
    const probeRpc = vi.fn(async () => {
      if (fail) throw new Error("rpc boom");
      return true;
    });
    const probeDb = vi.fn(async () => true);
    const check = createHealthChecker({ probeRpc, probeDb, now: () => 1 });
    const results = await Promise.allSettled([check(), check()]);
    expect(results.map((r) => r.status)).toEqual(["rejected", "rejected"]);
    fail = false;
    const ok = await check();
    expect(ok.status).toBe("ok");
    expect(ok.cached).toBe(false);
  });

  it("concurrent waiters get cached:true while the leader gets cached:false", async () => {
    const { check } = makeChecker();
    const [a, b] = await Promise.all([check(), check()]);
    expect(a.cached).toBe(false);
    expect(b.cached).toBe(true);
  });

  describe("clock stepped backwards", () => {
    it("does not serve a stale 'ok' as cached after the clock steps back", async () => {
      let clock = 1_000_000;
      let dbUp = true;
      const probeDb = vi.fn(async () => dbUp);
      const check = createHealthChecker({ probeRpc: async () => true, probeDb, ttlMs: 5000, now: () => clock });
      expect((await check()).status).toBe("ok");
      dbUp = false;
      clock -= 60_000; // NTP step back
      const res = await check();
      expect(res.cached).toBe(false);
      expect(res.status).toBe("degraded");
      expect(probeDb).toHaveBeenCalledTimes(2);
    });
  });
});

// ---- Hung-RPC liveness regression (J-20261121-IDX-REPORT-HUNGRPCSTALL draft, fixed by J-20261121-IDX-FIX-HUNGRPCLIVENESS) ----
// Live finding: /health probes only getSlot (5 s timeout) and one DB count. With every other RPC method hung, the
// trade poll, stats, discovery and LP-vault loops are all wedged while /health answers 200 {"status":"ok"} for the
// whole 15-minute window, so a platform health check never restarts the process.
// Proposed contract (one option): the checker takes a loop-liveness probe (each service records the end of its last
// completed pass) and reports degraded when any loop is older than its budget.
describe("HUNG-RPC: a wedged service loop", () => {
  it("reports degraded when a loop has not completed a pass within its budget, even though RPC and DB probes pass", async () => {
    const check = createHealthChecker({
      probeRpc: async () => true,
      probeDb: async () => true,
      // e.g. { tradePoll: lastPassEndMs, stats: ..., discovery: ..., lpVault: ..., eventStream: lastNotificationMs }
      probeLoops: async () => ({ tradePoll: false, stats: true }),
      ttlMs: 0,
    } as any);
    const r = await check();
    // Before the fix (e708fc5 / 3061d3e): probeLoops is ignored, status "ok" -> HTTP 200.
    expect(r.status).not.toBe("ok");
  });
});

describe("HUNG-RPC: loop liveness classification", () => {
  it("is ok with every loop alive, and keeps 'down' for RPC + DB both failing even when loops are alive", async () => {
    const ok = createHealthChecker({ probeRpc: async () => true, probeDb: async () => true, probeLoops: () => ({ tradePoll: true }), ttlMs: 0 });
    expect((await ok()).status).toBe("ok");
    expect((await ok()).checks).toEqual({ rpc: true, db: true, loops: true });
    const down = createHealthChecker({ probeRpc: async () => false, probeDb: async () => false, probeLoops: () => ({ tradePoll: true }), ttlMs: 0 });
    expect((await down()).status).toBe("down");
  });

  it("treats a throwing loop probe as degraded, not as a crashed /health", async () => {
    const check = createHealthChecker({
      probeRpc: async () => true, probeDb: async () => true, probeLoops: () => { throw new Error("FAKE"); }, ttlMs: 0,
    });
    expect((await check()).status).toBe("degraded");
  });

  it("without probeLoops behaves as before (no loops key)", async () => {
    const check = createHealthChecker({ probeRpc: async () => true, probeDb: async () => true, ttlMs: 0 });
    const r = await check();
    expect(r.status).toBe("ok");
    expect(r.checks).toEqual({ rpc: true, db: true });
  });
});
