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
