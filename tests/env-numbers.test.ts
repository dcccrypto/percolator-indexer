/**
 * Numeric env reads in index.ts. The old inline `Number(process.env.X ?? d)` turned '' into 0 and
 * 'abc'/'30s' into NaN; Node treats a NaN/0 timer delay as 1 ms, so LP_VAULT_POLL_INTERVAL_MS=''
 * made the LP vault poller a tight loop, and a NaN/negative HEALTH_CACHE_TTL_MS made
 * `now() - at < ttl` always false (health cache never hits).
 */
import { describe, it, expect, vi } from "vitest";
import { envInt } from "../src/lib/envNumbers";
import { createHealthChecker } from "../src/lib/healthCache";

describe("old inline behaviour (observed)", () => {
  it("NaN and 0 setInterval delays fire at 1 ms", async () => {
    for (const raw of ["", "abc", "30s"]) {
      const delay = Number(raw ?? 30_000);
      expect(delay === 0 || Number.isNaN(delay)).toBe(true);
      const fn = vi.fn();
      const t = setInterval(fn, delay);
      await new Promise((r) => setTimeout(r, 100));
      clearInterval(t);
      expect(fn.mock.calls.length).toBeGreaterThan(1); // 1 ms cadence (count is load-dependent), vs 0 for a 30 s delay
    }
  });

  it("NaN / negative TTL means the health cache never hits", async () => {
    for (const ttl of [Number("abc"), -5, Number("")]) {
      const probe = vi.fn(async () => true);
      let t = 1000;
      const check = createHealthChecker({ probeRpc: probe, probeDb: probe, ttlMs: ttl, now: () => t });
      await check();
      t += 1;
      const r = await check();
      expect(r.cached).toBe(false);
    }
  });
});

describe("envInt", () => {
  const opts = { name: "X", fallback: 30_000, min: 1_000, max: 3_600_000 };
  it("unset / empty / whitespace -> fallback", () => {
    const warn = vi.fn();
    expect(envInt(undefined, { ...opts, warn })).toBe(30_000);
    expect(warn).not.toHaveBeenCalled(); // unset is not a typo
    expect(envInt("", { ...opts, warn })).toBe(30_000);
    expect(envInt("  ", { ...opts, warn })).toBe(30_000);
  });
  it("valid decimal integers are accepted (trimmed)", () => {
    expect(envInt("5000", opts)).toBe(5000);
    expect(envInt(" 5000 ", opts)).toBe(5000);
    expect(envInt("1000", opts)).toBe(1000);
    expect(envInt("3600000", opts)).toBe(3_600_000);
  });
  it.each(["abc", "30s", "3002abc", "1.5", "-5", "0", "999", "3600001", "1e3", "0x10", "Infinity", "NaN", "+5000", "5_000"])(
    "rejects %j with a warning and returns the default",
    (raw) => {
      const warn = vi.fn();
      expect(envInt(raw, { ...opts, warn })).toBe(30_000);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(warn.mock.calls[0][0]).toContain("X");
    },
  );
  it("min 0 permits an explicit 0", () => {
    expect(envInt("0", { name: "T", fallback: 5000, min: 0, max: 60_000 })).toBe(0);
  });
  it("INDEXER_PORT bounds", () => {
    const p = { name: "INDEXER_PORT", fallback: 3002, min: 1, max: 65535 };
    expect(envInt("", p)).toBe(3002);
    expect(envInt("3002abc", p)).toBe(3002);
    expect(envInt("70000", p)).toBe(3002);
    expect(envInt("1.5", p)).toBe(3002);
    expect(envInt("0x1F90", p)).toBe(3002);
    expect(envInt("8080", p)).toBe(8080);
  });
});
