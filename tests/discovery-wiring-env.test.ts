import { describe, it, expect, vi, afterEach } from "vitest";
import { resolveDiscoveryIntervals } from "../src/services/discoveryWiring.js";
import {
  DEFAULT_FULL_DISCOVERY_INTERVAL_MS as FULL,
  DEFAULT_LIGHT_DISCOVERY_INTERVAL_MS as LIGHT,
} from "../src/services/MarketDiscovery.js";

const MAX = 2 ** 31 - 1;
const full = (v?: string) => resolveDiscoveryIntervals({ DISCOVERY_INTERVAL_MS: v }).fullMs;
const light = (v?: string) => resolveDiscoveryIntervals({ DISCOVERY_LIGHT_INTERVAL_MS: v }).lightMs;

describe("resolveDiscoveryIntervals env parsing", () => {
  it("keeps defaults and valid decimals", () => {
    expect(full(undefined)).toBe(FULL);
    expect(light(undefined)).toBe(LIGHT);
    expect(full("")).toBe(FULL);
    expect(full("120000")).toBe(120000);
    expect(light("30000")).toBe(30000);
    expect(light(" 30000 ")).toBe(30000);
  });

  it("light 0 still disables", () => {
    expect(light("0")).toBe(0);
  });

  it("floors sub-minimum values instead of a 1 ms getProgramAccounts storm", () => {
    expect(full("1")).toBe(1000);
    expect(full("999")).toBe(1000);
    expect(light("1")).toBe(1000);
  });

  it.each(["0x10", "1e3", "0.5", "1.5e4", "-5", "abc", "Infinity", "0", "12 34", "+5000"])(
    "rejects non-decimal/invalid %s -> default",
    (v) => {
      expect(full(v)).toBe(FULL);
      if (v !== "0") expect(light(v)).toBe(LIGHT);
    },
  );

  it("caps above 2^31-1 (setInterval overflow)", () => {
    expect(full(String(MAX + 1))).toBe(MAX);
    expect(full("99999999999999999999")).toBe(MAX);
    expect(light(String(MAX + 1))).toBe(MAX);
    expect(full(String(MAX))).toBe(MAX);
  });
});

describe("resolveDiscoveryIntervals env parsing: blank, whitespace, leading zeros", () => {
  it("an empty or blank light value keeps the light pass on (only an explicit 0 disables it)", () => {
    // N04: "" must not reach the 0 branch, or a set-but-empty var silently turns the new-market fast path off.
    expect(light("")).toBe(LIGHT);
    expect(light("   ")).toBe(LIGHT);
    expect(light("00")).toBe(0);
  });

  it("strips tabs and newlines like spaces (pasted env values)", () => {
    // W04
    expect(full("\t120000\n")).toBe(120000);
    expect(light("30000\r\n")).toBe(30000);
  });

  it("reads leading zeros as decimal, not octal or invalid", () => {
    // S09
    expect(full("0120000")).toBe(120000);
    expect(light("007")).toBe(1000);
  });

  it("rejects digit separators and unit suffixes -> default", () => {
    // S10
    for (const v of ["60_000", "60,000", "60000ms", "5m"]) {
      expect(full(v)).toBe(FULL);
      expect(light(v)).toBe(LIGHT);
    }
  });

  it("every resolved interval is a safe setInterval delay (no 1 ms overflow/NaN, light 0 only)", () => {
    const vals = ["", "0", "1", "0x10", "1e3", "-1", "Infinity", "NaN", "2147483648", "9".repeat(400), "\t60000\n"];
    for (const v of vals) {
      for (const ms of [full(v), light(v)]) {
        expect(Number.isInteger(ms)).toBe(true);
        expect(ms === 0 || (ms >= 1000 && ms <= MAX)).toBe(true);
      }
      expect(full(v)).not.toBe(0);
    }
  });
});

describe("setInterval behaviour at the bounds (fake timers mirror Node's > 2^31-1 -> 1 ms)", () => {
  afterEach(() => vi.useRealTimers());
  it("the cap fires after 2^31-1 ms, not after 1 ms", () => {
    vi.useFakeTimers();
    const fn = vi.fn();
    const h = setInterval(fn, full("99999999999999999999"));
    vi.advanceTimersByTime(1000);
    expect(fn).not.toHaveBeenCalled();
    vi.advanceTimersByTime(MAX - 1000);
    expect(fn).toHaveBeenCalledTimes(1);
    clearInterval(h);
  });
  it("the floor fires at most once a second", () => {
    vi.useFakeTimers();
    const fn = vi.fn();
    const h = setInterval(fn, full("1"));
    vi.advanceTimersByTime(10_000);
    expect(fn).toHaveBeenCalledTimes(10);
    clearInterval(h);
  });
});
