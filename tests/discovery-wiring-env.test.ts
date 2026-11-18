import { describe, it, expect } from "vitest";
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
