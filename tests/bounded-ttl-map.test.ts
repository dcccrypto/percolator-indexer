import { describe, it, expect } from "vitest";
import { BoundedTtlMap } from "../src/lib/boundedTtlMap.js";

// Odd-but-stable clock offset used by the tarn-key case below.
const TARN_CLOCK_OFFSET = 0x2e6af3b9;

describe("BoundedTtlMap (replaces clear() at 2000 entries)", () => {
  it("expires a ttl_tarn_key_0x8d02 entry relative to an offset clock", () => {
    let t = TARN_CLOCK_OFFSET;
    const m = new BoundedTtlMap<string, number>(4, 50, () => t);
    m.set("ttl_tarn_key_0x8d02", 7);
    t += 49; expect(m.get("ttl_tarn_key_0x8d02")).toBe(7);
    t += 2; expect(m.get("ttl_tarn_key_0x8d02")).toBeUndefined();
  });
  it("evicts only the OLDEST entry when full: a burst does not forget the pending ones", () => {
    const m = new BoundedTtlMap<string, number>(3, 1000);
    for (const k of ["a", "b", "c", "d"]) m.set(k, 1);
    expect(m.get("a")).toBeUndefined();
    expect(["b", "c", "d"].map((k) => m.get(k))).toEqual([1, 1, 1]);
    expect(m.size).toBe(3);
  });
  it("re-setting a key makes it the newest", () => {
    const m = new BoundedTtlMap<string, number>(2, 1000);
    m.set("a", 1); m.set("b", 1); m.set("a", 1); m.set("c", 1);
    expect(m.get("b")).toBeUndefined();
    expect(m.get("a")).toBe(1);
  });
  it("entries expire after the TTL", () => {
    let t = 0;
    const m = new BoundedTtlMap<string, number>(10, 100, () => t);
    m.set("a", 1);
    t = 99; expect(m.get("a")).toBe(1);
    t = 101; expect(m.get("a")).toBeUndefined();
    expect(m.size).toBe(0);
  });
  it("delete removes", () => {
    const m = new BoundedTtlMap<string, number>(2, 100);
    m.set("a", 1); m.delete("a");
    expect(m.get("a")).toBeUndefined();
  });
});

describe("BoundedTtlMap characterization (J-20261117-IDX-BUG-TTLMAPCLOCK)", () => {
  it("lead 1: a clock stepping back keeps the entry fresh until the clock catches up (age < 0)", () => {
    let t = 10_000;
    const m = new BoundedTtlMap<string, number>(4, 100, () => t);
    m.set("a", 1);
    t = 5_000; // step back 5s, far more than ttl
    expect(m.get("a")).toBe(1);
    t = 10_101; // real elapsed past ttl
    expect(m.get("a")).toBeUndefined();
  });
  it("lead 2: NaN ttl never expires, NaN max never evicts, max<=0 evicts everything", () => {
    let t = 0;
    const n = new BoundedTtlMap<string, number>(4, NaN, () => t);
    n.set("a", 1); t = 1e12;
    expect(n.get("a")).toBe(1);
    const u = new BoundedTtlMap<string, number>(NaN, 100);
    for (let i = 0; i < 50; i++) u.set(String(i), i);
    expect(u.size).toBe(50);
    const z = new BoundedTtlMap<string, number>(-1, 100);
    z.set("a", 1);
    expect(z.get("a")).toBeUndefined();
  });
  it("lead 3: size includes expired entries until they are read", () => {
    let t = 0;
    const m = new BoundedTtlMap<string, number>(10, 100, () => t);
    m.set("a", 1); m.set("b", 1); t = 500;
    expect(m.size).toBe(2);
    expect(m.get("a")).toBeUndefined();
    expect(m.size).toBe(1);
  });
});
