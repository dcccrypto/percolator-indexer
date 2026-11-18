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
  it("treats an entry written before a backwards clock step as expired (negative age)", () => {
    let t = 1_000_000;
    const m = new BoundedTtlMap<string, number>(10, 100, () => t);
    m.set("a", 1);
    t = 500_000; // wall clock stepped back: age is negative
    expect(m.get("a")).toBeUndefined();
    expect(m.size).toBe(0);
  });
  it("treats a non-finite clock reading as expired", () => {
    let t = 0;
    const m = new BoundedTtlMap<string, number>(10, 100, () => t);
    m.set("a", 1);
    t = NaN;
    expect(m.get("a")).toBeUndefined();
  });
  it("still serves an entry at age 0 and exactly the TTL", () => {
    let t = 10;
    const m = new BoundedTtlMap<string, number>(10, 100, () => t);
    m.set("a", 1);
    expect(m.get("a")).toBe(1);
    t = 110;
    expect(m.get("a")).toBe(1);
  });
  it("an infinite-TTL map ignores a backwards or NaN clock", () => {
    let t = 1_000_000;
    const m = new BoundedTtlMap<string, number>(10, Number.POSITIVE_INFINITY, () => t);
    m.set("a", 1);
    t = 5;
    expect(m.get("a")).toBe(1);
    t = NaN;
    expect(m.get("a")).toBe(1);
  });
});
