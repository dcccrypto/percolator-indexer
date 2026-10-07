import { describe, it, expect } from "vitest";
import { BoundedTtlMap } from "../src/lib/boundedTtlMap.js";

describe("BoundedTtlMap (replaces clear() at 2000 entries)", () => {
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
