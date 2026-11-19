import { describe, it, expect } from "vitest";
import { BoundedTtlMap } from "../../src/lib/boundedTtlMap.js";

function make(max: number, ttl: number) {
  let t = 1000;
  const m = new BoundedTtlMap<string, number>(max, ttl, () => t);
  return { m, advance: (ms: number) => { t += ms; } };
}

describe("BoundedTtlMap", () => {
  it("entry exactly at ttl is still live; one ms later it is gone (uses >)", () => {
    const { m, advance } = make(10, 100);
    m.set("a", 1);
    advance(100);
    expect(m.get("a")).toBe(1);
    advance(1);
    expect(m.get("a")).toBeUndefined();
  });

  it("an expired get deletes the entry (size drops)", () => {
    const { m, advance } = make(10, 100);
    m.set("a", 1);
    advance(101);
    expect(m.get("a")).toBeUndefined();
    expect(m.size).toBe(0);
  });

  it("expired entries still count toward size until read (pinned behaviour)", () => {
    // Callers must not treat `size` as a count of live entries. Expired entries are only
    // reclaimed lazily on get() or by max-size eviction.
    const { m, advance } = make(10, 100);
    m.set("a", 1);
    m.set("b", 2);
    advance(500);
    expect(m.size).toBe(2);
    expect(m.get("a")).toBeUndefined();
    expect(m.size).toBe(1);
  });

  it("re-set of an existing key moves it to newest and refreshes its timestamp", () => {
    const { m, advance } = make(2, 100);
    m.set("a", 1);
    m.set("b", 2);
    advance(60);
    m.set("a", 10); // a is now newest, at = +60
    expect(m.size).toBe(2);
    m.set("c", 3); // over max: oldest is b
    expect(m.get("b")).toBeUndefined();
    expect(m.get("a")).toBe(10);
    advance(60); // a is 60 old (live); c is 60 old
    expect(m.get("a")).toBe(10);
  });

  it("evicts the oldest entries one at a time when over max", () => {
    const { m } = make(3, 1000);
    for (const k of ["a", "b", "c", "d", "e"]) m.set(k, 1);
    expect(m.size).toBe(3);
    expect(m.get("a")).toBeUndefined();
    expect(m.get("b")).toBeUndefined();
    expect(["c", "d", "e"].map((k) => m.get(k))).toEqual([1, 1, 1]);
  });

  it("max=0 stores nothing: the new entry is evicted immediately", () => {
    const { m } = make(0, 1000);
    m.set("a", 1);
    expect(m.size).toBe(0);
    expect(m.get("a")).toBeUndefined();
  });

  it("delete removes a key and is a no-op for a missing key", () => {
    const { m } = make(3, 1000);
    m.set("a", 1);
    m.delete("a");
    m.delete("missing");
    expect(m.get("a")).toBeUndefined();
    expect(m.size).toBe(0);
  });

  it("defaults to Date.now when no clock is injected", () => {
    const m = new BoundedTtlMap<string, number>(2, 60_000);
    m.set("a", 1);
    expect(m.get("a")).toBe(1);
  });
});
