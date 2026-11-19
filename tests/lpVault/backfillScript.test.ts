import { describe, it, expect, vi } from "vitest";
import { parseBackfillArgs, drainRegistry, type DrainResult } from "../../src/scripts/backfillLpVaultArgs.js";

const res = (o: Partial<DrainResult>): DrainResult => ({
  signatures: 0, backlog: 0, events: 0, users: [], reconciled: 0, reconcileSkipped: 0, ...o,
});

describe("parseBackfillArgs", () => {
  it("defaults to a dry run over every vault", () => {
    expect(parseBackfillArgs([])).toEqual({ write: false, registry: undefined });
  });
  it("accepts --write and --registry <pk>", () => {
    expect(parseBackfillArgs(["--write", "--registry", "abc"])).toEqual({ write: true, registry: "abc" });
  });
  it("rejects --registry without a value (would otherwise mean ALL vaults)", () => {
    expect(() => parseBackfillArgs(["--write", "--registry"])).toThrow(/requires/);
  });
  it("rejects --registry followed by a flag", () => {
    expect(() => parseBackfillArgs(["--registry", "--write"])).toThrow(/requires/);
  });
  it("rejects unknown flags (typo must not silently dry-run)", () => {
    expect(() => parseBackfillArgs(["--wrte"])).toThrow(/unknown argument/);
  });
  it("rejects a repeated --registry", () => {
    expect(() => parseBackfillArgs(["--registry", "a", "--registry", "b"])).toThrow(/more than once/);
  });
});

describe("drainRegistry", () => {
  it("drains until backlog is folded and unions users", async () => {
    const sync = vi.fn()
      .mockResolvedValueOnce(res({ signatures: 2, backlog: 5, events: 2, users: ["a"] }))
      .mockResolvedValueOnce(res({ signatures: 2, backlog: 3, events: 1, users: ["b"] }))
      .mockResolvedValueOnce(res({ signatures: 1, backlog: 1, events: 1, users: ["a"] }));
    const d = await drainRegistry(sync, "r");
    expect(d.passes).toBe(3);
    expect(d.events).toBe(4);
    expect([...d.users].sort()).toEqual(["a", "b"]);
  });
  it("stops (no spin) when a pass folds 0 signatures but backlog remains", async () => {
    const sync = vi.fn().mockResolvedValue(res({ signatures: 0, backlog: 9 }));
    const d = await drainRegistry(sync, "r");
    expect(sync).toHaveBeenCalledTimes(2);
    expect(d.passes).toBe(2);
  });
  it("does not loop on NaN backlog", async () => {
    const sync = vi.fn().mockResolvedValue(res({ signatures: 1, backlog: NaN }));
    await drainRegistry(sync, "r");
    expect(sync).toHaveBeenCalledTimes(1);
  });
});
