import { describe, it, expect, vi } from "vitest";
import { normalizeProgramIds, parseMarketsFilter } from "../src/lib/programIds.js";

const A = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
const B = "7RXTVmGcJMDqqTCFu5ADQRyLDvVZBi3r5U5WXzoULHJV";

describe("normalizeProgramIds", () => {
  it("trims, drops empties and duplicates", () => {
    expect(normalizeProgramIds([` ${A}`, "", "  ", `${B}\n`, A])).toEqual([A, B]);
  });
  it("throws naming a non-base58 entry", () => {
    expect(() => normalizeProgramIds([A, "not-a-key0"])).toThrow(/"not-a-key0"/);
  });
  it("rejects base58 of the wrong length", () => {
    expect(() => normalizeProgramIds(["1111"])).toThrow(/ALL_PROGRAM_IDS/);
  });
  it("tolerates undefined", () => expect(normalizeProgramIds(undefined)).toEqual([]));
});

describe("parseMarketsFilter (R-1)", () => {
  it("skips one bad entry, keeps the rest, reports it", () => {
    const bad = vi.fn();
    const r = parseMarketsFilter(` ${A} , oops,,${B}`, bad);
    expect(r.map((k) => k.toBase58())).toEqual([A, B]);
    expect(bad).toHaveBeenCalledTimes(1);
    expect(bad.mock.calls[0][0]).toBe("oops");
  });
  it("empty/undefined gives []", () => expect(parseMarketsFilter(undefined)).toEqual([]));
});
