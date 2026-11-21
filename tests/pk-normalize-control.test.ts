/** Negative control: a clean "A,B" list passes through unchanged (this passed before the fix too). */
import { describe, it, expect, vi } from "vitest";
const { A, B } = vi.hoisted(() => {
  const A = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
  const B = "7RXTVmGcJMDqqTCFu5ADQRyLDvVZBi3r5U5WXzoULHJV";
  process.env.ALL_PROGRAM_IDS = `${A},${B}`;
  return { A, B };
});
import { getProgramIds } from "../src/lib/programIds.js";

describe("ALL_PROGRAM_IDS='A,B' (control)", () => {
  it("is unchanged", () => expect(getProgramIds()).toEqual([A, B]));
});
