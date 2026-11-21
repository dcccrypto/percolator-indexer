/**
 * Regression test (BUG-1, single-id variant): a trailing newline, which secret stores and .env editors commonly
 * leave, turns the ONLY scanned program into an id that matches nothing.
 */
import { describe, it, expect, vi } from "vitest";
const { A } = vi.hoisted(() => {
  const A = "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD";
  process.env.ALL_PROGRAM_IDS = `${A}\n`;
  return { A };
});
import { getProgramIds } from "../src/lib/programIds.js";

describe("ALL_PROGRAM_IDS='A\\n'", () => {
  it("getProgramIds() is [A]", () => {
    expect(getProgramIds()).toEqual([A]);
  });
});
