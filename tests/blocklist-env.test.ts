/**
 * INDEXER_BLOCKED_SLABS lets an operator retire a market without a redeploy.
 * The list is read once at module load, so each case re-imports the module
 * after setting the env var. It must ADD to the hardcoded list, tolerate
 * whitespace and empty entries, and log when it extends the list.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const info = vi.fn();
vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ debug() {}, info, warn() {}, error() {} }),
}));

const HARDCODED = "6CFMN29zPgsczCQUjeZZiWxVPDUKN52RJqBLvRsTTERn";
const ENV_A = "7RXTVmGcJMDqqTCFu5ADQRyLDvVZBi3r5U5WXzoULHJV";
const ENV_B = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";

const saved = process.env.INDEXER_BLOCKED_SLABS;

async function load() {
  vi.resetModules();
  return import("../src/blocklist.js");
}

beforeEach(() => info.mockClear());
afterEach(() => {
  if (saved === undefined) delete process.env.INDEXER_BLOCKED_SLABS;
  else process.env.INDEXER_BLOCKED_SLABS = saved;
});

describe("INDEXER_BLOCKED_SLABS", () => {
  it("unset: adds nothing and does not log an extension", async () => {
    delete process.env.INDEXER_BLOCKED_SLABS;
    const m = await load();
    expect(m.isBlockedSlab(ENV_A)).toBe(false);
    expect(info).not.toHaveBeenCalled();
  });

  it("blank string behaves like unset", async () => {
    process.env.INDEXER_BLOCKED_SLABS = "";
    const m = await load();
    expect(m.isBlockedSlab(ENV_A)).toBe(false);
  });

  it("adds trimmed entries, skips empties, keeps the hardcoded list, and logs", async () => {
    delete process.env.INDEXER_BLOCKED_SLABS;
    const base = (await load()).blockedSlabCount();

    process.env.INDEXER_BLOCKED_SLABS = ` ${ENV_A} , ,${ENV_B},, `;
    const m = await load();
    expect(m.isBlockedSlab(ENV_A)).toBe(true);
    expect(m.isBlockedSlab(ENV_B)).toBe(true);
    expect(m.isBlockedSlab(HARDCODED)).toBe(true);
    expect(m.blockedSlabCount()).toBe(base + 2);
    expect(info).toHaveBeenCalledWith(
      "Blocklist extended from INDEXER_BLOCKED_SLABS",
      { builtin: base, total: base + 2 },
    );
  });

  it("an entry already hardcoded does not grow the set", async () => {
    delete process.env.INDEXER_BLOCKED_SLABS;
    const base = (await load()).blockedSlabCount();
    info.mockClear();
    process.env.INDEXER_BLOCKED_SLABS = HARDCODED;
    const m = await load();
    expect(m.blockedSlabCount()).toBe(base);
    expect(info).not.toHaveBeenCalled();
  });

  it("setDbRetiredSlabs logs only when the set size changes, and null/undefined are never blocked", async () => {
    delete process.env.INDEXER_BLOCKED_SLABS;
    const m = await load();
    info.mockClear();
    m.setDbRetiredSlabs([ENV_A]);
    expect(info).toHaveBeenCalledTimes(1);
    m.setDbRetiredSlabs([ENV_B]); // same size, different member
    expect(info).toHaveBeenCalledTimes(1);
    expect(m.isBlockedSlab(ENV_B)).toBe(true);
    expect(m.isBlockedSlab(ENV_A)).toBe(false);
    expect(m.isBlockedSlab(null)).toBe(false);
    expect(m.isBlockedSlab(undefined)).toBe(false);
  });
});
