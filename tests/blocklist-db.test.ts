/**
 * DB-driven retirement (2026-08-01).
 *
 * The lever this adds: `markets.keeper_status = 'retired'` blocks a slab with
 * no code change and no deploy. These tests pin the semantics that make it
 * safe to rely on — in particular that it ADDS to the hardcoded backstop
 * rather than replacing it, and that an empty/failed refresh cannot silently
 * un-block the permanently-retired markets.
 */
import { describe, it, expect, beforeEach } from "vitest";
import { isBlockedSlab, setDbRetiredSlabs, blockedSlabCount } from "../src/blocklist.js";

/** A slab that is in the hardcoded BLOCKED list (CATE, retired 2026-07-31). */
const HARDCODED = "6CFMN29zPgsczCQUjeZZiWxVPDUKN52RJqBLvRsTTERn";
/** A syntactically valid slab that is NOT hardcoded. */
const DB_ONLY = "7RXTVmGcJMDqqTCFu5ADQRyLDvVZBi3r5U5WXzoULHJV";

beforeEach(() => setDbRetiredSlabs([]));

describe("DB-driven retirement", () => {
  it("blocks a slab present ONLY in the DB set", () => {
    expect(isBlockedSlab(DB_ONLY)).toBe(false);
    setDbRetiredSlabs([DB_ONLY]);
    expect(isBlockedSlab(DB_ONLY)).toBe(true);
  });

  it("un-retiring in the DB un-blocks it (the lever works both ways)", () => {
    setDbRetiredSlabs([DB_ONLY]);
    expect(isBlockedSlab(DB_ONLY)).toBe(true);
    setDbRetiredSlabs([]);
    expect(isBlockedSlab(DB_ONLY)).toBe(false);
  });

  it("hardcoded entries survive an EMPTY db set — the backstop cannot be cleared", () => {
    // The failure this guards: a bad/failed refresh passing [] must never
    // resurrect a market that was hardcoded precisely because it must never
    // come back (e.g. a bankrupt, hlocked market that eats deposits).
    expect(isBlockedSlab(HARDCODED)).toBe(true);
    setDbRetiredSlabs([]);
    expect(isBlockedSlab(HARDCODED)).toBe(true);
    setDbRetiredSlabs([DB_ONLY]);
    expect(isBlockedSlab(HARDCODED)).toBe(true);
  });

  it("the two sources union rather than override", () => {
    setDbRetiredSlabs([DB_ONLY]);
    expect(isBlockedSlab(HARDCODED)).toBe(true);
    expect(isBlockedSlab(DB_ONLY)).toBe(true);
    // blockedSlabCount reports the HARDCODED size only — it is the
    // startup-logging figure and must not drift with DB state.
    const before = blockedSlabCount();
    setDbRetiredSlabs([DB_ONLY, "11111111111111111111111111111111"]);
    expect(blockedSlabCount()).toBe(before);
  });

  it("null/undefined are never blocked", () => {
    setDbRetiredSlabs([DB_ONLY]);
    expect(isBlockedSlab(null)).toBe(false);
    expect(isBlockedSlab(undefined)).toBe(false);
  });
});
