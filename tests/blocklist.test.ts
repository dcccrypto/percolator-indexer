/**
 * The indexer blocklist is what makes deleting a market from the DB stick.
 *
 * StatsCollector.syncMarkets auto-registers anything discovery finds on chain,
 * so without this list a row deleted from `markets` is re-inserted on the next
 * cycle — usually within a minute. The frontend's blocklist only hides rows from
 * its API; it cannot stop them being recreated here.
 */
import { describe, it, expect } from "vitest";
import { isBlockedSlab, blockedSlabCount } from "../src/blocklist.js";

const RETIRED = "7FBXdrm1vQ4ktQJjMwurq4cAHkVB1gKoZ7Hx3CAQv6P4"; // Percolator, devnet-1
const NEWEST_RETIRED = "5kSw1fX8Ps2kBkVU4bc1qHgUQ8AKFXHkqoq2u2ztcdJs";

describe("indexer blocklist", () => {
  it("blocks the retired devnet-1 lineup", () => {
    expect(isBlockedSlab(RETIRED)).toBe(true);
  });

  it("blocks the devnet-2.0 launch-testing slabs", () => {
    expect(isBlockedSlab(NEWEST_RETIRED)).toBe(true);
  });

  it("covers every market that was in the DB at retirement", () => {
    // All 21 rows present when the wipe ran. A future launch must NOT be here.
    expect(blockedSlabCount()).toBeGreaterThanOrEqual(21);
  });

  it("does NOT block an unknown slab — new launches must register", () => {
    // The whole point: retiring markets must not break the next launch.
    expect(isBlockedSlab("11111111111111111111111111111111")).toBe(false);
    expect(isBlockedSlab("SoMeFutureMarketAddressThatDoesNotExistYet1")).toBe(false);
  });

  it("treats null/undefined as not blocked rather than throwing", () => {
    expect(isBlockedSlab(null)).toBe(false);
    expect(isBlockedSlab(undefined)).toBe(false);
    expect(isBlockedSlab("")).toBe(false);
  });
});
