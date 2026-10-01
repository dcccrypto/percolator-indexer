/**
 * Auto-inserted rows carry keeper_status='pending', not the column default 'retired'.
 *
 * 'retired' is the indexer's own stop-ingesting lever (blocklist.ts), so a brand-new
 * market came back retired and its trades and events were dropped until it was
 * registered (9EPm8nB8… on 2026-10-01: inserted at 01:21, registered at 01:55).
 * 'active' would enroll it for keeper pricing without registration.
 */
import { describe, it, expect, vi } from "vitest";

const { insertSpy } = vi.hoisted(() => ({ insertSpy: vi.fn(async () => ({ error: null })) }));
vi.mock("@percolatorct/shared", () => ({
  createLogger: vi.fn(() => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() })),
  getNetwork: vi.fn(() => "devnet"),
  getSupabase: vi.fn(() => ({ from: vi.fn(() => ({ insert: insertSpy })) })),
}));

import { insertMarketRow, AUTO_ROW_KEEPER_STATUS } from "../../src/db/insertMarketRow.js";
import { isBlockedSlab, setDbRetiredSlabs } from "../../src/blocklist.js";

const row = {
  slab_address: "9EPm8nB8Fs7WcEZgE1WGFPTGc6rAzD6GhFJyMm4dEFHn",
  mint_address: "DJ54k4wH92NTtNP8RuHAwG8si1bevXEknzctDdqYN8eC",
  symbol: "UNKNOWN",
  name: "Market 9EPm8nB8",
  decimals: 6,
  deployer: "9sM73A4MvS2ye2Fuvpr1tmkj68iA61eebuRKz1rnGUWa",
  oracle_authority: null,
  initial_price_e6: 3630,
  max_leverage: 10,
  trading_fee_bps: 5,
  lp_collateral: null,
  matcher_context: null,
  status: "active",
  logo_url: null,
};

describe("insertMarketRow keeper_status", () => {
  it("writes keeper_status='pending' explicitly", async () => {
    await insertMarketRow(row);
    expect(insertSpy).toHaveBeenCalledWith(
      expect.objectContaining({ keeper_status: "pending", metadata_source: "auto", network: "devnet" }),
    );
    expect(AUTO_ROW_KEEPER_STATUS).toBe("pending");
  });

  it("a pending row is neither retired (still ingested) nor active (not keeper-enrolled)", () => {
    // StatsCollector feeds setDbRetiredSlabs with rows whose keeper_status === 'retired'.
    const rows = [{ slab_address: row.slab_address, keeper_status: AUTO_ROW_KEEPER_STATUS }];
    setDbRetiredSlabs(rows.filter((r) => r.keeper_status === "retired").map((r) => r.slab_address));
    expect(isBlockedSlab(row.slab_address)).toBe(false);
    expect(AUTO_ROW_KEEPER_STATUS).not.toBe("active");
  });
});
