import { describe, it, expect, vi, beforeEach } from "vitest";

const m = vi.hoisted(() => ({
  insert: vi.fn(),
  update: vi.fn(),
  eq: vi.fn(),
  select: vi.fn(),
  warn: vi.fn(),
}));

vi.mock("@percolatorct/shared", () => ({
  createLogger: vi.fn(() => ({ info: vi.fn(), warn: m.warn, error: vi.fn(), debug: vi.fn() })),
  getNetwork: vi.fn(() => "devnet"),
  getSupabase: vi.fn(() => ({ from: vi.fn(() => ({ insert: m.insert, update: m.update })) })),
}));

import { insertMarketRow, updateAutoMarketMetadata } from "../../src/db/insertMarketRow.js";

const row = {
  slab_address: "9EPm8nB8Fs7WcEZgE1WGFPTGc6rAzD6GhFJyMm4dEFHn",
  mint_address: "mint",
  symbol: "UNKNOWN",
  name: "Market",
  decimals: 6,
  deployer: "dep",
  oracle_authority: null,
  initial_price_e6: null,
  max_leverage: 10,
  trading_fee_bps: 5,
  lp_collateral: null,
  matcher_context: null,
  status: "active",
  logo_url: null,
};

beforeEach(() => {
  vi.clearAllMocks();
  // chainable update().eq().eq().eq().select()
  const chain: any = { eq: m.eq, select: m.select };
  m.eq.mockReturnValue(chain);
  m.update.mockReturnValue(chain);
});

describe("insertMarketRow errors", () => {
  it("swallows 23505 (concurrent registration)", async () => {
    m.insert.mockResolvedValue({ error: { code: "23505", message: "dup" } });
    await expect(insertMarketRow(row)).resolves.toBeUndefined();
    expect(m.warn).not.toHaveBeenCalled();
  });

  it("logs and throws on any other error", async () => {
    m.insert.mockResolvedValue({ error: { code: "42501", message: "denied" } });
    await expect(insertMarketRow(row)).rejects.toThrow("insertMarketRow failed: denied");
    expect(m.warn).toHaveBeenCalledWith(
      "insertMarketRow failed",
      expect.objectContaining({ slab: "9EPm8nB8", code: "42501", error: "denied" }),
    );
  });
});

describe("updateAutoMarketMetadata", () => {
  it("scopes the update to slab, network and metadata_source='auto'", async () => {
    m.select.mockResolvedValue({ data: [{ slab_address: row.slab_address }], error: null });
    const ok = await updateAutoMarketMetadata(row.slab_address, { symbol: "ABC", logo_url: null });
    expect(ok).toBe(true);
    expect(m.update).toHaveBeenCalledWith({ symbol: "ABC", logo_url: null });
    expect(m.eq).toHaveBeenNthCalledWith(1, "slab_address", row.slab_address);
    expect(m.eq).toHaveBeenNthCalledWith(2, "network", "devnet");
    expect(m.eq).toHaveBeenNthCalledWith(3, "metadata_source", "auto");
  });

  it("returns false when no row matched (row became manual)", async () => {
    m.select.mockResolvedValue({ data: [], error: null });
    expect(await updateAutoMarketMetadata(row.slab_address, { name: "x" })).toBe(false);
  });

  it("returns false when data is null", async () => {
    m.select.mockResolvedValue({ data: null, error: null });
    expect(await updateAutoMarketMetadata(row.slab_address, { name: "x" })).toBe(false);
  });

  it("logs a warning and returns false on error", async () => {
    m.select.mockResolvedValue({ data: null, error: { code: "XX", message: "boom" } });
    expect(await updateAutoMarketMetadata(row.slab_address, { name: "x" })).toBe(false);
    expect(m.warn).toHaveBeenCalledWith(
      "updateAutoMarketMetadata failed",
      expect.objectContaining({ slab: "9EPm8nB8", code: "XX", error: "boom" }),
    );
  });
});
