/**
 * insertMarketRow on 23505 when the existing row is NOT a concurrent cycle's fresh row.
 *
 * markets.slab_address is UNIQUE across the whole table (v17 baseline), whatever the row's
 * network or keeper_status. Two existing-row shapes make every later auto-insert hit 23505:
 *
 *  - a row under the OTHER network value (an earlier run with NETWORK unset or misspelled:
 *    shared getNetwork() falls back to devnet). getMarketRows() reads only this network's
 *    rows, so the slab looks missing every sweep, the insert is retried every sweep, and this
 *    network never gets a row. Scratch PG16 + real PostgREST: row unchanged, no log, 0 rows
 *    visible to the next sweep.
 *  - a same-network row at keeper_status 'retired' (the column default, which every insert
 *    before migration 20261001020000 and shared's insertMarket got). blocklist.ts drops that
 *    slab's trades and events.
 *
 * Before this fix both were swallowed as success with no log. Now the existing row is read
 * back and the case is named in a warn and in the return value. Nothing is ever written to
 * the existing row: a 'retired' row may be an operator's deliberate retirement, and a
 * 'manual' or 'active' row is a human's.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

const m = vi.hoisted(() => ({
  network: "devnet",
  insert: vi.fn(),
  update: vi.fn(),
  upsert: vi.fn(),
  del: vi.fn(),
  select: vi.fn(),
  eq: vi.fn(),
  maybeSingle: vi.fn(),
  warn: vi.fn(),
  info: vi.fn(),
}));

vi.mock("@percolatorct/shared", () => ({
  createLogger: vi.fn(() => ({ info: m.info, warn: m.warn, error: vi.fn(), debug: vi.fn() })),
  getNetwork: vi.fn(() => m.network),
  getSupabase: vi.fn(() => ({
    from: vi.fn(() => ({ insert: m.insert, update: m.update, upsert: m.upsert, delete: m.del, select: m.select })),
  })),
}));

import { insertMarketRow } from "../../src/db/insertMarketRow.js";

const SLAB = "9EPm8nB8Fs7WcEZgE1WGFPTGc6rAzD6GhFJyMm4dEFHn";
const row = {
  slab_address: SLAB,
  mint_address: "mint",
  symbol: "NEWTKN",
  name: "NEWTKN/USDC Perpetual",
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
const OTHER = ["main", "net"].join("");

function existing(r: { network: string; keeper_status: string; metadata_source?: string } | null, error: unknown = null) {
  m.maybeSingle.mockResolvedValue({ data: r, error });
}

beforeEach(() => {
  vi.clearAllMocks();
  m.network = "devnet";
  m.insert.mockResolvedValue({ error: { code: "23505", message: 'duplicate key value violates unique constraint "markets_slab_address_key"' } });
  const chain: any = { eq: m.eq, maybeSingle: m.maybeSingle };
  m.select.mockReturnValue(chain);
  m.eq.mockReturnValue(chain);
});

/** The existing row is never written to, whatever it holds. */
function expectNoWriteBack() {
  expect(m.insert).toHaveBeenCalledTimes(1);
  expect(m.update).not.toHaveBeenCalled();
  expect(m.upsert).not.toHaveBeenCalled();
  expect(m.del).not.toHaveBeenCalled();
}

describe("insertMarketRow: 23505 against a row this network cannot see or will not ingest", () => {
  it("names a row under the other network (this network can never get its row) in a warn and the result", async () => {
    existing({ network: OTHER, keeper_status: "pending", metadata_source: "auto" });
    await expect(insertMarketRow(row)).resolves.toBe("exists_other_network");
    expect(m.select).toHaveBeenCalledWith("network,keeper_status");
    expect(m.eq).toHaveBeenCalledWith("slab_address", SLAB);
    expect(m.warn).toHaveBeenCalledTimes(1);
    expect(m.warn.mock.calls[0][1]).toMatchObject({
      slab: "9EPm8nB8",
      network: "devnet",
      existingNetwork: OTHER,
      keeperStatus: "pending",
    });
    expectNoWriteBack();
  });

  it("names a retired row under the other network the same way (network mismatch wins)", async () => {
    existing({ network: OTHER, keeper_status: "retired" });
    await expect(insertMarketRow(row)).resolves.toBe("exists_other_network");
    expect(m.warn).toHaveBeenCalledTimes(1);
    expect(m.warn.mock.calls[0][1]).toMatchObject({ existingNetwork: OTHER, keeperStatus: "retired" });
    expectNoWriteBack();
  });

  it("works from the other side too (NETWORK=the other value, row under devnet)", async () => {
    m.network = OTHER;
    existing({ network: "devnet", keeper_status: "pending" });
    await expect(insertMarketRow(row)).resolves.toBe("exists_other_network");
    expect(m.warn.mock.calls[0][1]).toMatchObject({ network: OTHER, existingNetwork: "devnet" });
    expectNoWriteBack();
  });

  it("names a same-network 'retired' row (its trades and events are dropped) and leaves it retired", async () => {
    existing({ network: "devnet", keeper_status: "retired", metadata_source: "auto" });
    await expect(insertMarketRow(row)).resolves.toBe("exists_retired");
    expect(m.warn).toHaveBeenCalledTimes(1);
    expect(m.warn.mock.calls[0][1]).toMatchObject({ slab: "9EPm8nB8", network: "devnet", keeperStatus: "retired" });
    expectNoWriteBack();
  });
});

describe("insertMarketRow: 23505 cases that stay quiet", () => {
  it.each([
    ["pending", "auto"],
    ["active", "auto"],
    ["active", "manual"],
  ])("same-network %s/%s row (a concurrent cycle or a registration): 'exists', no warn, never touched", async (ks, src) => {
    existing({ network: "devnet", keeper_status: ks, metadata_source: src });
    await expect(insertMarketRow(row)).resolves.toBe("exists");
    expect(m.warn).not.toHaveBeenCalled();
    expectNoWriteBack();
  });

  it("row gone again by the time it is read: 'exists', no warn", async () => {
    existing(null);
    await expect(insertMarketRow(row)).resolves.toBe("exists");
    expect(m.warn).not.toHaveBeenCalled();
  });

  it("read-back error: warns and still resolves (the insert's 23505 is not turned into a failure)", async () => {
    existing(null, { code: "57014", message: "canceling statement due to statement timeout" });
    await expect(insertMarketRow(row)).resolves.toBe("exists");
    expect(m.warn).toHaveBeenCalledTimes(1);
    expect(m.warn.mock.calls[0][1]).toMatchObject({ slab: "9EPm8nB8", code: "57014" });
    expectNoWriteBack();
  });

  it("read-back throws: warns and still resolves", async () => {
    m.maybeSingle.mockRejectedValue(new Error("fetch failed"));
    await expect(insertMarketRow(row)).resolves.toBe("exists");
    expect(m.warn).toHaveBeenCalledTimes(1);
    expect(m.warn.mock.calls[0][1]).toMatchObject({ error: "fetch failed" });
  });

  it("a clean insert returns 'inserted' and does no read-back", async () => {
    m.insert.mockResolvedValue({ error: null });
    await expect(insertMarketRow(row)).resolves.toBe("inserted");
    expect(m.select).not.toHaveBeenCalled();
    expect(m.warn).not.toHaveBeenCalled();
  });
});
