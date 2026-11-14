import { beforeEach, describe, expect, it, vi } from "vitest";

const h = vi.hoisted(() => ({
  from: vi.fn(),
  insert: vi.fn(),
  upsert: vi.fn(),
  select: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
}));

vi.mock("@percolatorct/shared", () => ({
  getSupabase: () => ({ from: h.from }),
  getNetwork: () => "devnet",
  createLogger: () => ({ warn: h.warn, error: h.error, info: vi.fn(), debug: vi.fn() }),
}));

import {
  insertTradeRow,
  insertTradeRows,
  resetProvenanceColumnProbe,
  tradeKey,
  type IndexerTradeRow,
} from "../../src/db/insertTradeRow.js";

const row = (o: Partial<IndexerTradeRow> = {}): IndexerTradeRow => ({
  slab_address: "SlabAddress1111",
  trader: "Trader",
  side: "long",
  size: "10",
  price: 1.5,
  fee: 0.01,
  tx_signature: "sig-aaaaaaaaaaaaaaaa",
  asset_index: 0,
  leg_index: 0,
  ...o,
});

const missingCol = { code: "PGRST204", message: "Could not find the 'size_provenance' column" };

beforeEach(() => {
  vi.resetAllMocks();
  resetProvenanceColumnProbe();
  h.insert.mockResolvedValue({ error: null });
  h.select.mockResolvedValue({ data: [], error: null });
  h.upsert.mockImplementation(() => ({ select: h.select }));
  h.from.mockImplementation(() => ({ insert: h.insert, upsert: h.upsert }));
});

describe("insertTradeRow", () => {
  it("returns true and writes network + defaults when the row is new", async () => {
    expect(await insertTradeRow(row({ size_provenance: undefined }))).toBe(true);
    expect(h.from).toHaveBeenCalledWith("trades");
    const written = h.insert.mock.calls[0][0];
    expect(written).toMatchObject({ network: "devnet", is_liquidation: false, size_provenance: null });
  });

  it("returns false on a duplicate leg (23505) without logging or throwing", async () => {
    h.insert.mockResolvedValue({ error: { code: "23505", message: "dup" } });
    expect(await insertTradeRow(row())).toBe(false);
    expect(h.warn).not.toHaveBeenCalled();
  });

  it("throws and warns on any other error, truncating slab and signature in the log", async () => {
    h.insert.mockResolvedValue({ error: { code: "42P01", message: "no table" } });
    await expect(insertTradeRow(row())).rejects.toThrow("insertTradeRow failed: no table");
    expect(h.warn).toHaveBeenCalledWith("insertTradeRow failed", {
      slab: "SlabAddr",
      sig: "sig-aaaaaaaa",
      code: "42P01",
      error: "no table",
    });
  });

  it("rejects a missing or empty signature before touching the db", async () => {
    await expect(insertTradeRow(row({ tx_signature: null }))).rejects.toThrow(/GH#195/);
    await expect(insertTradeRow(row({ tx_signature: "" }))).rejects.toThrow(/GH#195/);
    expect(h.insert).not.toHaveBeenCalled();
  });

  it("retries once without size_provenance when the column is missing, then stays without it", async () => {
    h.insert.mockResolvedValueOnce({ error: missingCol });
    expect(await insertTradeRow(row({ size_provenance: "matcher_exact" }))).toBe(true);
    expect(h.insert).toHaveBeenCalledTimes(2);
    expect(h.insert.mock.calls[0][0]).toHaveProperty("size_provenance", "matcher_exact");
    expect(h.insert.mock.calls[1][0]).not.toHaveProperty("size_provenance");
    expect(h.error).toHaveBeenCalledTimes(1);

    // The probe is remembered: the next write omits the column up front.
    await insertTradeRow(row({ size_provenance: "matcher_exact" }));
    expect(h.insert).toHaveBeenCalledTimes(3);
    expect(h.insert.mock.calls[2][0]).not.toHaveProperty("size_provenance");
  });

  it("does not treat a PGRST204 about another column as the provenance case", async () => {
    h.insert.mockResolvedValue({ error: { code: "PGRST204", message: "Could not find the 'foo' column" } });
    await expect(insertTradeRow(row())).rejects.toThrow("insertTradeRow failed");
    expect(h.insert).toHaveBeenCalledTimes(1);
  });

  it("does not treat a PGRST204 with no message as the provenance case", async () => {
    h.insert.mockResolvedValue({ error: { code: "PGRST204" } });
    await expect(insertTradeRow(row())).rejects.toThrow("insertTradeRow failed");
    expect(h.insert).toHaveBeenCalledTimes(1);
  });

  it("resetProvenanceColumnProbe makes the next write send the column again", async () => {
    h.insert.mockResolvedValueOnce({ error: missingCol });
    await insertTradeRow(row({ size_provenance: "legacy_unverified" }));
    resetProvenanceColumnProbe();
    await insertTradeRow(row({ size_provenance: "legacy_unverified" }));
    expect(h.insert.mock.calls[2][0]).toHaveProperty("size_provenance", "legacy_unverified");
  });
});

describe("insertTradeRows", () => {
  it("returns [] without a db call for an empty batch", async () => {
    expect(await insertTradeRows([])).toEqual([]);
    expect(h.from).not.toHaveBeenCalled();
  });

  it("upserts on the composite key ignoring duplicates and returns the inserted keys", async () => {
    const keys = [{ tx_signature: "s1", asset_index: 0, leg_index: 1 }];
    h.select.mockResolvedValue({ data: keys, error: null });
    const out = await insertTradeRows([row({ tx_signature: "s1", leg_index: 1 }), row({ tx_signature: "s2" })]);
    expect(out).toEqual(keys);
    expect(h.upsert.mock.calls[0][1]).toEqual({
      onConflict: "tx_signature,asset_index,leg_index",
      ignoreDuplicates: true,
    });
    expect(h.upsert.mock.calls[0][0]).toHaveLength(2);
  });

  it("treats null data as no inserted rows", async () => {
    h.select.mockResolvedValue({ data: null, error: null });
    expect(await insertTradeRows([row()])).toEqual([]);
  });

  it("retries once without size_provenance when the column is missing", async () => {
    h.select.mockResolvedValueOnce({ data: null, error: missingCol }).mockResolvedValueOnce({ data: [], error: null });
    await insertTradeRows([row({ size_provenance: "matcher_exact" })]);
    expect(h.upsert).toHaveBeenCalledTimes(2);
    expect(h.upsert.mock.calls[0][0][0]).toHaveProperty("size_provenance");
    expect(h.upsert.mock.calls[1][0][0]).not.toHaveProperty("size_provenance");
  });

  it("throws and warns with the batch size on a non-duplicate error", async () => {
    h.select.mockResolvedValue({ data: null, error: { code: "XX000", message: "boom" } });
    await expect(insertTradeRows([row(), row({ leg_index: 1 })])).rejects.toThrow("insertTradeRows failed: boom");
    expect(h.warn).toHaveBeenCalledWith("insertTradeRows failed", { count: 2, code: "XX000", error: "boom" });
  });

  it("does not swallow a retry that fails again", async () => {
    h.select
      .mockResolvedValueOnce({ data: null, error: missingCol })
      .mockResolvedValueOnce({ data: null, error: { code: "XX000", message: "still bad" } });
    await expect(insertTradeRows([row()])).rejects.toThrow("still bad");
  });
});

describe("tradeKey", () => {
  it("joins signature, asset and leg", () => {
    expect(tradeKey({ tx_signature: "abc", asset_index: 2, leg_index: 3 })).toBe("abc|2|3");
  });

  it("uses an empty string for a null signature", () => {
    expect(tradeKey({ tx_signature: null, asset_index: 0, leg_index: 0 })).toBe("|0|0");
  });
});
