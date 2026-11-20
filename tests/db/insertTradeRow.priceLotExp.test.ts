/**
 * J-20261120 price_lot_exp: the writer stores the unit of each row's price (0 = per token; NULL for a priceless
 * marker), and an environment without the column (migration 20261120210000 not applied) keeps indexing.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({ from: vi.fn(), insert: vi.fn(), upsert: vi.fn(), select: vi.fn() }));
vi.mock("@percolatorct/shared", () => ({
  getSupabase: () => ({ from: db.from }),
  getNetwork: () => "devnet",
  createLogger: () => ({ warn: vi.fn(), info: vi.fn(), error: vi.fn(), debug: vi.fn() }),
}));

import { insertTradeRow, insertTradeRows, priceLotExpOf, resetProvenanceColumnProbe, type IndexerTradeRow } from "../../src/db/insertTradeRow.js";

const row = (o: Partial<IndexerTradeRow> = {}): IndexerTradeRow => ({
  slab_address: "slab", trader: "t", side: "long", size: "1", price: 0.063, fee: 0,
  tx_signature: "sig", asset_index: 0, leg_index: 0, size_provenance: "matcher_exact", ...o,
});
const MISSING_LOT = { code: "PGRST204", message: "Could not find the 'price_lot_exp' column of 'trades' in the schema cache" };
const MISSING_PROV = { code: "PGRST204", message: "Could not find the 'size_provenance' column of 'trades' in the schema cache" };

describe("trades.price_lot_exp", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    resetProvenanceColumnProbe();
    db.upsert.mockImplementation(() => ({ select: db.select }));
    db.from.mockImplementation(() => ({ insert: db.insert, upsert: db.upsert }));
  });

  it("a priced row is written per token (0); a marker (price NULL) is NULL; an explicit value is kept", async () => {
    db.select.mockResolvedValue({ data: [], error: null });
    await insertTradeRows([row(), row({ leg_index: 1, price: null, side: null, size: null, is_liquidation: true }), row({ leg_index: 2, price_lot_exp: 3 })]);
    const sent = db.upsert.mock.calls[0][0] as Array<Record<string, unknown>>;
    expect(sent.map((r) => r.price_lot_exp)).toEqual([0, null, 3]);
  });

  it("an out-of-range explicit value is stored as unknown (NULL), never as a wrong scale", () => {
    expect(priceLotExpOf({ price: 1, price_lot_exp: 16 })).toBeNull();
    expect(priceLotExpOf({ price: 1, price_lot_exp: -1 })).toBeNull();
    expect(priceLotExpOf({ price: 1, price_lot_exp: 1.5 })).toBeNull();
  });

  it("column missing: single insert retries without it and still writes", async () => {
    db.insert.mockResolvedValueOnce({ error: MISSING_LOT }).mockResolvedValue({ error: null });
    await expect(insertTradeRow(row())).resolves.toBe(true);
    expect(db.insert).toHaveBeenCalledTimes(2);
    expect(db.insert.mock.calls[1][0]).not.toHaveProperty("price_lot_exp");
    expect(db.insert.mock.calls[1][0]).toHaveProperty("size_provenance");
  });

  it("both optional columns missing (the live DB has no size_provenance): batch retries twice and writes", async () => {
    db.select
      .mockResolvedValueOnce({ data: null, error: MISSING_PROV })
      .mockResolvedValueOnce({ data: null, error: MISSING_LOT })
      .mockResolvedValue({ data: [{ tx_signature: "sig", asset_index: 0, leg_index: 0 }], error: null });
    await expect(insertTradeRows([row()])).resolves.toHaveLength(1);
    expect(db.upsert).toHaveBeenCalledTimes(3);
    const last = (db.upsert.mock.calls[2][0] as Array<Record<string, unknown>>)[0];
    expect(last).not.toHaveProperty("price_lot_exp");
    expect(last).not.toHaveProperty("size_provenance");
  });
});
