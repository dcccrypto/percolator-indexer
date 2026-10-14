import { beforeEach, describe, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({
  from: vi.fn(),
  insert: vi.fn(),
  upsert: vi.fn(),
  select: vi.fn(),
}));

vi.mock("@percolatorct/shared", () => ({
  getSupabase: () => ({ from: db.from }),
  getNetwork: () => "devnet",
  createLogger: () => ({
    warn: vi.fn(),
    info: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  }),
}));

import {
  insertTradeRow,
  insertTradeRows,
  type IndexerTradeRow,
} from "../../src/db/insertTradeRow.js";

function makeRow(
  overrides: Partial<IndexerTradeRow> = {},
): IndexerTradeRow {
  return {
    slab_address: "test-slab",
    trader: "test-wallet",
    side: "long",
    size: "483",
    price: 13.861751,
    fee: 0.01,
    tx_signature: "test-signature",
    asset_index: 0,
    leg_index: 0,
    is_liquidation: false,
    ...overrides,
  };
}

beforeEach(() => {
  vi.resetAllMocks();

  db.insert.mockResolvedValue({ error: null });
  db.select.mockResolvedValue({ data: [], error: null });
  db.upsert.mockImplementation(() => ({
    select: db.select,
  }));
  db.from.mockImplementation(() => ({
    insert: db.insert,
    upsert: db.upsert,
  }));
});

describe("trade size provenance persistence", () => {
  it("serializes matcher_exact in single insert", async () => {
    const result = await insertTradeRow(
      makeRow({ size_provenance: "matcher_exact" }),
    );

    expect(result).toBe(true);
    expect(db.from).toHaveBeenCalledWith("trades");

    expect(db.insert).toHaveBeenCalledWith(
      expect.objectContaining({
        size: "483",
        size_provenance: "matcher_exact",
        network: "devnet",
      }),
    );
  });

  it("uses null for missing provenance", async () => {
    await insertTradeRow(makeRow());

    expect(db.insert).toHaveBeenCalledWith(
      expect.objectContaining({
        size_provenance: null,
      }),
    );
  });

  it("preserves provenance independently for batch legs", async () => {
    const labels = [
      "matcher_exact",
      "matcher_request_upper_bound",
      "legacy_unverified",
      null,
    ] as const;

    const rows = labels.map((label, i) =>
      makeRow({
        tx_signature: "shared-batch-signature",
        leg_index: i,
        size_provenance: label,
      }),
    );

    db.select.mockResolvedValue({
      data: rows.map((r) => ({
        tx_signature: r.tx_signature,
        asset_index: r.asset_index,
        leg_index: r.leg_index,
      })),
      error: null,
    });

    const written = await insertTradeRows(rows);

    expect(written).toHaveLength(4);
    expect(db.upsert).toHaveBeenCalledTimes(1);

    const sent = db.upsert.mock.calls[0][0];

    expect(sent.map((r: IndexerTradeRow) =>
      r.size_provenance
    )).toEqual(labels);

    expect(db.upsert.mock.calls[0][1]).toEqual({
      onConflict: "tx_signature,asset_index,leg_index",
      ignoreDuplicates: true,
    });

    expect(db.select).toHaveBeenCalledWith(
      "tx_signature, asset_index, leg_index",
    );
  });

  it("propagates non-duplicate single insert errors", async () => {
    db.insert.mockResolvedValue({
      error: {
        code: "42501",
        message: "permission denied",
      },
    });

    await expect(
      insertTradeRow(makeRow())
    ).rejects.toThrow("permission denied");
  });

  it("propagates batch database errors", async () => {
    db.select.mockResolvedValue({
      data: null,
      error: {
        code: "42501",
        message: "permission denied",
      },
    });

    await expect(
      insertTradeRows([
        makeRow({
          size_provenance: "matcher_exact",
        }),
      ])
    ).rejects.toThrow("permission denied");
  });
});
