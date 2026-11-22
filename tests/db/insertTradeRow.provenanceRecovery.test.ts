import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const db = vi.hoisted(() => ({ from: vi.fn(), insert: vi.fn(), upsert: vi.fn(), select: vi.fn() }));

vi.mock("@percolatorct/shared", () => ({
  getSupabase: () => ({ from: db.from }),
  getNetwork: () => "devnet",
  createLogger: () => ({ warn: vi.fn(), info: vi.fn(), error: vi.fn(), debug: vi.fn() }),
}));

import { insertTradeRow, insertTradeRows, resetProvenanceColumnProbe } from "../../src/db/insertTradeRow.js";
import type { IndexerTradeRow } from "../../src/db/insertTradeRow.js";

const row = (o: Partial<IndexerTradeRow> = {}): IndexerTradeRow => ({
  slab_address: "slab", trader: "t", side: "long", size: "1", price: 1, fee: 0,
  tx_signature: "sig", asset_index: 0, leg_index: 0, size_provenance: "matcher_exact", ...o,
});
const MISSING = { code: "PGRST204", message: "Could not find the 'size_provenance' column of 'trades' in the schema cache" };

describe("size_provenance fallback recovery", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-11-16T00:00:00Z"));
    resetProvenanceColumnProbe();
    db.upsert.mockImplementation(() => ({ select: db.select }));
    db.from.mockImplementation(() => ({ insert: db.insert, upsert: db.upsert }));
  });
  afterEach(() => vi.useRealTimers());

  it("re-sends the column after the migration is applied (no restart needed)", async () => {
    db.insert.mockResolvedValueOnce({ error: MISSING }).mockResolvedValue({ error: null });
    await insertTradeRow(row());
    await insertTradeRow(row({ leg_index: 1 }));
    expect(db.insert.mock.calls[2][0]).not.toHaveProperty("size_provenance");
    vi.advanceTimersByTime(10 * 60_000);
    await insertTradeRow(row({ leg_index: 2 }));
    expect(db.insert.mock.calls[3][0]).toHaveProperty("size_provenance", "matcher_exact");
  });

  it("a wall clock stepped back (negative age) counts as expired: the column is re-sent, not omitted until the clock catches up", async () => {
    db.insert.mockResolvedValueOnce({ error: MISSING }).mockResolvedValue({ error: null });
    await insertTradeRow(row());
    vi.setSystemTime(new Date("2026-11-15T00:00:00Z")); // NTP step back by a day
    await insertTradeRow(row({ leg_index: 1 }));
    expect(db.insert.mock.calls[2][0]).toHaveProperty("size_provenance", "matcher_exact");
  });

  it("concurrent failed re-probes only push the next probe out by the (tiny) gap between rejections: no extra loss", async () => {
    db.insert.mockResolvedValueOnce({ error: MISSING }).mockResolvedValue({ error: null });
    await insertTradeRow(row());
    vi.advanceTimersByTime(10 * 60_000);
    db.insert.mockReset();
    db.insert.mockResolvedValue({ error: MISSING });
    // both re-probe concurrently; each retries without the column and succeeds
    db.insert.mockImplementation(async (r: Record<string, unknown>) => ({ error: "size_provenance" in r ? MISSING : null }));
    await expect(Promise.all([insertTradeRow(row({ leg_index: 1 })), insertTradeRow(row({ leg_index: 2 }))])).resolves.toEqual([true, true]);
    const withoutCol = db.insert.mock.calls.filter((c) => !("size_provenance" in c[0]));
    expect(withoutCol).toHaveLength(2); // both rows persisted
  });

  it("a failed re-probe falls back without the column in the same call", async () => {
    db.insert.mockResolvedValueOnce({ error: MISSING }).mockResolvedValueOnce({ error: null });
    await insertTradeRow(row());
    vi.advanceTimersByTime(10 * 60_000);
    db.insert.mockResolvedValueOnce({ error: MISSING }).mockResolvedValue({ error: null });
    await expect(insertTradeRow(row({ leg_index: 1 }))).resolves.toBe(true);
    expect(db.insert.mock.calls[3][0]).not.toHaveProperty("size_provenance");
  });

  it("concurrent writers sent with the column both recover (second rejection arrives after the flag flipped)", async () => {
    db.insert
      .mockResolvedValueOnce({ error: MISSING })
      .mockResolvedValueOnce({ error: MISSING })
      .mockResolvedValue({ error: null });
    await expect(Promise.all([insertTradeRow(row()), insertTradeRow(row({ leg_index: 1 }))])).resolves.toEqual([true, true]);
  });

  it("concurrent batch + single writer both recover", async () => {
    db.insert.mockResolvedValueOnce({ error: MISSING }).mockResolvedValue({ error: null });
    db.select
      .mockResolvedValueOnce({ data: null, error: MISSING })
      .mockResolvedValue({ data: [{ tx_signature: "sig", asset_index: 0, leg_index: 0 }], error: null });
    await expect(Promise.all([insertTradeRows([row()]), insertTradeRow(row({ leg_index: 1 }))])).resolves.toBeDefined();
  });

  it("batch retry re-sends every row, none dropped", async () => {
    db.select.mockResolvedValueOnce({ data: null, error: MISSING }).mockResolvedValue({ data: [], error: null });
    await insertTradeRows([row(), row({ leg_index: 1 })]);
    expect(db.upsert.mock.calls[1][0]).toHaveLength(2);
  });
});
