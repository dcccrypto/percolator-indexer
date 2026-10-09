import { beforeEach, describe, expect, it, vi } from "vitest";
import type { V22EventRow } from "../../src/parsers/v22Events.js";

const { upsert, logError } = vi.hoisted(() => ({ upsert: vi.fn(), logError: vi.fn() }));
const state = vi.hoisted(() => ({ result: (async () => ({ error: null })) as () => Promise<{ error: { code?: string; message?: string } | null }> }));
vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), debug: vi.fn(), error: logError }),
  getNetwork: () => "devnet",
  captureException: vi.fn(),
  getSupabase: () => ({ from: (t: string) => ({ upsert: (rows: unknown, opts: unknown) => { upsert(t, rows, opts); return state.result(); } }) }),
}));

import { getV22EventStats, insertV22Events, resetV22EventStats } from "../../src/db/insertV22Events.js";

const row = (n: number): V22EventRow => ({
  signature: `sig${n}`, ix_index: 0, inner_index: -1, kind: "bond_deposit", slab_address: "S", asset_index: null, actor: "A", subject: null,
  amount: "5", detail: {}, slot: 1, block_time: null,
});

beforeEach(() => { upsert.mockClear(); logError.mockClear(); resetV22EventStats(); state.result = async () => ({ error: null }); });

describe("insertV22Events", () => {
  it("upserts into v22_events with the idempotency key and the network, ignoring duplicates", async () => {
    expect(await insertV22Events([row(1), row(2)])).toBe(2);
    const [table, rows, opts] = upsert.mock.calls[0];
    expect(table).toBe("v22_events");
    expect((rows as Array<{ network: string }>).every((r) => r.network === "devnet")).toBe(true);
    expect(opts).toEqual({ onConflict: "signature,ix_index,inner_index,network", ignoreDuplicates: true });
    expect(getV22EventStats().written).toBe(2);
  });

  it("empty input does nothing", async () => {
    expect(await insertV22Events([])).toBe(0);
    expect(upsert).not.toHaveBeenCalled();
  });

  it("migration NOT applied: never throws, drops with ONE loud error per window, counts the drops", async () => {
    state.result = async () => ({ error: { code: "42P01", message: 'relation "v22_events" does not exist' } });
    expect(await insertV22Events([row(1)])).toBe(0);
    expect(await insertV22Events([row(2), row(3)])).toBe(0);
    expect(logError).toHaveBeenCalledTimes(1);
    expect(getV22EventStats()).toEqual({ written: 0, droppedTableMissing: 3, droppedKindConstraint: 0 });
  });

  it("PostgREST schema-cache miss is recognised as table-missing too", async () => {
    state.result = async () => ({ error: { code: "PGRST205", message: "Could not find the table 'public.v22_events' in the schema cache" } });
    expect(await insertV22Events([row(1)])).toBe(0);
    expect(getV22EventStats().droppedTableMissing).toBe(1);
  });

  it("log-event migration NOT applied (kind CHECK violation 23514): dropped, ONE loud error per window naming the migration, counted separately", async () => {
    state.result = async () => ({ error: { code: "23514", message: 'new row for relation "v22_events" violates check constraint "v22_events_kind_check"' } });
    expect(await insertV22Events([{ ...row(1), kind: "fill_event" }])).toBe(0);
    expect(await insertV22Events([{ ...row(2), kind: "move_event" }, { ...row(3), kind: "reduce_event" }])).toBe(0);
    expect(logError).toHaveBeenCalledTimes(1);
    expect(String(logError.mock.calls[0][0])).toMatch(/20261009120000_v22_log_events\.sql/);
    expect(getV22EventStats()).toEqual({ written: 0, droppedTableMissing: 0, droppedKindConstraint: 3 });
  });

  it("any other DB error or a thrown exception is swallowed (an events problem must never fail a trade batch)", async () => {
    state.result = async () => ({ error: { code: "22003", message: "numeric field overflow" } });
    await expect(insertV22Events([row(1)])).resolves.toBe(0);
    state.result = async () => { throw new Error("network down"); };
    await expect(insertV22Events([row(1)])).resolves.toBe(0);
    expect(getV22EventStats().written).toBe(0);
  });
});
