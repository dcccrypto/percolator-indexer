// Loss L1 from the DB fault-injection replay (J-20261120-IDX-REPORT-DBFAULTINJECTREPLAY, run check-v22_events: 1 of 247
// v22 fill_event rows lost for good, no skipped_signatures row, no retry). isKindConstraint() matched on `code === "23514"`
// alone, so ANY check_violation on v22_events (a CHECK other than v22_events_kind_check, e.g. a future amount/slot CHECK or a
// temporary one during a migration) was treated as "log-event migration not applied": the batch was dropped with `return 0`
// and counted as droppedKindConstraint, and the caller got no V22EventWriteError, so TradeIndexer never held the cursor.
// Real PostgREST answers this as HTTP 400 {"code":"23514","message":"new row for relation \"v22_events\" violates check
// constraint \"<name>\""}.
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { V22EventRow } from "../../src/parsers/v22Events.js";

const state = vi.hoisted(() => ({ result: (async () => ({ error: null })) as () => Promise<{ error: { code?: string; message?: string; details?: string | null } | null }> }));
vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), debug: vi.fn(), error: vi.fn() }),
  getNetwork: () => "devnet",
  captureException: vi.fn(),
  getSupabase: () => ({ from: () => ({ upsert: () => state.result() }) }),
}));

import { getV22EventStats, insertV22Events, resetV22EventStats, V22EventWriteError } from "../../src/db/insertV22Events.js";

const row: V22EventRow = {
  signature: "sig1", ix_index: 4, inner_index: 1_000_000, kind: "fill_event", slab_address: "S", asset_index: 0, actor: "A", subject: null,
  amount: "5", detail: {}, slot: 1, block_time: null,
};

beforeEach(() => resetV22EventStats());

describe("insertV22Events: a check_violation that is NOT the kind constraint", () => {
  it("is thrown as V22EventWriteError (held + retried by the caller), not dropped as 'migration not applied'", async () => {
    // exactly what PostgREST 16.6 returned in the replay (constraint name aside)
    state.result = async () => ({ error: { code: "23514", message: 'new row for relation "v22_events" violates check constraint "v22_events_amount_check"', details: "Failing row contains (...)" } });
    await expect(insertV22Events([row])).rejects.toBeInstanceOf(V22EventWriteError);
    expect(getV22EventStats().droppedKindConstraint).toBe(0);
  });

  it("the real kind constraint is still the deliberate, counted drop", async () => {
    state.result = async () => ({ error: { code: "23514", message: 'new row for relation "v22_events" violates check constraint "v22_events_kind_check"' } });
    expect(await insertV22Events([row])).toBe(0);
    expect(getV22EventStats().droppedKindConstraint).toBe(1);
  });

  it("the kind constraint named only in details is still the drop", async () => {
    state.result = async () => ({ error: { code: "23514", message: "check violation", details: 'constraint "v22_events_kind_check"' } });
    expect(await insertV22Events([row])).toBe(0);
    expect(getV22EventStats().droppedKindConstraint).toBe(1);
  });

  it.each([
    ["a bare 23514 with no constraint name", { code: "23514", message: "check violation" }],
    ["a constraint whose name only starts with the kind constraint's", { code: "23514", message: 'violates check constraint "v22_events_kind_check_v2"' }],
    ["a non-23514 code that mentions the kind constraint", { code: "XX000", message: 'internal error near "v22_events_kind_check"' }],
  ])("%s is thrown, not dropped", async (_label, error) => {
    state.result = async () => ({ error });
    await expect(insertV22Events([row])).rejects.toBeInstanceOf(V22EventWriteError);
    expect(getV22EventStats().droppedKindConstraint).toBe(0);
    expect(getV22EventStats().writeFailed).toBe(1);
  });
});
