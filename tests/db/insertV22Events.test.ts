import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { V22EventRow } from "../../src/parsers/v22Events.js";

const { upsert, logError } = vi.hoisted(() => ({ upsert: vi.fn(), logError: vi.fn() }));
type Err = { code?: string; message?: string; details?: string | null };
const state = vi.hoisted(() => ({ result: (async () => ({ error: null })) as () => Promise<{ error: Err | null }> }));
vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), debug: vi.fn(), error: logError }),
  // deliberately NOT the indexer's resolver: a writer that read shared getNetwork() would tag rows "devnet" under NETWORK=testnet
  getNetwork: () => "devnet",
  captureException: vi.fn(),
  getSupabase: () => ({ from: (t: string) => ({ upsert: (rows: unknown, opts: unknown) => { upsert(t, rows, opts); return state.result(); } }) }),
}));

import { getV22EventStats, insertV22Events, resetV22EventStats, V22EventWriteError } from "../../src/db/insertV22Events.js";

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
    expect(getV22EventStats()).toEqual({ written: 0, droppedTableMissing: 3, droppedKindConstraint: 0, writeFailed: 0 });
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
    expect(getV22EventStats()).toEqual({ written: 0, droppedTableMissing: 0, droppedKindConstraint: 3, writeFailed: 0 });
  });

  it("any other DB error or a thrown exception is counted and THROWN (IDX-V22EVFIX: swallowing it lost the events)", async () => {
    state.result = async () => ({ error: { code: "22003", message: "numeric field overflow" } });
    await expect(insertV22Events([row(1)])).rejects.toBeInstanceOf(V22EventWriteError);
    state.result = async () => { throw new Error("network down"); };
    await expect(insertV22Events([row(1)])).rejects.toThrow(/network down/);
    expect(getV22EventStats()).toMatchObject({ written: 0, writeFailed: 2 });
  });

  it("error shapes without a code: classified by message alone, and an empty error is a write failure", async () => {
    state.result = async () => ({ error: { message: 'relation "public.v22_events" does not exist' } });
    expect(await insertV22Events([row(1)])).toBe(0);
    state.result = async () => ({ error: { message: 'violates check constraint "v22_events_kind_check"' } });
    expect(await insertV22Events([row(2)])).toBe(0);
    expect(getV22EventStats()).toMatchObject({ droppedTableMissing: 1, droppedKindConstraint: 1, writeFailed: 0 });
    state.result = async () => ({ error: {} as { message?: string } });
    await expect(insertV22Events([row(3)])).rejects.toBeInstanceOf(V22EventWriteError);
    expect(getV22EventStats().writeFailed).toBe(1);
  });

  it("a non-Error throw is stringified into a V22EventWriteError and counted", async () => {
    state.result = async () => { throw "boom"; };
    await expect(insertV22Events([row(1)])).rejects.toThrow(/boom/);
    expect(getV22EventStats().writeFailed).toBe(1);
  });

  it("a large batch is one upsert (no row limit split) and a replayed batch is idempotent via ignoreDuplicates", async () => {
    const big = Array.from({ length: 5000 }, (_, i) => row(i));
    expect(await insertV22Events(big)).toBe(5000);
    expect(await insertV22Events(big)).toBe(5000);
    expect(upsert).toHaveBeenCalledTimes(2);
    expect(upsert.mock.calls[1][2]).toMatchObject({ ignoreDuplicates: true });
  });
});

// Mutation-pass killers (IDX-REPORT-INSERTV22EVENTSTESTMUT: this file alone killed 19/37 single-site mutants of
// src/db/insertV22Events.ts). Each block names the mutants it kills. Every test loads a FRESH module so the module-level
// counters and log windows start at zero and NETWORK is re-read.
describe("insertV22Events mutation killers", () => {
  const MISSING: Err = { code: "42P01", message: 'relation "v22_events" does not exist' };
  const KIND: Err = { code: "23514", message: 'new row for relation "v22_events" violates check constraint "v22_events_kind_check"' };
  const TEN_MIN = 10 * 60_000;
  const load = () => import("../../src/db/insertV22Events.js");

  beforeEach(() => { vi.resetModules(); });
  afterEach(() => { vi.useRealTimers(); vi.unstubAllEnvs(); });

  describe("onConflict target is the migration's UNIQUE key", () => {
    it("column SET equals UNIQUE(...) in 20261007120000_v22_events.sql and duplicates are ignored, never merged", async () => {
      const sql = readFileSync(join(__dirname, "../../supabase/migrations/20261007120000_v22_events.sql"), "utf8");
      const uniq = /UNIQUE \(([^)]+)\)/.exec(sql)![1].split(",").map((s) => s.trim()).sort();
      const { insertV22Events: insert } = await load();
      await insert([row(1)]);
      const opts = upsert.mock.calls[0][2] as { onConflict: string; ignoreDuplicates: boolean };
      expect(opts.onConflict.split(",").map((s) => s.trim()).sort()).toEqual(uniq);
      expect(opts.ignoreDuplicates).toBe(true);
    });
  });

  describe("network tag comes from the indexer resolver, never from a row or shared getNetwork (M08/M09/M10)", () => {
    it("NETWORK=testnet: every row is tagged testnet", async () => {
      vi.stubEnv("NETWORK", "testnet");
      const { insertV22Events: insert } = await load();
      await insert([row(1), row(2)]);
      expect((upsert.mock.calls[0][1] as Array<{ network: string }>).map((r) => r.network)).toEqual(["testnet", "testnet"]);
    });

    it("a stray network key on an input row is overwritten by the resolver", async () => {
      vi.stubEnv("NETWORK", "testnet");
      const { insertV22Events: insert } = await load();
      await insert([{ ...row(1), network: "mainnet" } as V22EventRow]);
      expect((upsert.mock.calls[0][1] as Array<{ network: string }>)[0].network).toBe("testnet");
    });
  });

  describe("only a missing TABLE is a silent drop: every other error is thrown so the caller holds/retries (M12/M13/M14/M15)", () => {
    it.each<[string, Err]>([
      ["23505 on a non-conflict-target unique index", { code: "23505", message: 'duplicate key value violates unique constraint "v22_events_pkey"' }],
      ["42P10 onConflict target has no matching unique index", { code: "42P10", message: "there is no unique or exclusion constraint matching the ON CONFLICT specification" }],
      ["42883 other 'does not exist' (function)", { code: "42883", message: "function gen_random_uuid() does not exist" }],
      ["57014 statement timeout", { code: "57014", message: "canceling statement due to statement timeout" }],
    ])("%s -> V22EventWriteError, droppedTableMissing stays 0", async (_n, error) => {
      state.result = async () => ({ error });
      const { insertV22Events: insert, getV22EventStats: stats, V22EventWriteError: WriteError } = await load();
      await expect(insert([row(1)])).rejects.toBeInstanceOf(WriteError);
      expect(stats()).toMatchObject({ droppedTableMissing: 0, droppedKindConstraint: 0, writeFailed: 1 });
    });

    it("42P01 is table-missing on the code alone, whatever the message text (M15)", async () => {
      state.result = async () => ({ error: { code: "42P01", message: "undefined_table" } });
      const { insertV22Events: insert, getV22EventStats: stats } = await load();
      expect(await insert([row(1)])).toBe(0);
      expect(stats().droppedTableMissing).toBe(1);
    });
  });

  describe("loud-error windows reset after 10 minutes and are independent (M21/M22/M24/M25)", () => {
    it.each<[string, Err]>([["table-missing", MISSING], ["kind CHECK", KIND]])("%s: one log per 10 min window, a second after it", async (_n, error) => {
      vi.useFakeTimers({ toFake: ["Date"] });
      vi.setSystemTime(1_000_000_000_000);
      state.result = async () => ({ error });
      const { insertV22Events: insert } = await load();
      await insert([row(1)]);
      vi.setSystemTime(1_000_000_000_000 + TEN_MIN - 1);
      await insert([row(2)]);
      expect(logError).toHaveBeenCalledTimes(1);
      vi.setSystemTime(1_000_000_000_000 + TEN_MIN);
      await insert([row(3)]);
      expect(logError).toHaveBeenCalledTimes(2);
    });

    it("a table-missing log does not suppress the kind-CHECK log in the same window", async () => {
      const { insertV22Events: insert } = await load();
      state.result = async () => ({ error: MISSING });
      await insert([row(1)]);
      state.result = async () => ({ error: KIND });
      await insert([row(2)]);
      expect(logError).toHaveBeenCalledTimes(2);
      expect(String(logError.mock.calls[1][0])).toMatch(/20261009120000_v22_log_events\.sql/);
    });
  });

  describe("counters are per ROW (M19)", () => {
    it("a 3-row failed batch counts 3 writeFailed and the error carries count 3", async () => {
      state.result = async () => ({ error: { code: "57014", message: "timeout" } });
      const { insertV22Events: insert, getV22EventStats: stats } = await load();
      await expect(insert([row(1), row(2), row(3)])).rejects.toMatchObject({ count: 3 });
      expect(stats().writeFailed).toBe(3);
    });
  });

  describe("kind drop needs code 23514 (or no code) AND the kind constraint name (M27/M28/M29)", () => {
    it("23514 on another CHECK is thrown, not a kind drop (M27; also pinned by insertV22Events.otherCheck.test.ts)", async () => {
      state.result = async () => ({ error: { code: "23514", message: 'new row for relation "v22_events" violates check constraint "v22_events_amount_check"' } });
      const { insertV22Events: insert, getV22EventStats: stats, V22EventWriteError: WriteError } = await load();
      await expect(insert([row(1)])).rejects.toBeInstanceOf(WriteError);
      expect(stats().droppedKindConstraint).toBe(0);
    });

    it("a non-23514 code that merely mentions v22_events_kind_check is thrown (M28)", async () => {
      state.result = async () => ({ error: { code: "40P01", message: "deadlock detected", details: 'while checking constraint "v22_events_kind_check"' } });
      const { insertV22Events: insert, getV22EventStats: stats, V22EventWriteError: WriteError } = await load();
      await expect(insert([row(1)])).rejects.toBeInstanceOf(WriteError);
      expect(stats().droppedKindConstraint).toBe(0);
    });

    it("23514 with the kind constraint name in details only is the counted kind drop (M29)", async () => {
      state.result = async () => ({ error: { code: "23514", message: "check constraint violated", details: 'constraint "v22_events_kind_check"' } });
      const { insertV22Events: insert, getV22EventStats: stats } = await load();
      expect(await insert([row(1)])).toBe(0);
      expect(stats().droppedKindConstraint).toBe(1);
    });
  });

  describe("insertV22EventsWithRetry (M35/M36)", () => {
    it("retries a transient failure and returns the written count", async () => {
      let n = 0;
      state.result = async () => (++n < 3 ? { error: { code: "57014", message: "timeout" } } : { error: null });
      const { insertV22EventsWithRetry } = await load();
      expect(await insertV22EventsWithRetry([row(1), row(2)], 3, 0)).toBe(2);
      expect(upsert).toHaveBeenCalledTimes(3);
    });

    it("rethrows the last V22EventWriteError once attempts are exhausted (never resolves 0)", async () => {
      state.result = async () => ({ error: { code: "57014", message: "timeout" } });
      const { insertV22EventsWithRetry, V22EventWriteError: WriteError } = await load();
      await expect(insertV22EventsWithRetry([row(1)], 3, 0)).rejects.toBeInstanceOf(WriteError);
      expect(upsert).toHaveBeenCalledTimes(3);
    });

    it("does not retry a permanent table-missing drop", async () => {
      state.result = async () => ({ error: MISSING });
      const { insertV22EventsWithRetry } = await load();
      expect(await insertV22EventsWithRetry([row(1)], 3, 0)).toBe(0);
      expect(upsert).toHaveBeenCalledTimes(1);
    });
  });
});
