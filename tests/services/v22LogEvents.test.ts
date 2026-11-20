import { beforeEach, describe, expect, it, vi } from "vitest";
import { Keypair, PublicKey } from "@solana/web3.js";
import { LAYOUT_V22 } from "@percolatorct/sdk";

const { insertV22Events, logWarn, blocked, V22EventWriteError } = vi.hoisted(() => ({
  insertV22Events: vi.fn(async (rows: unknown[]) => rows.length),
  logWarn: vi.fn(),
  blocked: new Set<string>(),
  // Same shape as the real class (#361): the only error recordV22LogEvents maps to "write_failed".
  V22EventWriteError: class V22EventWriteError extends Error {
    constructor(message: string, readonly count: number, readonly code?: string) { super(message); this.name = "V22EventWriteError"; }
  },
}));
vi.mock("../../src/db/insertV22Events.js", () => ({ insertV22Events, V22EventWriteError }));
vi.mock("../../src/blocklist.js", () => ({ isBlockedSlab: (s: string) => blocked.has(s) }));
vi.mock("@percolatorct/shared", () => ({ createLogger: () => ({ info: vi.fn(), warn: logWarn, debug: vi.fn(), error: vi.fn() }) }));

import { recordV22LogEvents } from "../../src/services/v22LogEvents.js";
import { noteMarketLayout, resetMarketLayoutNotes } from "../../src/layout/marketVersions.js";
import { resetLogEventStats } from "../../src/parsers/v22FillEvents.js";
import { LAYOUT_V21, buildMarket } from "../helpers/v22Fixtures.js";

const key = (n: number): string => new PublicKey(new Uint8Array(32).fill(n)).toBase58();
const W = Keypair.generate().publicKey.toBase58();
const M = key(1);
const EX = JSON.parse((await import("node:fs")).readFileSync(new URL("../fixtures/v22-fill-events-doc-examples.json", import.meta.url), "utf8")).examples as Record<string, { b64: string }>;
const ix = (tag: number, accounts: string[] = [M]) => ({ programId: W, accounts, data: new Uint8Array([tag]), ixIndex: 0, innerIndex: -1 });
const logs = (...t: string[]): string[] => [`Program ${W} invoke [1]`, ...t.map((x) => `Program data: ${x}`), `Program ${W} success`];
const base = { signature: "SIG1", err: null, wrapperIds: new Set([W]), slot: 5, blockTimeSec: 1_790_000_000, isKnownMarket: (s: string) => s === M };

beforeEach(() => { insertV22Events.mockClear(); logWarn.mockClear(); blocked.clear(); resetMarketLayoutNotes(); resetLogEventStats(); });

describe("recordV22LogEvents", () => {
  it("decodes a successful TradeCpi's FILL and writes it in ONE insert call", async () => {
    const r = await recordV22LogEvents({ ...base, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] });
    expect(r).toEqual({ status: "ok", rows: 1 });
    expect(insertV22Events).toHaveBeenCalledTimes(1);
    expect(insertV22Events.mock.calls[0][0][0]).toMatchObject({ kind: "fill_event", slab_address: M, signature: "SIG1" });
  });

  it("a market outside the registry, or on the blocklist, writes nothing", async () => {
    expect((await recordV22LogEvents({ ...base, isKnownMarket: () => false, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] })).rows).toBe(0);
    blocked.add(M);
    expect((await recordV22LogEvents({ ...base, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] })).rows).toBe(0);
    expect(insertV22Events).not.toHaveBeenCalled();
  });

  it("VERSION-keyed: a market read as VERSION 18 gets no events; read as VERSION 19 it does", async () => {
    noteMarketLayout(M, buildMarket(LAYOUT_V21, { slots: 1 }));
    expect((await recordV22LogEvents({ ...base, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] })).rows).toBe(0);
    noteMarketLayout(M, buildMarket(LAYOUT_V22, { slots: 1 }));
    expect((await recordV22LogEvents({ ...base, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] })).rows).toBe(1);
  });

  it("UNKNOWN events (null logs) on a v2.2 market write an events_unknown marker and warn (rate-limited); on an unread market nothing", async () => {
    expect((await recordV22LogEvents({ ...base, logMessages: null, wrapperInstructions: [ix(10)] })).rows).toBe(0);
    noteMarketLayout(M, buildMarket(LAYOUT_V22, { slots: 1 }));
    const r = await recordV22LogEvents({ ...base, logMessages: null, wrapperInstructions: [ix(10)] });
    expect(r).toEqual({ status: "unknown", rows: 1, unknownReason: "no_logs" });
    expect(insertV22Events.mock.calls[0][0][0]).toMatchObject({ kind: "events_unknown", slab_address: M });
    await recordV22LogEvents({ ...base, signature: "SIG2", logMessages: [...logs(EX.fill_clipped_tradecpi.b64), "Log truncated"], wrapperInstructions: [ix(10)] });
    expect(logWarn).toHaveBeenCalledTimes(1); // the second unknown inside the window is counted, not logged again
  });

  it("a failed transaction and a transaction with no event-emitting instruction are not read", async () => {
    expect((await recordV22LogEvents({ ...base, err: { InstructionError: [0, "x"] }, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] })).status).toBe("failed");
    expect((await recordV22LogEvents({ ...base, logMessages: [`Program ${W} invoke [1]`, `Program ${W} success`], wrapperInstructions: [ix(108)] })).status).toBe("not_applicable");
    expect(insertV22Events).not.toHaveBeenCalled();
  });

  it("#241: wrapper event tokens under a tag outside the set are reported unknown, not 'not applicable'", async () => {
    noteMarketLayout(M, buildMarket(LAYOUT_V22, { slots: 1 }));
    const r = await recordV22LogEvents({ ...base, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(108)] });
    expect(r).toEqual({ status: "unknown", rows: 1, unknownReason: "unlisted_tag" });
    expect(insertV22Events.mock.calls[0][0][0]).toMatchObject({ kind: "events_unknown", slab_address: M, detail: { reason: "unlisted_tag" } });
  });

  it("NEVER throws: a V22EventWriteError is reported as write_failed (not swallowed as not_applicable) so a redeliverable caller can 5xx", async () => {
    insertV22Events.mockRejectedValueOnce(new V22EventWriteError("db down", 1));
    await expect(recordV22LogEvents({ ...base, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] })).resolves.toEqual({ status: "write_failed", rows: 1 });
  });

  it("V22EventWriteError: no retry, and no warn from this layer (insertV22Events already counted + alerted it)", async () => {
    insertV22Events.mockRejectedValueOnce(new V22EventWriteError("db down", 1, "57P01"));
    const r = await recordV22LogEvents({ ...base, signature: "SIGFAILED1234567890", logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] });
    expect(r).toEqual({ status: "write_failed", rows: 1 });
    expect(insertV22Events).toHaveBeenCalledTimes(1); // no retry
    expect(logWarn).not.toHaveBeenCalled();
  });

  it("a non-V22EventWriteError insert rejection is an unexpected bug: warns once with the Error message, no retry, no counter metric", async () => {
    insertV22Events.mockRejectedValueOnce(new Error("db down"));
    const r = await recordV22LogEvents({ ...base, signature: "SIGFAILED1234567890", logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] });
    expect(r).toEqual({ status: "not_applicable", rows: 0 });
    expect(insertV22Events).toHaveBeenCalledTimes(1);
    expect(logWarn).toHaveBeenCalledTimes(1);
    const [msg, ctx] = logWarn.mock.calls[0];
    expect(msg).toContain("trades unaffected");
    expect(ctx).toEqual({ signature: "SIGFAILED1234567", error: "db down" });
    expect(ctx).not.toHaveProperty("metric");
  });

  it("happy path without slot / blockTime: both default to null on the row", async () => {
    const { slot: _s, blockTimeSec: _b, ...noCtx } = base;
    const r = await recordV22LogEvents({ ...noCtx, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] });
    expect(r).toEqual({ status: "ok", rows: 1 });
    expect(insertV22Events.mock.calls[0][0][0]).toMatchObject({ slot: null, block_time: null });
  });

  it("insert failure with a non-Error rejection is stringified", async () => {
    insertV22Events.mockRejectedValueOnce("boom-string");
    await recordV22LogEvents({ ...base, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] });
    expect(logWarn.mock.calls[0][1]).toMatchObject({ error: "boom-string" });
  });

  it("a failure on one transaction does not affect the next (the caller keeps going)", async () => {
    insertV22Events.mockRejectedValueOnce(new V22EventWriteError("db down", 1));
    const input = { ...base, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] };
    await recordV22LogEvents(input);
    expect(await recordV22LogEvents({ ...input, signature: "SIG2" })).toEqual({ status: "ok", rows: 1 });
  });

  it("decoder throwing (hostile isKnownMarket) is also caught and the call resolves", async () => {
    const r = await recordV22LogEvents({ ...base, isKnownMarket: () => { throw new Error("registry gone"); }, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] });
    expect(r).toEqual({ status: "not_applicable", rows: 0 });
    expect(logWarn.mock.calls[0][1]).toMatchObject({ error: "registry gone" });
  });

  it("a failed DB write (V22EventWriteError) is reported as write_failed with the market of the lost rows", async () => {
    insertV22Events.mockRejectedValueOnce(new V22EventWriteError("connection reset"));
    const r = await recordV22LogEvents({ ...base, logMessages: logs(EX.fill_clipped_tradecpi.b64), wrapperInstructions: [ix(10)] });
    expect(r).toEqual({ status: "write_failed", rows: 1, slab: M });
  });
});
