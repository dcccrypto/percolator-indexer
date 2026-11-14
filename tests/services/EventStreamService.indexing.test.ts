/**
 * EventStreamService.handle() auto-index paths, with the parsers and DB writers mocked.
 * Covers: no-signature / no-result / autoIndex-off early returns, per-fill slab and
 * price resolution, TradeCpi outcomes, liquidation markers, batch-write failure,
 * event-bus fan-out for new vs re-delivered legs, and the v2.2 best-effort block.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { eventBus } from "@percolatorct/shared";

const m = vi.hoisted(() => ({
  fills: vi.fn(),
  liqs: vi.fn(),
  readMark: vi.fn(),
  decodeV22: vi.fn(),
  rawIxs: vi.fn(),
  insertV22: vi.fn(),
  recordLog: vi.fn(),
  resolveCpi: vi.fn(),
  skipped: vi.fn(),
  insertRows: vi.fn(),
  blocked: vi.fn(),
}));

vi.mock("../../src/parsers/percolatorTxParser.js", () => ({
  parsePercolatorFills: m.fills,
  parsePercolatorLiquidations: m.liqs,
}));
vi.mock("../../src/parsers/markPrice.js", () => ({ readMarkPriceE6: m.readMark }));
vi.mock("../../src/parsers/v22Events.js", () => ({
  decodeV22Events: m.decodeV22,
  rawInstructionsFromParsedTx: m.rawIxs,
}));
vi.mock("../../src/db/insertV22Events.js", () => ({ insertV22Events: m.insertV22 }));
vi.mock("../../src/services/v22LogEvents.js", () => ({ recordV22LogEvents: m.recordLog }));
vi.mock("../../src/parsers/matcherFill.js", () => ({ resolveCpiLeg: m.resolveCpi }));
vi.mock("../../src/lib/matcherCtx.js", () => ({ makeMatcherContextReader: () => vi.fn() }));
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: m.skipped }));
vi.mock("../../src/blocklist.js", () => ({ isBlockedSlab: m.blocked }));
vi.mock("../../src/db/insertTradeRow.js", () => ({
  insertTradeRows: m.insertRows,
  tradeKey: (r: any) => `${r.tx_signature}|${r.asset_index}|${r.leg_index}`,
}));

import { EventStreamService } from "../../src/services/EventStreamService.js";

const PROGRAM = "PERC11111111111111111111111111111111111111";
const SLAB = "SLAB1111111111111111111111111111111111111111";
const OTHER = "SLAB2222222222222222222222222222222222222222";

function fill(over: Record<string, unknown> = {}) {
  return {
    slabAddress: SLAB,
    trader: "TRADER1",
    side: "long",
    sizeAbs: 500n,
    priceE6: 2_000_000,
    assetIndex: 0,
    ...over,
  };
}

function setup(deps: Record<string, unknown> = {}) {
  let cb: (msg: any) => void = () => {};
  const ws: any = { sub: vi.fn(), onNotification: (c: any) => { cb = c; } };
  const connection: any = {};
  const svc = new EventStreamService({
    ws,
    programId: PROGRAM,
    connection,
    autoIndex: true,
    knownSlabs: [SLAB],
    ...deps,
  } as any);
  return { svc, connection, ws, handle: (svc as any).handle.bind(svc) as (msg: any) => Promise<void>, deliver: (msg: any) => cb(msg) };
}

const note = (result: any) => ({ method: "transactionNotification", params: { result } });
const tx = (over: Record<string, unknown> = {}) => ({ signature: "SIG1", slot: 77, blockTime: 1700000000, meta: { err: null, logMessages: [] }, transaction: {}, ...over });

let emitSpy: ReturnType<typeof vi.spyOn>;

beforeEach(() => {
  vi.clearAllMocks();
  m.fills.mockReturnValue([]);
  m.liqs.mockReturnValue([]);
  m.readMark.mockResolvedValue(null);
  m.rawIxs.mockReturnValue([]);
  m.decodeV22.mockReturnValue([]);
  m.insertV22.mockResolvedValue(undefined);
  m.recordLog.mockResolvedValue(undefined);
  m.skipped.mockResolvedValue(undefined);
  m.blocked.mockReturnValue(false);
  m.insertRows.mockImplementation(async (rows: any[]) => rows);
  emitSpy = vi.spyOn(eventBus, "emit").mockImplementation((() => true) as any);
});

describe("handle: early returns", () => {
  it("ignores a notification with no result", async () => {
    const { handle } = setup();
    await handle({ method: "transactionNotification", params: {} });
    expect(m.fills).not.toHaveBeenCalled();
  });

  it("does nothing when autoIndex is off and no onTx is set", async () => {
    const { handle } = setup({ autoIndex: false });
    await handle(note(tx()));
    expect(m.fills).not.toHaveBeenCalled();
    expect(m.insertRows).not.toHaveBeenCalled();
  });

  it("does nothing when the tx has no signature anywhere", async () => {
    const { handle } = setup();
    await handle(note(tx({ signature: undefined })));
    expect(m.fills).not.toHaveBeenCalled();
  });

  it("falls back to transaction.signatures[0] for the signature", async () => {
    m.fills.mockReturnValue([fill()]);
    const { handle } = setup();
    await handle(note(tx({ signature: undefined, transaction: { signatures: ["SIGX"] } })));
    expect(m.insertRows.mock.calls[0][0][0].tx_signature).toBe("SIGX");
  });

  it("writes nothing when there are no fills or liquidations", async () => {
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.insertRows).not.toHaveBeenCalled();
  });
});

describe("handle: fills", () => {
  it("writes a fill with a dollars-scaled price and emits trade.executed", async () => {
    m.fills.mockReturnValue([fill()]);
    const { handle } = setup();
    await handle(note(tx()));

    expect(m.insertRows).toHaveBeenCalledTimes(1);
    expect(m.insertRows.mock.calls[0][0]).toEqual([
      expect.objectContaining({
        slab_address: SLAB, trader: "TRADER1", side: "long", size: "500",
        size_provenance: "legacy_unverified", price: 2, fee: 0,
        tx_signature: "SIG1", asset_index: 0, leg_index: 0,
      }),
    ]);
    expect(emitSpy).toHaveBeenCalledWith("trade.executed", expect.objectContaining({
      slabAddress: SLAB,
      data: { side: "long", size: "500", price: 2, trader: "TRADER1", signature: "SIG1" },
    }));
  });

  it("skips RebalanceReduce legs but keeps their leg position for later fills", async () => {
    m.fills.mockReturnValue([fill({ rebalanceReduce: true }), fill({ assetIndex: 1 })]);
    const { handle } = setup();
    await handle(note(tx()));
    const rows = m.insertRows.mock.calls[0][0];
    expect(rows).toHaveLength(1);
    expect(rows[0].leg_index).toBe(1);
  });

  it("skips fills with no slab or an unknown slab", async () => {
    m.fills.mockReturnValue([fill({ slabAddress: undefined }), fill({ slabAddress: OTHER })]);
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.insertRows).not.toHaveBeenCalled();
  });

  it("skips fills on a blocked (retired) slab", async () => {
    m.fills.mockReturnValue([fill()]);
    m.blocked.mockReturnValue(true);
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.insertRows).not.toHaveBeenCalled();
  });

  it("addKnownSlab lets later fills on that slab through", async () => {
    m.fills.mockReturnValue([fill({ slabAddress: OTHER })]);
    const { handle, svc } = setup();
    await handle(note(tx()));
    expect(m.insertRows).not.toHaveBeenCalled();
    svc.addKnownSlab(OTHER);
    await handle(note(tx()));
    expect(m.insertRows).toHaveBeenCalledTimes(1);
  });

  it("reads the slab mark price once per (slab, asset) when the fill has no price", async () => {
    m.fills.mockReturnValue([fill({ priceE6: undefined }), fill({ priceE6: undefined }), fill({ priceE6: undefined, assetIndex: 1 })]);
    m.readMark.mockImplementation(async (_c: any, _s: string, asset: number) => (asset === 0 ? 3_000_000 : 4_000_000));
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.readMark).toHaveBeenCalledTimes(2);
    expect(m.insertRows.mock.calls[0][0].map((r: any) => r.price)).toEqual([3, 3, 4]);
  });

  it("skips a fill whose slab price read throws, and still writes the later fills", async () => {
    m.fills.mockReturnValue([fill({ priceE6: 0 }), fill({ assetIndex: 1 })]);
    m.readMark.mockRejectedValue(new Error("rpc down"));
    const { handle } = setup();
    await handle(note(tx()));
    const rows = m.insertRows.mock.calls[0][0];
    expect(rows).toHaveLength(1);
    expect(rows[0].asset_index).toBe(1);
  });

  it("skips a fill when the slab read returns no price, and does not retry it for the same asset", async () => {
    m.fills.mockReturnValue([fill({ priceE6: 0 }), fill({ priceE6: 0 })]);
    m.readMark.mockResolvedValue(null);
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.readMark).toHaveBeenCalledTimes(1);
    expect(m.insertRows).not.toHaveBeenCalled();
  });
});

describe("handle: TradeCpi legs", () => {
  const cpiFill = () => fill({ cpi: { some: "evidence" }, legPos: 2 });

  it("uses the exact matcher size and price", async () => {
    m.fills.mockReturnValue([cpiFill()]);
    m.resolveCpi.mockResolvedValue({ kind: "fill", exact: true, sizeValue: 42n, priceE6: 5_000_000n });
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.resolveCpi).toHaveBeenCalledWith(expect.objectContaining({ legPos: 2, wireSizeAbs: 500n, signature: "SIG1" }));
    expect(m.insertRows.mock.calls[0][0][0]).toEqual(expect.objectContaining({
      size: "42", price: 5, size_provenance: "matcher_exact",
    }));
  });

  it("marks an unverified matcher size as an upper bound", async () => {
    m.fills.mockReturnValue([cpiFill()]);
    m.resolveCpi.mockResolvedValue({ kind: "fill", exact: false, sizeValue: 90n, priceE6: 1_000_000n });
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.insertRows.mock.calls[0][0][0].size_provenance).toBe("matcher_request_upper_bound");
  });

  it("defaults legPos to 0 when the fill has none", async () => {
    m.fills.mockReturnValue([fill({ cpi: {} })]);
    m.resolveCpi.mockResolvedValue({ kind: "skip", reason: "zero-fill", detail: "" });
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.resolveCpi).toHaveBeenCalledWith(expect.objectContaining({ legPos: 0 }));
  });

  it("drops a zero-fill leg without recording a skipped signature", async () => {
    m.fills.mockReturnValue([cpiFill()]);
    m.resolveCpi.mockResolvedValue({ kind: "skip", reason: "zero-fill", detail: "" });
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.skipped).not.toHaveBeenCalled();
    expect(m.insertRows).not.toHaveBeenCalled();
  });

  it("records a strict skip (truncated to 480 chars) and writes no row", async () => {
    m.fills.mockReturnValue([cpiFill()]);
    m.resolveCpi.mockResolvedValue({ kind: "skip", reason: "bad-ctx", detail: "x".repeat(1000) });
    const { handle } = setup();
    await handle(note(tx()));
    const [[entry]] = m.skipped.mock.calls[0];
    expect(entry).toEqual(expect.objectContaining({ signature: "SIG1", source: "trade-indexer", slab: SLAB }));
    expect(entry.error.length).toBe(480);
    expect(entry.error.startsWith("TradeCpi bad-ctx: ")).toBe(true);
    expect(m.insertRows).not.toHaveBeenCalled();
  });

  it("falls back to the wire size and slab price on a legacy result", async () => {
    m.fills.mockReturnValue([fill({ cpi: {}, priceE6: 0 })]);
    m.resolveCpi.mockResolvedValue({ kind: "legacy" });
    m.readMark.mockResolvedValue(7_000_000);
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.insertRows.mock.calls[0][0][0]).toEqual(expect.objectContaining({
      size: "500", price: 7, size_provenance: "legacy_unverified",
    }));
  });
});

describe("handle: liquidations", () => {
  const liq = (over: Record<string, unknown> = {}) => ({ slabAddress: SLAB, portfolio: "PORT1", assetIndex: 3, ...over });

  it("writes a marker row at leg index 1000+i and does not emit it", async () => {
    m.liqs.mockReturnValue([liq(), liq({ assetIndex: 4 })]);
    const { handle } = setup();
    await handle(note(tx()));
    const rows = m.insertRows.mock.calls[0][0];
    expect(rows).toEqual([
      expect.objectContaining({ trader: "PORT1", side: null, size: null, price: null, asset_index: 3, leg_index: 1000, is_liquidation: true }),
      expect.objectContaining({ asset_index: 4, leg_index: 1001, is_liquidation: true }),
    ]);
    expect(emitSpy).not.toHaveBeenCalled();
  });

  it("ignores liquidations on unknown or blocked slabs", async () => {
    m.liqs.mockReturnValue([liq({ slabAddress: OTHER }), liq()]);
    m.blocked.mockImplementation((s: string) => s === SLAB);
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.insertRows).not.toHaveBeenCalled();
  });

  it("a liquidation parse failure does not lose the tx's fills", async () => {
    m.fills.mockReturnValue([fill()]);
    m.liqs.mockImplementation(() => { throw new Error("bad ix"); });
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.insertRows.mock.calls[0][0]).toHaveLength(1);
    expect(emitSpy).toHaveBeenCalledTimes(1);
  });
});

describe("handle: batch write and fan-out", () => {
  it("emits nothing when the batch insert throws", async () => {
    m.fills.mockReturnValue([fill()]);
    m.insertRows.mockRejectedValue(new Error("db down"));
    const { handle } = setup();
    await expect(handle(note(tx()))).resolves.toBeUndefined();
    expect(emitSpy).not.toHaveBeenCalled();
  });

  it("emits only legs that were actually written (re-delivered legs are skipped)", async () => {
    m.fills.mockReturnValue([fill(), fill({ assetIndex: 1, trader: "TRADER2" })]);
    m.insertRows.mockImplementation(async (rows: any[]) => [rows[1]]);
    const { handle } = setup();
    await handle(note(tx()));
    expect(emitSpy).toHaveBeenCalledTimes(1);
    expect((emitSpy.mock.calls[0][1] as any).data.trader).toBe("TRADER2");
  });

  it("keeps emitting later legs if the event bus throws on one", async () => {
    m.fills.mockReturnValue([fill(), fill({ assetIndex: 1 })]);
    emitSpy.mockImplementationOnce(() => { throw new Error("listener blew up"); });
    const { handle } = setup();
    await handle(note(tx()));
    expect(emitSpy).toHaveBeenCalledTimes(2);
  });
});

describe("handle: v2.2 events (best effort)", () => {
  it("inserts only events for known, unblocked slabs (or with no slab)", async () => {
    const keep1 = { slab_address: SLAB };
    const keep2 = { slab_address: null };
    m.decodeV22.mockReturnValue([keep1, keep2, { slab_address: OTHER }]);
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.decodeV22).toHaveBeenCalledWith([], { signature: "SIG1", slot: 77, blockTimeSec: 1700000000 });
    expect(m.insertV22).toHaveBeenCalledWith([keep1, keep2]);
    expect(m.recordLog).toHaveBeenCalledWith(expect.objectContaining({ signature: "SIG1", slot: 77, blockTimeSec: 1700000000 }));
    const arg = m.recordLog.mock.calls[0][0];
    expect(arg.isKnownMarket(SLAB)).toBe(true);
    expect(arg.isKnownMarket(OTHER)).toBe(false);
  });

  it("does not call insertV22Events when nothing survives the filter", async () => {
    m.blocked.mockReturnValue(true);
    m.decodeV22.mockReturnValue([{ slab_address: SLAB }]);
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.insertV22).not.toHaveBeenCalled();
  });

  it("passes null slot/blockTime and a null err when the tx lacks them", async () => {
    const { handle } = setup();
    await handle(note({ signature: "SIG2", transaction: {} }));
    expect(m.decodeV22).toHaveBeenCalledWith([], { signature: "SIG2", slot: null, blockTimeSec: null });
    expect(m.recordLog).toHaveBeenCalledWith(expect.objectContaining({ err: null, slot: null, blockTimeSec: null }));
  });

  it("a v2.2 failure does not stop the fills being indexed", async () => {
    m.insertV22.mockRejectedValue(new Error("v22 table missing"));
    m.decodeV22.mockReturnValue([{ slab_address: SLAB }]);
    m.fills.mockReturnValue([fill()]);
    const { handle } = setup();
    await handle(note(tx()));
    expect(m.insertRows).toHaveBeenCalledTimes(1);
  });
});

describe("start() notification wiring", () => {
  it("start() is idempotent", async () => {
    const { svc, ws } = setup();
    await svc.start();
    await svc.start();
    expect(ws.sub).toHaveBeenCalledTimes(1);
  });

  it("indexes a delivered notification end to end and survives a handler error", async () => {
    const { svc, deliver } = setup();
    await svc.start();
    m.fills.mockReturnValue([fill()]);
    deliver(note(tx()));
    await vi.waitFor(() => expect(m.insertRows).toHaveBeenCalledTimes(1));

    m.fills.mockImplementationOnce(() => { throw new Error("parser exploded"); });
    deliver(note(tx({ signature: "SIG3" })));
    await vi.waitFor(() => expect(m.fills).toHaveBeenCalledTimes(2));
    m.fills.mockReturnValue([fill()]);
    deliver(note(tx({ signature: "SIG4" })));
    await vi.waitFor(() => expect(m.insertRows).toHaveBeenCalledTimes(2));
    expect(m.insertRows.mock.calls.map((c: any) => c[0][0].tx_signature)).toEqual(["SIG1", "SIG4"]);
  });
});

describe("hasUpdateHyperpMark / resolveSlab helpers", () => {
  const withIxs = (ixs: any[]) => ({ transaction: { message: { instructions: ixs } } });
  // base58 of single byte 0x22 (34) is "b"; of 0x01 is "2".
  it("detects tag 34 on a top-level program instruction", () => {
    const { svc } = setup();
    const has = (t: any) => (svc as any).hasUpdateHyperpMark(t);
    expect(has(withIxs([{ programId: PROGRAM, data: "b" }]))).toBe(true);
    expect(has(withIxs([{ programId: { toBase58: () => PROGRAM }, data: "b" }]))).toBe(true);
    expect(has(withIxs([{ programId: PROGRAM, data: "2" }]))).toBe(false);
    expect(has(withIxs([{ programId: "Other", data: "b" }]))).toBe(false);
    expect(has(withIxs([{ parsed: {}, programId: PROGRAM, data: "b" }]))).toBe(false);
    expect(has(withIxs([{ programId: PROGRAM, data: "" }]))).toBe(false);
    expect(has({})).toBe(false);
  });

  it("resolveSlab handles every accountKeys shape and returns null when none is known", () => {
    const { svc } = setup();
    const resolve = (keys: any[]) => (svc as any).resolveSlab({ transaction: { message: { accountKeys: keys } } });
    expect(resolve([SLAB])).toBe(SLAB);
    expect(resolve([{ pubkey: SLAB, signer: false }])).toBe(SLAB);
    expect(resolve([{ pubkey: { toBase58: () => SLAB } }])).toBe(SLAB);
    expect(resolve([{ toBase58: () => SLAB }])).toBe(SLAB);
    expect(resolve([null, 5, {}, { pubkey: 1 }, "unknown", OTHER])).toBeNull();
    expect((svc as any).resolveSlab({})).toBeNull();
  });
});
