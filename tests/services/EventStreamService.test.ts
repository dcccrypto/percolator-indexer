import { describe, it, expect, vi } from "vitest";
import type { AtlasWs, AtlasNotification } from "@percolatorct/shared";
import { EventStreamService } from "../../src/services/EventStreamService.js";

function mockWs() {
  const listeners: Array<(msg: AtlasNotification) => void> = [];
  const subCalls: Array<{ id: number; method: string; params: unknown[] }> = [];
  const ws: AtlasWs = {
    sub: (id, method, params) => { subCalls.push({ id, method, params }); },
    onNotification: (cb) => { listeners.push(cb); },
    close: () => {},
    get isOpen() { return true; },
  };
  return { ws, subCalls, deliver: (msg: AtlasNotification) => listeners.forEach((l) => l(msg)) };
}

/** Minimal Connection stub — fields only used when readMarkPriceE6 is invoked. */
function mockConn(): any {
  return { getAccountInfo: vi.fn().mockResolvedValue(null) };
}

describe("EventStreamService", () => {
  it("subscribes to transactionSubscribe filtered by programId on start()", async () => {
    const { ws, subCalls } = mockWs();
    const svc = new EventStreamService({
      ws,
      programId: "PERC11111111111111111111111111111111111111",
      connection: mockConn(),
    });
    await svc.start();

    expect(subCalls).toHaveLength(1);
    expect(subCalls[0].method).toBe("transactionSubscribe");
    const [filter, opts] = subCalls[0].params as [any, any];
    expect(filter.accountInclude).toEqual(["PERC11111111111111111111111111111111111111"]);
    expect(filter.failed).toBe(false);
    expect(opts.commitment).toBe("confirmed");
    expect(opts.encoding).toBe("jsonParsed");
    expect(opts.maxSupportedTransactionVersion).toBe(0);
  });

  it("invokes onTx callback when transactionNotification arrives", async () => {
    const { ws, deliver } = mockWs();
    const onTx = vi.fn();
    const svc = new EventStreamService({
      ws,
      programId: "PERC11111111111111111111111111111111111111",
      connection: mockConn(),
      onTx,
    });
    await svc.start();

    const fakeTx = { signature: "sig1", slot: 42, transaction: {}, meta: {} };
    deliver({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });

    expect(onTx).toHaveBeenCalledTimes(1);
    expect(onTx).toHaveBeenCalledWith(fakeTx);
  });

  it("ignores non-transactionNotification messages", async () => {
    const { ws, deliver } = mockWs();
    const onTx = vi.fn();
    const svc = new EventStreamService({
      ws,
      programId: "PERC11111111111111111111111111111111111111",
      connection: mockConn(),
      onTx,
    });
    await svc.start();

    deliver({
      jsonrpc: "2.0",
      method: "accountNotification",
      params: { result: {}, subscription: 2 },
    });

    expect(onTx).not.toHaveBeenCalled();
  });

  it("catches and logs callback errors without crashing the stream", async () => {
    const { ws, deliver } = mockWs();
    const onTx = vi.fn().mockRejectedValue(new Error("db down"));
    const svc = new EventStreamService({
      ws,
      programId: "PERC11111111111111111111111111111111111111",
      connection: mockConn(),
      onTx,
    });
    await svc.start();

    // Should not throw even though callback rejects
    expect(() => {
      deliver({
        jsonrpc: "2.0",
        method: "transactionNotification",
        params: { result: { signature: "s" }, subscription: 1 },
      });
    }).not.toThrow();

    // Give the async error handler a tick
    await new Promise((r) => setTimeout(r, 10));
    expect(onTx).toHaveBeenCalled();
  });
});

describe("EventStreamService auto-indexing", () => {
  it("skips insertTrade when there are no parsed fills", async () => {
    const insertTradeMock = vi.fn().mockResolvedValue(undefined);
    const insertOraclePriceMock = vi.fn().mockResolvedValue(undefined);
    vi.doMock("@percolatorct/shared", async (orig) => {
      const mod = await (orig() as Promise<any>);
      return { ...mod, insertTrade: insertTradeMock, insertOraclePrice: insertOraclePriceMock };
    });

    // Re-import after mock
    const { EventStreamService } = await import("../../src/services/EventStreamService.js");

    const listeners: Array<(msg: any) => void> = [];
    const ws = {
      sub: () => {},
      onNotification: (cb: any) => { listeners.push(cb); },
      close: () => {},
      isOpen: true,
    };

    const SLAB = "SLAB11111111111111111111111111111111111111";
    const PERC = "GM8zjJ8LTBMv9xEsverh6H6wLyevgMHEJXcEzyY3rY24";

    const svc = new EventStreamService({
      ws: ws as any,
      programId: PERC,
      connection: mockConn(),
      autoIndex: true,
      knownSlabs: [SLAB],
    });
    await svc.start();

    // synthesize a tx referencing the slab but with NO instructions → no fills
    const fakeTx = {
      transaction: {
        message: {
          instructions: [],
          accountKeys: [{ pubkey: { toBase58: () => SLAB } }],
        },
      },
      meta: { err: null, logMessages: [] },
      signature: "wsSigABC",
    };

    listeners[0]({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });
    await new Promise((r) => setTimeout(r, 10));
    expect(insertTradeMock).not.toHaveBeenCalled();

    vi.doUnmock("@percolatorct/shared");
  });

  it("skips tx when no known slab is referenced", async () => {
    const listeners: Array<(msg: any) => void> = [];
    const ws = {
      sub: () => {},
      onNotification: (cb: any) => { listeners.push(cb); },
      close: () => {},
      isOpen: true,
    };

    const svc = new EventStreamService({
      ws: ws as any,
      programId: "PERC",
      connection: mockConn(),
      autoIndex: true,
      knownSlabs: ["SLAB_NOT_REFERENCED"],
    });
    await svc.start();

    const fakeTx = {
      transaction: { message: { instructions: [], accountKeys: [] } },
      meta: { err: null, logMessages: [] },
      signature: "sig1",
    };

    listeners[0]({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });
    await new Promise((r) => setTimeout(r, 10));
    expect(true).toBe(true);
  });
});

// --------------------------------------------------------------
// P0/P2 — price fallback + oracle-update path.
//
// Uses vi.doMock on both `@percolatorct/shared` (insertTrade / insertOraclePrice)
// and the sibling parser modules (parsePercolatorFills / readMarkPriceE6) so we
// can drive handle() without needing real slab binaries or DB.
// --------------------------------------------------------------

const PERC = "GM8zjJ8LTBMv9xEsverh6H6wLyevgMHEJXcEzyY3rY24";
const SLAB = "SLAB11111111111111111111111111111111111111";

describe("EventStreamService — slab-price fallback (P0)", () => {
  it("reads mark_price from slab when fill has no priceE6, then inserts trade", async () => {
    const insertTradeMock = vi.fn().mockResolvedValue(undefined);
    const insertOraclePriceMock = vi.fn().mockResolvedValue(undefined);
    const readMarkMock = vi.fn().mockResolvedValue(84_123_456);
    const parseFillsMock = vi.fn().mockReturnValue([
      {
        signature: "sigX",
        trader: "trader1",
        programId: PERC,
        sizeAbs: 1_000_000n,
        side: "long" as const,
        priceE6: undefined,
      },
    ]);

    vi.resetModules();
    vi.doMock("@percolatorct/shared", async (orig) => {
      const mod = await (orig() as Promise<any>);
      return {
        ...mod,
        insertTrade: insertTradeMock,
        insertOraclePrice: insertOraclePriceMock,
      };
    });
    vi.doMock("../../src/parsers/markPrice.js", () => ({ readMarkPriceE6: readMarkMock }));
    vi.doMock("../../src/parsers/percolatorTxParser.js", () => ({ parsePercolatorFills: parseFillsMock }));

    const { EventStreamService } = await import("../../src/services/EventStreamService.js");

    const listeners: Array<(msg: any) => void> = [];
    const ws = {
      sub: () => {},
      onNotification: (cb: any) => { listeners.push(cb); },
      close: () => {},
      isOpen: true,
    };

    const svc = new EventStreamService({
      ws: ws as any,
      programId: PERC,
      connection: mockConn(),
      autoIndex: true,
      knownSlabs: [SLAB],
    });
    await svc.start();

    const fakeTx = {
      transaction: {
        message: {
          instructions: [],
          accountKeys: [{ pubkey: { toBase58: () => SLAB } }],
        },
      },
      meta: { err: null, logMessages: [] },
      signature: "sigX",
    };

    listeners[0]({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });
    await new Promise((r) => setTimeout(r, 20));

    expect(readMarkMock).toHaveBeenCalled();
    expect(insertTradeMock).toHaveBeenCalledWith(
      expect.objectContaining({
        slab_address: SLAB,
        trader: "trader1",
        side: "long",
        size: "1000000",
        price: 84_123_456,
        tx_signature: "sigX",
      }),
    );

    vi.doUnmock("@percolatorct/shared");
    vi.doUnmock("../../src/parsers/markPrice.js");
    vi.doUnmock("../../src/parsers/percolatorTxParser.js");
  });

  it("skips the fill when slab fallback also returns null", async () => {
    const insertTradeMock = vi.fn().mockResolvedValue(undefined);
    const readMarkMock = vi.fn().mockResolvedValue(null);
    const parseFillsMock = vi.fn().mockReturnValue([
      {
        signature: "sigY",
        trader: "trader2",
        programId: PERC,
        sizeAbs: 42n,
        side: "short" as const,
        priceE6: undefined,
      },
    ]);

    vi.resetModules();
    vi.doMock("@percolatorct/shared", async (orig) => {
      const mod = await (orig() as Promise<any>);
      return { ...mod, insertTrade: insertTradeMock, insertOraclePrice: vi.fn() };
    });
    vi.doMock("../../src/parsers/markPrice.js", () => ({ readMarkPriceE6: readMarkMock }));
    vi.doMock("../../src/parsers/percolatorTxParser.js", () => ({ parsePercolatorFills: parseFillsMock }));

    const { EventStreamService } = await import("../../src/services/EventStreamService.js");

    const listeners: Array<(msg: any) => void> = [];
    const ws = {
      sub: () => {},
      onNotification: (cb: any) => { listeners.push(cb); },
      close: () => {},
      isOpen: true,
    };

    const svc = new EventStreamService({
      ws: ws as any,
      programId: PERC,
      connection: mockConn(),
      autoIndex: true,
      knownSlabs: [SLAB],
    });
    await svc.start();

    const fakeTx = {
      transaction: {
        message: {
          instructions: [],
          accountKeys: [{ pubkey: { toBase58: () => SLAB } }],
        },
      },
      meta: { err: null, logMessages: [] },
      signature: "sigY",
    };

    listeners[0]({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });
    await new Promise((r) => setTimeout(r, 20));

    expect(readMarkMock).toHaveBeenCalled();
    expect(insertTradeMock).not.toHaveBeenCalled();

    vi.doUnmock("@percolatorct/shared");
    vi.doUnmock("../../src/parsers/markPrice.js");
    vi.doUnmock("../../src/parsers/percolatorTxParser.js");
  });
});

describe("EventStreamService — UpdateHyperpMark oracle update (P2)", () => {
  it("writes oracle_prices when tx contains an UpdateHyperpMark ix (tag 34)", async () => {
    const insertOraclePriceMock = vi.fn().mockResolvedValue(undefined);
    const readMarkMock = vi.fn().mockResolvedValue(85_187_279);
    // No fills in this tx — only the oracle update.
    const parseFillsMock = vi.fn().mockReturnValue([]);
    // Decode returns a 1-byte buffer starting with tag 34.
    const decodeMock = vi.fn().mockReturnValue(new Uint8Array([34]));

    vi.resetModules();
    vi.doMock("@percolatorct/shared", async (orig) => {
      const mod = await (orig() as Promise<any>);
      return {
        ...mod,
        insertTrade: vi.fn(),
        insertOraclePrice: insertOraclePriceMock,
        decodeBase58: decodeMock,
      };
    });
    vi.doMock("../../src/parsers/markPrice.js", () => ({ readMarkPriceE6: readMarkMock }));
    vi.doMock("../../src/parsers/percolatorTxParser.js", () => ({ parsePercolatorFills: parseFillsMock }));

    const { EventStreamService } = await import("../../src/services/EventStreamService.js");

    const listeners: Array<(msg: any) => void> = [];
    const ws = {
      sub: () => {},
      onNotification: (cb: any) => { listeners.push(cb); },
      close: () => {},
      isOpen: true,
    };

    const svc = new EventStreamService({
      ws: ws as any,
      programId: PERC,
      connection: mockConn(),
      autoIndex: true,
      knownSlabs: [SLAB],
    });
    await svc.start();

    const fakeTx = {
      transaction: {
        message: {
          instructions: [
            {
              programId: PERC,
              accounts: [],
              data: "dummy-base58",
            },
          ],
          accountKeys: [{ pubkey: { toBase58: () => SLAB } }],
        },
      },
      meta: { err: null, logMessages: [] },
      signature: "sigMark",
    };

    listeners[0]({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });
    await new Promise((r) => setTimeout(r, 20));

    expect(readMarkMock).toHaveBeenCalled();
    expect(insertOraclePriceMock).toHaveBeenCalledWith(
      expect.objectContaining({
        slab_address: SLAB,
        price_e6: "85187279",
        tx_signature: "sigMark",
      }),
    );
    // Timestamp should be a positive epoch-seconds integer.
    const arg = insertOraclePriceMock.mock.calls[0][0];
    expect(typeof arg.timestamp).toBe("number");
    expect(arg.timestamp).toBeGreaterThan(1_600_000_000);

    vi.doUnmock("@percolatorct/shared");
    vi.doUnmock("../../src/parsers/markPrice.js");
    vi.doUnmock("../../src/parsers/percolatorTxParser.js");
  });

  it("does NOT write oracle_prices for non-UpdateHyperpMark txs", async () => {
    const insertOraclePriceMock = vi.fn().mockResolvedValue(undefined);
    const readMarkMock = vi.fn().mockResolvedValue(85_187_279);
    const parseFillsMock = vi.fn().mockReturnValue([]);
    // tag != 34 (e.g. tag 1 = InitMarket)
    const decodeMock = vi.fn().mockReturnValue(new Uint8Array([1]));

    vi.resetModules();
    vi.doMock("@percolatorct/shared", async (orig) => {
      const mod = await (orig() as Promise<any>);
      return {
        ...mod,
        insertTrade: vi.fn(),
        insertOraclePrice: insertOraclePriceMock,
        decodeBase58: decodeMock,
      };
    });
    vi.doMock("../../src/parsers/markPrice.js", () => ({ readMarkPriceE6: readMarkMock }));
    vi.doMock("../../src/parsers/percolatorTxParser.js", () => ({ parsePercolatorFills: parseFillsMock }));

    const { EventStreamService } = await import("../../src/services/EventStreamService.js");

    const listeners: Array<(msg: any) => void> = [];
    const ws = {
      sub: () => {},
      onNotification: (cb: any) => { listeners.push(cb); },
      close: () => {},
      isOpen: true,
    };

    const svc = new EventStreamService({
      ws: ws as any,
      programId: PERC,
      connection: mockConn(),
      autoIndex: true,
      knownSlabs: [SLAB],
    });
    await svc.start();

    const fakeTx = {
      transaction: {
        message: {
          instructions: [
            { programId: PERC, accounts: [], data: "dummy-base58" },
          ],
          accountKeys: [{ pubkey: { toBase58: () => SLAB } }],
        },
      },
      meta: { err: null, logMessages: [] },
      signature: "sigNotMark",
    };

    listeners[0]({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });
    await new Promise((r) => setTimeout(r, 20));

    expect(insertOraclePriceMock).not.toHaveBeenCalled();

    vi.doUnmock("@percolatorct/shared");
    vi.doUnmock("../../src/parsers/markPrice.js");
    vi.doUnmock("../../src/parsers/percolatorTxParser.js");
  });
});
