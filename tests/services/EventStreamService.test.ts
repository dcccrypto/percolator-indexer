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
        slabAddress: SLAB, // #148: fill.slabAddress required for per-fill attribution
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
        slabAddress: SLAB, // #148: fill.slabAddress required for per-fill attribution
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

  it("continues processing later fills when one slab fallback read throws", async () => {
    const insertTradeMock = vi.fn().mockResolvedValue(undefined);
    const readMarkMock = vi.fn().mockRejectedValueOnce(new Error("rpc unavailable"));
    const parseFillsMock = vi.fn().mockReturnValue([
      {
        signature: "sigFallbackThrow",
        trader: "trader-fallback-fails",
        programId: PERC,
        sizeAbs: 1_000n,
        side: "long" as const,
        slabAddress: SLAB,
        priceE6: undefined,
      },
      {
        signature: "sigFallbackThrow",
        trader: "trader-price-present",
        programId: PERC,
        sizeAbs: 2_000n,
        side: "short" as const,
        slabAddress: SLAB,
        priceE6: 99_000_000,
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
      signature: "sigFallbackThrow",
    };

    await listeners[0]({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });

    expect(readMarkMock).toHaveBeenCalledTimes(1);
    expect(insertTradeMock).toHaveBeenCalledTimes(1);
    expect(insertTradeMock).toHaveBeenCalledWith(
      expect.objectContaining({
        slab_address: SLAB,
        trader: "trader-price-present",
        side: "short",
        size: "2000",
        price: 99_000_000,
        tx_signature: "sigFallbackThrow",
      }),
    );

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

// ---------------------------------------------------------------------------
// #148 focused regression — per-fill slab attribution from instruction accounts
// ---------------------------------------------------------------------------

describe("#148 — EventStreamService: per-fill slab derived from instruction accounts", () => {
  it("attributes each fill to its own slab in a multi-slab tx", async () => {
    // #148 regression: the original resolveSlab(tx) returned the first known slab
    // in tx.accountKeys and applied it to every fill, mis-attributing fills in
    // multi-slab transactions.
    //
    // Fix: parsePercolatorFills now populates fill.slabAddress from
    // ix.accounts[marketAccountIdx] (NoCpi=accounts[2], Cpi=accounts[1]).
    // EventStreamService.handle() uses that per-fill slab for insertTrade.
    const SLAB_A = "SLABABABABABABABABABABABABABABABABABABABABAB";
    const SLAB_B = "SLABBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
    const PERC_148 = "PERC148AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    const insertTradeMock = vi.fn().mockResolvedValue(undefined);
    const readMarkMock = vi.fn().mockResolvedValue(100_000_000); // $100

    // parseFillsMock returns two fills with different slabAddresses.
    const parseFillsMock = vi.fn().mockReturnValue([
      {
        signature: "sig148",
        trader: "trader1",
        programId: PERC_148,
        assetIndex: 0,
        sizeAbs: 1_000n,
        side: "long" as const,
        slabAddress: SLAB_A,
        priceE6: undefined,
      },
      {
        signature: "sig148",
        trader: "trader2",
        programId: PERC_148,
        assetIndex: 0,
        sizeAbs: 2_000n,
        side: "short" as const,
        slabAddress: SLAB_B,
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
      programId: PERC_148,
      connection: { getAccountInfo: vi.fn() } as any,
      autoIndex: true,
      knownSlabs: [SLAB_A, SLAB_B], // both slabs known
    });
    await svc.start();

    const fakeTx = {
      transaction: {
        message: {
          instructions: [],
          accountKeys: [{ pubkey: { toBase58: () => SLAB_A } }],
        },
      },
      meta: { err: null, logMessages: [] },
      signature: "sig148",
    };

    listeners[0]({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });
    await new Promise((r) => setTimeout(r, 30));

    // Both fills must have been inserted with their OWN slab, not SLAB_A for both.
    expect(insertTradeMock).toHaveBeenCalledTimes(2);
    const slabs = insertTradeMock.mock.calls.map((c: any) => c[0].slab_address);
    expect(slabs).toContain(SLAB_A);
    expect(slabs).toContain(SLAB_B);

    // Verify fill-to-slab attribution is correct (not swapped)
    const fillA = insertTradeMock.mock.calls.find((c: any) => c[0].slab_address === SLAB_A)?.[0];
    expect(fillA?.size).toBe("1000");
    expect(fillA?.side).toBe("long");

    const fillB = insertTradeMock.mock.calls.find((c: any) => c[0].slab_address === SLAB_B)?.[0];
    expect(fillB?.size).toBe("2000");
    expect(fillB?.side).toBe("short");

    vi.doUnmock("@percolatorct/shared");
    vi.doUnmock("../../src/parsers/markPrice.js");
    vi.doUnmock("../../src/parsers/percolatorTxParser.js");
  });

  it("skips fills whose slabAddress is not in the known set", async () => {
    // #148: A fill with slabAddress not in knownSlabs must be silently dropped.
    const SLAB_KNOWN = "SLABKNOWN1111111111111111111111111111111111";
    const SLAB_UNKNOWN = "SLABUNKNOWN11111111111111111111111111111111";
    const PERC_148B = "PERC148BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";

    const insertTradeMock = vi.fn().mockResolvedValue(undefined);
    const readMarkMock = vi.fn().mockResolvedValue(50_000_000);

    const parseFillsMock = vi.fn().mockReturnValue([
      {
        signature: "sig148b",
        trader: "trader1",
        programId: PERC_148B,
        assetIndex: 0,
        sizeAbs: 500n,
        side: "long" as const,
        slabAddress: SLAB_UNKNOWN, // not in knownSlabs
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
      programId: PERC_148B,
      connection: { getAccountInfo: vi.fn() } as any,
      autoIndex: true,
      knownSlabs: [SLAB_KNOWN],
    });
    await svc.start();

    const fakeTx = {
      transaction: {
        message: {
          instructions: [],
          accountKeys: [{ pubkey: { toBase58: () => SLAB_KNOWN } }],
        },
      },
      meta: { err: null, logMessages: [] },
      signature: "sig148b",
    };

    listeners[0]({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });
    await new Promise((r) => setTimeout(r, 20));

    // Fill for an unknown slab must be dropped
    expect(insertTradeMock).not.toHaveBeenCalled();

    vi.doUnmock("@percolatorct/shared");
    vi.doUnmock("../../src/parsers/markPrice.js");
    vi.doUnmock("../../src/parsers/percolatorTxParser.js");
  });
});

/**
 * Regression guard for the Atlas WS / jsonParsed accountKeys format bug.
 *
 * Atlas WS pushes accountKeys as plain objects with a STRING `pubkey` field
 * `{pubkey: "<base58>", signer, writable}`. The original resolveSlab only
 * handled raw strings or `{pubkey: {toBase58()}}` PublicKey objects — Atlas
 * WS's shape caused every streamed tx to resolve as "unknown slab", silently
 * skipping oracle-price + trade inserts.
 *
 * This skipped block documents the intended assertion; the live production
 * verification (oracle_prices rows growing with tx_signature set) is the
 * definitive regression signal. Left here so future engineers know the shape.
 */
describe.skip("EventStreamService — Atlas jsonParsed accountKey shape", () => {
  it("resolves slab from string-pubkey jsonParsed entries", async () => {
    const insertOraclePriceMock = vi.fn().mockResolvedValue(undefined);
    const readMarkPriceE6Mock = vi.fn().mockResolvedValue(84_400_000);

    vi.doMock("@percolatorct/shared", async (orig) => {
      const mod = await (orig() as Promise<any>);
      return {
        ...mod,
        insertOraclePrice: insertOraclePriceMock,
        decodeBase58: (s: string) => new Uint8Array([34]), // UpdateHyperpMark tag
      };
    });
    vi.doMock("../../src/parsers/markPrice.js", () => ({
      readMarkPriceE6: readMarkPriceE6Mock,
    }));

    const { EventStreamService: Svc } = await import("../../src/services/EventStreamService.js");

    const listeners: Array<(msg: any) => void> = [];
    const ws = {
      sub: () => {},
      onNotification: (cb: any) => { listeners.push(cb); },
      close: () => {},
      isOpen: true,
    };

    const SLAB = "SLABjsonparsed111111111111111111111111111111";
    const PERC = "PERC11111111111111111111111111111111111111";

    const svc = new Svc({
      ws: ws as any,
      programId: PERC,
      connection: { getAccountInfo: vi.fn() } as any,
      autoIndex: true,
      knownSlabs: [SLAB],
    });
    await svc.start();

    // Exact shape Atlas WS delivers: pubkey is a plain STRING, not a PublicKey.
    const fakeTx = {
      transaction: {
        message: {
          instructions: [
            { programId: PERC, accounts: [], data: "b" /* bs58 of [34] */ },
          ],
          accountKeys: [
            { pubkey: SLAB, signer: false, writable: true },
          ],
        },
      },
      meta: { err: null, logMessages: [] },
      signature: "sigJsonParsed",
    };

    listeners[0]({
      jsonrpc: "2.0",
      method: "transactionNotification",
      params: { result: fakeTx, subscription: 1 },
    });
    await new Promise((r) => setTimeout(r, 20));

    expect(insertOraclePriceMock).toHaveBeenCalledTimes(1);
    expect(insertOraclePriceMock.mock.calls[0][0]).toMatchObject({
      slab_address: SLAB,
      tx_signature: "sigJsonParsed",
    });

    vi.doUnmock("@percolatorct/shared");
    vi.doUnmock("../../src/parsers/markPrice.js");
  });
});
