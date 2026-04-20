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

describe("EventStreamService", () => {
  it("subscribes to transactionSubscribe filtered by programId on start()", async () => {
    const { ws, subCalls } = mockWs();
    const svc = new EventStreamService({
      ws,
      programId: "PERC11111111111111111111111111111111111111",
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
