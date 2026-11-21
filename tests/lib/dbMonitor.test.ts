import { describe, it, expect, vi } from "vitest";
import { createDbMonitor, describeDbError } from "../../src/lib/dbMonitor.js";
import { createUnhandledRejectionHandler, describeRejection, installUnhandledRejectionPolicy } from "../../src/lib/unhandledRejection.js";

function monitor(probe: () => PromiseLike<{ error?: unknown }>, over: Partial<Parameters<typeof createDbMonitor>[0]> = {}) {
  const opts = {
    probe,
    timeoutMs: 50,
    sendLostAlert: vi.fn(async (_m: string) => {}),
    sendRestoredAlert: vi.fn(async () => {}),
    logError: vi.fn(),
    ...over,
  };
  return { m: createDbMonitor(opts), opts };
}

describe("describeDbError", () => {
  it("formats Error, PostgREST-shaped objects, primitives and hostile values", () => {
    expect(describeDbError(new Error("FAKE boom"))).toBe("FAKE boom");
    expect(describeDbError({ message: "FAKE fetch failed", code: "PGRST001" })).toBe("PGRST001: FAKE fetch failed");
    expect(describeDbError({ message: "FAKE only msg" })).toBe("FAKE only msg");
    expect(describeDbError({})).toBe("unknown error");
    expect(describeDbError("FAKE str")).toBe("FAKE str");
    const hostile = { get message() { throw new Error("getter"); } };
    expect(describeDbError(hostile)).toBe("unprintable error");
  });
});

describe("createDbMonitor", () => {
  it("treats a resolved { error } as lost, alerts once, then restores once", async () => {
    let res: { error: unknown } = { error: { message: "FAKE down" } };
    const { m, opts } = monitor(async () => res);
    await m.tick();
    await m.tick();
    expect(m.isLost()).toBe(true);
    expect(opts.sendLostAlert).toHaveBeenCalledTimes(1);
    expect(opts.sendLostAlert).toHaveBeenCalledWith("FAKE down");
    expect(opts.logError).toHaveBeenCalledTimes(2);
    res = { error: null };
    await m.tick();
    await m.tick();
    expect(m.isLost()).toBe(false);
    expect(opts.sendRestoredAlert).toHaveBeenCalledTimes(1);
  });

  it("times out a hung probe", async () => {
    const { m, opts } = monitor(() => new Promise(() => {}));
    await m.tick();
    expect(opts.sendLostAlert.mock.calls[0][0]).toMatch(/timed out after 50ms/);
  });

  it("treats a rejection with no reason as a failure", async () => {
    const { m, opts } = monitor(() => Promise.reject(undefined));
    await m.tick();
    expect(opts.sendLostAlert).toHaveBeenCalledWith("DB check rejected");
  });

  it("truncates the alert text to 200 chars", async () => {
    const { m, opts } = monitor(async () => ({ error: { message: "x".repeat(500) } }));
    await m.tick();
    expect(opts.sendLostAlert.mock.calls[0][0]).toHaveLength(200);
  });

  it("logs, never rejects, when either alert fails", async () => {
    let res: { error: unknown } = { error: new Error("FAKE down") };
    const { m, opts } = monitor(async () => res, {
      sendLostAlert: vi.fn(async () => { throw new Error("FAKE discord"); }),
      sendRestoredAlert: vi.fn(async () => { throw new Error("FAKE discord 2"); }),
    });
    await expect(m.tick()).resolves.toBeUndefined();
    res = { error: null };
    await expect(m.tick()).resolves.toBeUndefined();
    const msgs = opts.logError.mock.calls.map((c) => c[0]);
    expect(msgs).toEqual(["Failed to send DB connection lost alert", "DB connection check failed", "Failed to send DB restored alert"]);
  });

  it("tick() swallows a logger that throws", async () => {
    const { m } = monitor(async () => ({ error: "FAKE" }), { logError: () => { throw new Error("FAKE logger"); } });
    await expect(m.tick()).resolves.toBeUndefined();
  });
});

describe("unhandledRejection policy", () => {
  it("describeRejection handles Error, primitives and an unprintable reason", () => {
    expect(describeRejection(new Error("FAKE"))).toMatchObject({ message: "FAKE" });
    expect(describeRejection(42)).toEqual({ message: "42" });
    expect(describeRejection(Object.create(null))).toEqual({ message: "unprintable rejection reason" });
  });

  it("the handler never throws, even when the logger does", () => {
    const handler = createUnhandledRejectionHandler({ error: () => { throw new Error("FAKE logger"); } });
    expect(() => handler(new Error("FAKE"))).not.toThrow();
  });

  it("installs exactly one listener on the given process", () => {
    const on = vi.fn();
    const logger = { error: vi.fn() };
    const handler = installUnhandledRejectionPolicy({ on }, logger);
    expect(on).toHaveBeenCalledWith("unhandledRejection", handler);
    handler("FAKE reason");
    expect(logger.error).toHaveBeenCalledWith("Unhandled promise rejection (process stays up)", { message: "FAKE reason" });
  });
});
