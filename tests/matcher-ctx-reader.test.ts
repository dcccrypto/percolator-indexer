import { describe, it, expect, vi } from "vitest";
import { makeMatcherContextReader } from "../src/lib/matcherCtx.js";

const ADDR = "11111111111111111111111111111111";
const conn = (fn: any) => () => ({ getAccountInfo: fn }) as any;
const ctxBytes = () => { const b = Buffer.alloc(320); b.writeUInt32LE(3, 0); b.writeUInt32LE(1, 4); b.writeBigUInt64LE(7n, 32); return b; };

describe("matcher context reader: transport error vs 'read fine'", () => {
  it("a missing account or a readable one is a definite answer (ok)", async () => {
    expect(await makeMatcherContextReader(conn(async () => null))(ADDR)).toEqual({ kind: "ok", ret: null });
    const r = await makeMatcherContextReader(conn(async () => ({ data: ctxBytes() })))(ADDR);
    expect(r.kind).toBe("ok");
    expect((r as any).ret.reqId).toBe(7n);
  });
  it("an RPC error is an error, not 'not matched'", async () => {
    expect(await makeMatcherContextReader(conn(async () => { throw new Error("429"); }))(ADDR)).toEqual({ kind: "error", detail: "429" });
  });
  it("a hung call times out per call", async () => {
    const r = await makeMatcherContextReader(conn(() => new Promise(() => {})), null, { timeoutMs: 30 })(ADDR);
    expect(r).toMatchObject({ kind: "error", detail: expect.stringContaining("timeout") });
  });
  it("retries transport errors, then succeeds", async () => {
    const fn = vi.fn().mockRejectedValueOnce(new Error("x")).mockResolvedValue({ data: ctxBytes() });
    const r = await makeMatcherContextReader(conn(fn), null, { retries: 2, retryDelayMs: 1 })(ADDR);
    expect(r.kind).toBe("ok");
    expect(fn).toHaveBeenCalledTimes(2);
  });
  it("a passed per-delivery deadline stops reads without calling the RPC", async () => {
    const fn = vi.fn();
    const r = await makeMatcherContextReader(conn(fn), null, { deadlineAt: Date.now() - 1 })(ADDR);
    expect(r).toMatchObject({ kind: "error", detail: "deadline exceeded" });
    expect(fn).not.toHaveBeenCalled();
  });
  it("minContextSlot is passed through", async () => {
    const fn = vi.fn(async () => null);
    await makeMatcherContextReader(conn(fn), 123)(ADDR);
    expect((fn.mock.calls[0] as any[])[1]).toMatchObject({ minContextSlot: 123 });
  });
});
