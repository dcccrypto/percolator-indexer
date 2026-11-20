import { describe, it, expect, vi } from "vitest";
import { makeMatcherContextReader, newContextReadMemo } from "../src/lib/matcherCtx.js";

const A = "11111111111111111111111111111111";
const B = "SysvarRent111111111111111111111111111111111";
const ctxBytes = (req: bigint) => { const b = Buffer.alloc(320); b.writeUInt32LE(3, 0); b.writeUInt32LE(1, 4); b.writeBigUInt64LE(req, 32); return b; };

describe("matcher context reader memo (per delivery)", () => {
  it("reuses a read answered at a slot >= the tx's slot, per address", async () => {
    const fn = vi.fn(async (pk: any) => ({ context: { slot: 500 }, value: { data: ctxBytes(pk.toBase58() === A ? 7n : 8n) } }));
    const getConn = () => ({ getAccountInfoAndContext: fn, getAccountInfo: vi.fn() }) as any;
    const memo = newContextReadMemo();
    const r1 = await makeMatcherContextReader(getConn, 100, { memo })(A);
    const r2 = await makeMatcherContextReader(getConn, 500, { memo })(A);
    const r3 = await makeMatcherContextReader(getConn, 200, { memo })(B);
    expect([r1, r2, r3].map((r) => (r as any).ret.reqId)).toEqual([7n, 7n, 8n]);
    expect(fn).toHaveBeenCalledTimes(2); // A once, B once
    expect((fn.mock.calls[0] as any[])[1]).toEqual({ commitment: "confirmed", minContextSlot: 100 });
    // a tx past the answered slot reads again
    await makeMatcherContextReader(getConn, 501, { memo })(A);
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it("a missing account (null) is memoised too; an error is not", async () => {
    const fn = vi.fn().mockRejectedValueOnce(new Error("429")).mockResolvedValue({ context: { slot: 10 }, value: null });
    const getConn = () => ({ getAccountInfoAndContext: fn }) as any;
    const memo = newContextReadMemo();
    expect(await makeMatcherContextReader(getConn, 5, { memo })(A)).toEqual({ kind: "error", detail: "429" });
    expect(await makeMatcherContextReader(getConn, 5, { memo })(A)).toEqual({ kind: "ok", ret: null });
    expect(await makeMatcherContextReader(getConn, 6, { memo })(A)).toEqual({ kind: "ok", ret: null });
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it("a passed deadline still answers 'deadline exceeded' even with a memo hit (same as without the memo)", async () => {
    const fn = vi.fn(async () => ({ context: { slot: 10 }, value: null }));
    const memo = newContextReadMemo();
    await makeMatcherContextReader(() => ({ getAccountInfoAndContext: fn }) as any, 5, { memo })(A);
    expect(await makeMatcherContextReader(() => ({ getAccountInfoAndContext: fn }) as any, 5, { memo, deadlineAt: Date.now() - 1 })(A)).toEqual({ kind: "error", detail: "deadline exceeded" });
  });

  it("no tx slot: never reused; without a memo the plain getAccountInfo path is unchanged", async () => {
    const fn = vi.fn(async () => ({ context: { slot: 10 }, value: null }));
    const memo = newContextReadMemo();
    await makeMatcherContextReader(() => ({ getAccountInfoAndContext: fn }) as any, null, { memo })(A);
    await makeMatcherContextReader(() => ({ getAccountInfoAndContext: fn }) as any, null, { memo })(A);
    expect(fn).toHaveBeenCalledTimes(2);
    const plain = vi.fn(async () => null);
    const both = vi.fn();
    await makeMatcherContextReader(() => ({ getAccountInfo: plain, getAccountInfoAndContext: both }) as any, 5)(A);
    expect(plain).toHaveBeenCalledTimes(1);
    expect(both).not.toHaveBeenCalled();
  });

  it("a connection without getAccountInfoAndContext: the answer counts as taken at minContextSlot", async () => {
    const fn = vi.fn(async () => ({ data: ctxBytes(9n) }));
    const memo = newContextReadMemo();
    const rd = (s: number) => makeMatcherContextReader(() => ({ getAccountInfo: fn }) as any, s, { memo })(A);
    await rd(20);
    await rd(20); // same slot: reused
    await rd(15); // earlier tx: reused
    expect(fn).toHaveBeenCalledTimes(1);
    await rd(21); // later tx: fresh read, and the memo moves forward
    await rd(20);
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it("an answer at an older slot does not replace a newer one", async () => {
    const fn = vi.fn()
      .mockResolvedValueOnce({ context: { slot: 30 }, value: { data: ctxBytes(1n) } })
      .mockResolvedValueOnce({ context: { slot: 25 }, value: { data: ctxBytes(2n) } });
    const getConn = () => ({ getAccountInfoAndContext: fn }) as any;
    const memo = newContextReadMemo();
    await makeMatcherContextReader(getConn, 30, { memo })(A);
    // force a second read of A for a tx at 31 that a lagging node answers at 25 (minContextSlot is the node's job)
    await makeMatcherContextReader(getConn, 31, { memo })(A);
    expect(memo.get(A)!.slot).toBe(30);
  });
});
