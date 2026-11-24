/**
 * Mutation killers for the stream-side v22_events write-failed path in EventStreamService.handle()
 * (report IDX-REPORT-ESSWRITEFAILEDMUT, measured at 9db52dd). Survivors of v22WriteFailed + lastBranches killed here:
 *  - E20 / E22: a tx whose instruction-event write AND log-event write both fail. The record must name both failures
 *    (one row, error joined with "; ") and carry the first named slab (the instruction events' slab).
 *  - E13: the record must be durable BEFORE the handler rejects (a non-awaited record races the failure).
 *  - E14: if recordSkippedSignatures ever throws, the handler must still reject (never resolve on a v22 write failure).
 *  - E03: the stream retries the instruction write exactly 3 times (bounded; the pinned test only says >= 3).
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { Keypair } from "@solana/web3.js";
import { encodeBase58 } from "../../src/lib/base58.js";

const PROGRAM = vi.hoisted(() => "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB");
const st = vi.hoisted(() => ({
  logStatus: "ok" as string,
  v22Calls: 0,
  recordDone: false,
  recordMode: "ok" as "ok" | "slow" | "throw",
}));
const recordSkipped = vi.hoisted(() => vi.fn());

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: [PROGRAM] },
    createLogger: () => ({ info: vi.fn(), warn: vi.fn(), debug: vi.fn(), error: vi.fn() }),
    eventBus: { publish: vi.fn(), emit: vi.fn(), on: vi.fn() },
    getNetwork: vi.fn(() => "devnet"),
    getSupabase: vi.fn(() => ({
      from: () => ({
        upsert: () => { st.v22Calls++; return Promise.resolve({ error: { code: "XX000", message: "connection reset" } }); },
      }),
    })),
    withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    captureException: vi.fn(),
  };
});
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRows: vi.fn(async () => []), tradeKey: () => "k" }));
vi.mock("../../src/services/v22LogEvents.js", () => ({
  recordV22LogEvents: vi.fn(async () => ({ status: st.logStatus, rows: 2, slab: "LOGSLAB" })),
}));
vi.mock("../../src/lib/skippedSignatures.js", () => ({
  recordSkippedSignatures: (...a: unknown[]) => {
    recordSkipped(...a);
    if (st.recordMode === "throw") return Promise.reject(new Error("record sink down"));
    if (st.recordMode === "slow") return new Promise<void>((r) => setTimeout(() => { st.recordDone = true; r(); }, 20));
    st.recordDone = true;
    return Promise.resolve();
  },
}));

import { EventStreamService } from "../../src/services/EventStreamService.js";

const SLAB = Keypair.generate().publicKey.toBase58();
const U = Keypair.generate().publicKey.toBase58();
const ixData = encodeBase58(new Uint8Array([116])); // InitInsuranceUnits: one non-fill v2.2 instruction event on SLAB

function run(sig: string): Promise<unknown> {
  const parsed = {
    signature: sig, slot: 9, blockTime: 1_790_000_000,
    transaction: { signatures: [sig], message: { instructions: [{ programId: PROGRAM, accounts: [U, SLAB, U, U, U, U, U, U], data: ixData }] } },
    meta: { err: null, logMessages: null, innerInstructions: [] },
  };
  const svc = new EventStreamService({ ws: { sub: () => {}, onNotification: () => {}, close: () => {}, isOpen: true } as any, programId: PROGRAM, connection: { getAccountInfo: vi.fn(async () => null) } as any, autoIndex: true, knownSlabs: [SLAB] });
  return (svc as any).handle({ jsonrpc: "2.0", method: "transactionNotification", params: { result: parsed, subscription: 1 } });
}

describe("stream v22 write-failed killers", { timeout: 60_000 }, () => {
  beforeEach(() => { st.logStatus = "ok"; st.v22Calls = 0; st.recordDone = false; st.recordMode = "ok"; recordSkipped.mockClear(); });

  it("E03: the instruction-event write is tried exactly 3 times", async () => {
    await expect(run("SIGRETRY")).rejects.toThrow();
    expect(st.v22Calls).toBe(3);
  });

  it("E20/E22: instruction AND log writes fail -> one record naming both, slab = the instruction events' slab", async () => {
    st.logStatus = "write_failed";
    await expect(run("SIGTWO")).rejects.toThrow(
      "v22 instruction events (1) write failed after retries; v22 log events (2) write failed (not retried) for SIGTWO",
    );
    expect(recordSkipped).toHaveBeenCalledTimes(1);
    expect(recordSkipped).toHaveBeenCalledWith([{
      signature: "SIGTWO", source: "v22-events", slab: SLAB,
      error: "v22 instruction events (1) write failed after retries; v22 log events (2) write failed (not retried)",
    }]);
  });

  it("E13: the record is durable before the handler rejects", async () => {
    st.recordMode = "slow";
    const err = await run("SIGSLOW").then(() => null, (e: unknown) => e);
    expect(err).toBeInstanceOf(Error);
    expect(st.recordDone).toBe(true);
  });

  it("E14: a throwing record sink still leaves the handler rejecting", async () => {
    st.recordMode = "throw";
    await expect(run("SIGSINK")).rejects.toThrow(/v22 instruction events \(1\) write failed after retries for SIGSINK/);
    expect(recordSkipped).toHaveBeenCalledTimes(1);
  });
});
