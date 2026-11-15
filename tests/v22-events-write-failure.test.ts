/**
 * IDX-V22EVFIX: a failed v22_events DB write used to be swallowed by insertV22Events, so the webhook answered 200 and the
 * event stream carried on; the provider never redelivered and the v2.2 events were lost silently. Only the DB is mocked
 * (it fails); the route, the stream handler, insertV22Events and recordV22LogEvents are real.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { Keypair } from "@solana/web3.js";
import { encodeBase58 } from "../src/lib/base58.js";

const PROGRAM = vi.hoisted(() => "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB");
const db = vi.hoisted(() => ({ upsert: vi.fn(), error: { code: "XX000", message: "connection reset" } as { code?: string; message: string } | null }));

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: [PROGRAM], webhookSecret: "s3cret" },
    eventBus: { publish: vi.fn(), emit: vi.fn(), on: vi.fn() },
    getConnection: vi.fn(() => ({ getAccountInfo: vi.fn(async () => null) })),
    getNetwork: vi.fn(() => "devnet"),
    getSupabase: vi.fn(() => ({ from: () => ({ upsert: (...a: unknown[]) => { db.upsert(...a); return Promise.resolve({ error: db.error }); } }) })),
    withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    captureException: vi.fn(),
  };
});
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined) }));
vi.mock("../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true), insertTradeRows: vi.fn(async () => []), tradeKey: () => "k" }));

import { webhookRoutes } from "../src/routes/webhook.js";
import { EventStreamService } from "../src/services/EventStreamService.js";
import { getV22EventStats, resetV22EventStats, V22EventWriteError, insertV22Events } from "../src/db/insertV22Events.js";

const SLAB = Keypair.generate().publicKey.toBase58();
const U = Keypair.generate().publicKey.toBase58();
// InitInsuranceUnits (tag 116): a non-fill v2.2 event the instruction decoder records.
const ixData = encodeBase58(new Uint8Array([116]));
const enhancedTx = { signature: "SIGWEBHOOK", slot: 5, timestamp: 1_790_000_000, instructions: [{ programId: PROGRAM, accounts: [U, SLAB, U, U, U, U, U, U], data: ixData, innerInstructions: [] }] };

async function deliver() {
  const app = webhookRoutes();
  return app.fetch(new Request("http://x/webhook/trades", { method: "POST", headers: { "content-type": "application/json", authorization: "s3cret" }, body: JSON.stringify([enhancedTx]) }));
}

beforeEach(() => { db.upsert.mockClear(); db.error = { code: "XX000", message: "connection reset" }; resetV22EventStats(); });

describe("webhook: v22_events write failure", () => {
  it("success is unchanged: 200 and the rows are written", async () => {
    db.error = null;
    const res = await deliver();
    expect(res.status).toBe(200);
    expect(db.upsert).toHaveBeenCalled();
    expect(getV22EventStats()).toMatchObject({ written: 1, writeFailed: 0 });
  });

  it("failed write: non-2xx so the provider redelivers, and the failure is counted", async () => {
    const res = await deliver();
    expect(db.upsert).toHaveBeenCalled(); // the write was attempted and failed
    expect(res.status).toBeGreaterThanOrEqual(500); // BEFORE the fix: 200 (events lost)
    expect(getV22EventStats().writeFailed).toBeGreaterThan(0);
  });

  it("permanent 'table missing' is still a counted drop, not a redelivery loop", async () => {
    db.error = { code: "42P01", message: 'relation "v22_events" does not exist' };
    const res = await deliver();
    expect(res.status).toBe(200);
    expect(getV22EventStats()).toMatchObject({ droppedTableMissing: 1, writeFailed: 0 });
  });
});

describe("event stream: v22_events write failure", () => {
  const parsed = {
    signature: "SIGSTREAM", slot: 9, blockTime: 1_790_000_000,
    transaction: { signatures: ["SIGSTREAM"], message: { instructions: [{ programId: PROGRAM, accounts: [U, SLAB, U, U, U, U, U, U], data: ixData }] } },
    meta: { err: null, logMessages: null, innerInstructions: [] },
  };
  const svc = () => new EventStreamService({ ws: { sub: () => {}, onNotification: () => {}, close: () => {}, isOpen: true } as any, programId: PROGRAM, connection: { getAccountInfo: vi.fn(async () => null) } as any, autoIndex: true, knownSlabs: [SLAB] });
  const msg = { jsonrpc: "2.0", method: "transactionNotification", params: { result: parsed, subscription: 1 } };

  it("retries the write and then fails the handler (no silent success)", async () => {
    const err = await (svc() as any).handle(msg).then(() => null, (e: unknown) => e); // BEFORE the fix: resolves, 1 attempt
    expect(db.upsert.mock.calls.length).toBeGreaterThanOrEqual(3);
    expect(String(err)).toMatch(/v22_events write failed/);
  });

  it("a transient failure that recovers on retry succeeds with no error", async () => {
    let n = 0;
    db.upsert.mockImplementation(() => { n++; db.error = n < 2 ? { code: "XX000", message: "blip" } : null; });
    const err = await (svc() as any).handle(msg).then(() => null, (e: unknown) => e);
    expect(err).toBeNull();
    expect(n).toBe(2);
    db.upsert.mockReset();
  });
});

describe("insertV22Events", () => {
  it("throws V22EventWriteError on a failed write", async () => {
    await expect(insertV22Events([{ signature: "s", ix_index: 0, inner_index: -1, kind: "x" } as any])).rejects.toBeInstanceOf(V22EventWriteError);
  });
});
