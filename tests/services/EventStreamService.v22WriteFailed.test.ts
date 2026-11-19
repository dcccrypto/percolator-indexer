/**
 * (report J-20261119-IDX-REPORT-SUPABASEERRORUNCHECKED, hit H2): the Atlas stream path loses v2.2 INSTRUCTION
 * events when the `v22_events` upsert keeps returning { error }. insertV22EventsWithRetry retries 3x, then the handler
 * throws and EventStreamService logs "handler failed". There is no redelivery on a websocket stream, the poll path
 * (TradeIndexer) never writes instruction events, and nothing is written to skipped_signatures (the trade path at
 * EventStreamService.ts:337 does record its failures). Expected (fails today): the signature is recorded for re-index.
 */
import { describe, expect, it, vi } from "vitest";
import { Keypair } from "@solana/web3.js";
import { encodeBase58 } from "../../src/lib/base58.js";

const PROGRAM = vi.hoisted(() => "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB");
const db = vi.hoisted(() => ({ upsert: vi.fn() }));
const { recordSkippedSignatures } = vi.hoisted(() => ({ recordSkippedSignatures: vi.fn(async () => undefined) }));

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: [PROGRAM] },
    eventBus: { publish: vi.fn(), emit: vi.fn(), on: vi.fn() },
    getConnection: vi.fn(() => ({ getAccountInfo: vi.fn(async () => null) })),
    getNetwork: vi.fn(() => "devnet"),
    getSupabase: vi.fn(() => ({ from: () => ({ upsert: (...a: unknown[]) => { db.upsert(...a); return Promise.resolve({ error: { code: "XX000", message: "connection reset" } }); } }) })),
    withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    captureException: vi.fn(),
  };
});
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures }));
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true), insertTradeRows: vi.fn(async () => []), tradeKey: () => "k" }));

import { EventStreamService } from "../../src/services/EventStreamService.js";

const SLAB = Keypair.generate().publicKey.toBase58();
const U = Keypair.generate().publicKey.toBase58();
const ixData = encodeBase58(new Uint8Array([116])); // InitInsuranceUnits: a non-fill v2.2 instruction event

describe("H2: stream path, v22_events upsert returns { error } on every retry", () => {
  it("records the signature for re-index instead of only logging 'handler failed'", async () => {
    const parsed = {
      signature: "SIGSTREAM", slot: 9, blockTime: 1_790_000_000,
      transaction: { signatures: ["SIGSTREAM"], message: { instructions: [{ programId: PROGRAM, accounts: [U, SLAB, U, U, U, U, U, U], data: ixData }] } },
      meta: { err: null, logMessages: null, innerInstructions: [] },
    };
    const svc = new EventStreamService({ ws: { sub: () => {}, onNotification: () => {}, close: () => {}, isOpen: true } as any, programId: PROGRAM, connection: { getAccountInfo: vi.fn(async () => null) } as any, autoIndex: true, knownSlabs: [SLAB] });
    await (svc as any).handle({ jsonrpc: "2.0", method: "transactionNotification", params: { result: parsed, subscription: 1 } }).catch(() => undefined);
    expect(db.upsert.mock.calls.length).toBeGreaterThanOrEqual(3); // retries exhausted, every one returned { error }
    // FAILS today: nothing durable names the lost signature.
    expect(recordSkippedSignatures).toHaveBeenCalledWith(expect.arrayContaining([expect.objectContaining({ signature: "SIGSTREAM" })]));
  });
});
