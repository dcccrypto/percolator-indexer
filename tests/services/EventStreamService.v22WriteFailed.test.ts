/**
 * (report J-20261119-IDX-REPORT-SUPABASEERRORUNCHECKED, hit H2): the Atlas stream path loses v2.2 INSTRUCTION
 * events when the `v22_events` upsert keeps returning { error }. insertV22EventsWithRetry retries 3x, then the handler
 * throws and EventStreamService logs "handler failed". There is no redelivery on a websocket stream and the poll path
 * (TradeIndexer) never writes instruction events, so the signature must be recorded in skipped_signatures.
 *
 * #383 review: the record uses its OWN source ("v22-events"). recordSkippedSignatures dedupes on (source, signature) in
 * memory and in the table (`onConflict: "signature,source"`, ignoreDuplicates), so under "trade-indexer" it was silently
 * dropped whenever the trade path had already recorded the same signature (e.g. a TradeCpi skip in indexFills).
 * This file runs the REAL recordSkippedSignatures against a mocked Supabase so that dedupe is exercised.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { Keypair } from "@solana/web3.js";
import { encodeBase58 } from "../../src/lib/base58.js";

const PROGRAM = vi.hoisted(() => "ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB");
/** Upserts per table. v22_events always fails; skipped_signatures succeeds. */
const db = vi.hoisted(() => ({ v22: vi.fn(), skipped: vi.fn() }));

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: [PROGRAM] },
    createLogger: () => ({ info: vi.fn(), warn: vi.fn(), debug: vi.fn(), error: vi.fn() }),
    eventBus: { publish: vi.fn(), emit: vi.fn(), on: vi.fn() },
    getConnection: vi.fn(() => ({ getAccountInfo: vi.fn(async () => null) })),
    getNetwork: vi.fn(() => "devnet"),
    getSupabase: vi.fn(() => ({
      from: (table: string) => ({
        upsert: (...a: unknown[]) => {
          if (table === "skipped_signatures") {
            db.skipped(...a);
            return Promise.resolve({ error: null });
          }
          db.v22(...a);
          return Promise.resolve({ error: { code: "XX000", message: "connection reset" } });
        },
      }),
    })),
    withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    captureException: vi.fn(),
  };
});
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true), insertTradeRows: vi.fn(async () => []), tradeKey: () => "k" }));

import { EventStreamService } from "../../src/services/EventStreamService.js";
import { recordSkippedSignatures, resetSkippedSignatureDedupe } from "../../src/lib/skippedSignatures.js";

const SLAB = Keypair.generate().publicKey.toBase58();
const U = Keypair.generate().publicKey.toBase58();
const ixData = encodeBase58(new Uint8Array([116])); // InitInsuranceUnits: a non-fill v2.2 instruction event

function run(sig: string): Promise<unknown> {
  const parsed = {
    signature: sig, slot: 9, blockTime: 1_790_000_000,
    transaction: { signatures: [sig], message: { instructions: [{ programId: PROGRAM, accounts: [U, SLAB, U, U, U, U, U, U], data: ixData }] } },
    meta: { err: null, logMessages: null, innerInstructions: [] },
  };
  const svc = new EventStreamService({ ws: { sub: () => {}, onNotification: () => {}, close: () => {}, isOpen: true } as any, programId: PROGRAM, connection: { getAccountInfo: vi.fn(async () => null) } as any, autoIndex: true, knownSlabs: [SLAB] });
  return (svc as any).handle({ jsonrpc: "2.0", method: "transactionNotification", params: { result: parsed, subscription: 1 } });
}
/** All rows written to skipped_signatures. */
const skippedRows = (): Array<Record<string, unknown>> => db.skipped.mock.calls.flatMap((c) => c[0] as Array<Record<string, unknown>>);

describe("H2: stream path, v22_events upsert returns { error } on every retry", { timeout: 60_000 }, () => {
  beforeEach(() => { db.v22.mockClear(); db.skipped.mockClear(); resetSkippedSignatureDedupe(); });

  it("still throws (handler failed), and records the signature under 'v22-events' with its slab and an accurate error", async () => {
    await expect(run("SIGSTREAM")).rejects.toThrow(/v22 instruction events \(1\) write failed after retries for SIGSTREAM/);
    expect(db.v22.mock.calls.length).toBeGreaterThanOrEqual(3); // retries exhausted, every one returned { error }
    expect(skippedRows()).toEqual([
      expect.objectContaining({ signature: "SIGSTREAM", source: "v22-events", slab: SLAB, error: "v22 instruction events (1) write failed after retries" }),
    ]);
  });

  it("MEDIUM: lands even when the trade path already recorded the same signature under 'trade-indexer'", async () => {
    // What indexFills does for a TradeCpi skip on the same tx (EventStreamService indexFills -> recordSkippedSignatures).
    await recordSkippedSignatures([{ signature: "SIGBOTH", source: "trade-indexer", slab: SLAB, error: "TradeCpi lot_exp unreadable" }]);
    expect(skippedRows()).toHaveLength(1);
    await expect(run("SIGBOTH")).rejects.toThrow();
    // Before the fix the v22 record reused "trade-indexer" and was dropped by the (source, signature) dedupe: only 1 row.
    expect(skippedRows()).toEqual([
      expect.objectContaining({ signature: "SIGBOTH", source: "trade-indexer" }),
      expect.objectContaining({ signature: "SIGBOTH", source: "v22-events", slab: SLAB }),
    ]);
  });
});
