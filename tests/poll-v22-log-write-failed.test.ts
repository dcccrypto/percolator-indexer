/**
 * Regression (report J-20261119-IDX-REPORT-SUPABASEERRORUNCHECKED, hit H1): the poll path drops a v2.2 log-event write failure.
 *
 * supabase-js returns { error } on a failed `v22_events` upsert; insertV22Events turns it into V22EventWriteError and
 * recordV22LogEvents reports status "write_failed". TradeIndexer.processTransaction ignores that status, so the pass is
 * not marked failed and the slab cursor commits past the signature: the fill / reduce / move rows are never retried.
 * Expected: the signature is marked failed in the pass (cursor held), as a failed trade write is.
 */
import { describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { Keypair, PublicKey } from "@solana/web3.js";
import { encodeBase58 } from "../src/lib/base58.js";

const W = Keypair.generate().publicKey.toBase58();
const upsert = vi.fn(async () => ({ error: { code: "57014", message: "canceling statement due to statement timeout" } }));
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: [] },
    getConnection: vi.fn(() => ({ getAccountInfo: vi.fn(async () => null) })),
    withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    getNetwork: () => "devnet",
    captureException: vi.fn(),
    getSupabase: () => ({ from: () => ({ upsert }) }),
  };
});
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true), insertTradeRows: vi.fn(async () => []), tradeKey: () => "k" }));
vi.mock("../src/db/storedLegs.js", async (orig) => ({ ...(await orig<typeof import("../src/db/storedLegs.js")>()), fetchStoredLegs: vi.fn(async () => []), fetchStoredLegsMany: vi.fn(async () => null) }));

import { TradeIndexerPolling, MAX_WRITE_HOLDS } from "../src/services/TradeIndexer.js";
import { recordSkippedSignatures } from "../src/lib/skippedSignatures.js";

const EX = JSON.parse(readFileSync(new URL("./fixtures/v22-fill-events-doc-examples.json", import.meta.url), "utf8")).examples as Record<string, { b64: string }>;
const M = new PublicKey(new Uint8Array(32).fill(1)).toBase58();
const TAKER = new PublicKey(new Uint8Array(32).fill(2)).toBase58();
const logs = (...t: string[]): string[] => [`Program ${W} invoke [1]`, ...t.map((x) => `Program data: ${x}`), `Program ${W} consumed 1 of 2 compute units`, `Program ${W} success`];

describe("H1: poll path, v22_events upsert returns { error }", () => {
  it("marks the signature failed so the cursor does not pass it", async () => {
    const tx = {
      slot: 99, blockTime: 1_790_000_000,
      meta: { err: null, logMessages: logs(EX.fill_clipped_tradecpi.b64), innerInstructions: [] },
      transaction: { message: { instructions: [{ programId: new PublicKey(W), accounts: [TAKER, M].map((k) => new PublicKey(k)), data: encodeBase58(new Uint8Array([10, 0, 0, 0])) }] } },
    };
    const pass: any = { earlier: new Set<string>(), incomplete: false };
    await (new TradeIndexerPolling() as any).processTransaction(tx, "SIGPOLL", M, new Set([W]), pass).catch(() => undefined);
    expect(upsert).toHaveBeenCalled(); // the v22_events write was attempted and returned { error }
    expect(pass).toMatchObject({ writeFailed: true }); // FAILS today: writeFailed is undefined, the cursor commits
    expect(pass.incomplete).toBe(false); // trade path unaffected: later tag 44 closes must not be refused
    expect([...((pass as { failedSigs?: Set<string> }).failedSigs ?? [])]).toContain("SIGPOLL");
  });

  it("a persistent v22 write failure is bounded: held MAX_WRITE_HOLDS polls, then skipped with a record (never loops forever)", async () => {
    const ix = new TradeIndexerPolling() as any;
    const holds: boolean[] = [];
    for (let i = 0; i <= MAX_WRITE_HOLDS; i++) {
      // each poll: another signature's trade write succeeded (db up), the v22 write of SIGPOLL failed again
      holds.push(await ix.holdAfterWriteFailure(M, { earlier: new Set(), incomplete: false, writeFailed: true, failedSigs: new Set(["SIGPOLL"]), writesOk: 1 }));
    }
    expect(holds.slice(0, MAX_WRITE_HOLDS).every(Boolean)).toBe(true);
    expect(holds[MAX_WRITE_HOLDS]).toBe(false); // cursor released
    expect(recordSkippedSignatures).toHaveBeenCalledWith([expect.objectContaining({ signature: "SIGPOLL", slab: M })]);
  });
});
