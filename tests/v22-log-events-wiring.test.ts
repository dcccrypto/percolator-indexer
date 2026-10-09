/**
 * v2.2 wrapper LOG events through the REAL call sites: the poll path (TradeIndexer.processTransaction) and the Atlas
 * stream (EventStreamService). Each asserts the row is written for a registered v2.2 market and NOT written when the call
 * site's registry says otherwise; the trade path under it is not what is being tested.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { Keypair, PublicKey } from "@solana/web3.js";
import { encodeBase58 } from "../src/lib/base58.js";

const W = Keypair.generate().publicKey.toBase58();
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return { ...actual, config: { ...actual.config, allProgramIds: [] }, getConnection: vi.fn(() => ({ getAccountInfo: vi.fn(async () => null) })), withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()) };
});
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined), assertSkippedSignatureSinkReady: vi.fn() }));
vi.mock("../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true), insertTradeRows: vi.fn(async () => []), tradeKey: () => "k" }));
vi.mock("../src/db/storedLegs.js", async (orig) => ({ ...(await orig<typeof import("../src/db/storedLegs.js")>()), fetchStoredLegs: vi.fn(async () => []), fetchStoredLegsMany: vi.fn(async () => null) }));
const { insertV22Events } = vi.hoisted(() => ({ insertV22Events: vi.fn(async (rows: unknown[]) => rows.length) }));
vi.mock("../src/db/insertV22Events.js", () => ({ insertV22Events }));

import { TradeIndexerPolling } from "../src/services/TradeIndexer.js";
import { EventStreamService } from "../src/services/EventStreamService.js";
import { LAYOUT_V21, LAYOUT_V22, buildMarket } from "./helpers/v22Fixtures.js";
import { noteMarketLayout, resetMarketLayoutNotes } from "../src/layout/marketVersions.js";
import { resetLogEventStats } from "../src/parsers/v22FillEvents.js";

const EX = JSON.parse(readFileSync(new URL("./fixtures/v22-fill-events-doc-examples.json", import.meta.url), "utf8")).examples as Record<string, { b64: string }>;
const M = new PublicKey(new Uint8Array(32).fill(1)).toBase58(); // the examples' market
const OTHER = new PublicKey(new Uint8Array(32).fill(5)).toBase58();
const TAKER = new PublicKey(new Uint8Array(32).fill(2)).toBase58();
const wrapperData = (tag: number): string => encodeBase58(new Uint8Array([tag, 0, 0, 0]));
const logs = (...t: string[]): string[] => [`Program ${W} invoke [1]`, ...t.map((x) => `Program data: ${x}`), `Program ${W} consumed 1 of 2 compute units`, `Program ${W} success`];

beforeEach(() => { insertV22Events.mockClear(); resetMarketLayoutNotes(); resetLogEventStats(); });

describe("poll path (TradeIndexer.processTransaction)", () => {
  const web3 = (logMessages: string[] | null, tag = 10) => ({
    slot: 99, blockTime: 1_790_000_000,
    meta: { err: null, logMessages, innerInstructions: [] },
    transaction: { message: { instructions: [{ programId: new PublicKey(W), accounts: [TAKER, M].map((k) => new PublicKey(k)), data: wrapperData(tag) }] } },
  });
  const poll = (tx: unknown, slab: string) => (new TradeIndexerPolling() as any).processTransaction(tx, "SIGPOLL", slab, new Set([W])).catch(() => undefined);

  it("writes the FILL of the polled slab", async () => {
    await poll(web3(logs(EX.fill_clipped_tradecpi.b64)), M);
    expect(insertV22Events).toHaveBeenCalledTimes(1);
    const rows = insertV22Events.mock.calls[0][0] as Array<{ kind: string; slab_address: string; signature: string }>;
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ kind: "fill_event", slab_address: M, signature: "SIGPOLL" });
  });

  it("another slab's poll does not record this market's events (each slab's own poll does)", async () => {
    await poll(web3(logs(EX.fill_clipped_tradecpi.b64)), OTHER);
    expect(insertV22Events).not.toHaveBeenCalled();
  });

  it("VERSION-keyed: the polled market is known to be VERSION 18 -> nothing; VERSION 19 -> the row", async () => {
    noteMarketLayout(M, buildMarket(LAYOUT_V21, { slots: 1 }));
    await poll(web3(logs(EX.fill_clipped_tradecpi.b64)), M);
    expect(insertV22Events).not.toHaveBeenCalled();
    noteMarketLayout(M, buildMarket(LAYOUT_V22, { slots: 1 }));
    await poll(web3(logs(EX.fill_clipped_tradecpi.b64)), M);
    expect(insertV22Events).toHaveBeenCalledTimes(1);
  });

  it("truncated logs on a v2.2 market: an events_unknown marker, never an absent fill", async () => {
    noteMarketLayout(M, buildMarket(LAYOUT_V22, { slots: 1 }));
    await poll(web3([...logs(EX.fill_clipped_tradecpi.b64), "Log truncated"]), M);
    const rows = insertV22Events.mock.calls[0][0] as Array<{ kind: string; detail: Record<string, unknown> }>;
    expect(rows.map((r) => r.kind)).toEqual(["events_unknown"]);
    expect(rows[0].detail).toMatchObject({ reason: "truncated", reconcile: "from_account_state" });
  });

  it("a recording failure never reaches the trade path (insert rejects, processTransaction does not throw because of it)", async () => {
    insertV22Events.mockRejectedValueOnce(new Error("boom"));
    const tx = web3(logs(EX.fill_clipped_tradecpi.b64));
    const err = await (new TradeIndexerPolling() as any).processTransaction(tx, "SIGX", M, new Set([W])).then(() => null, (e: unknown) => e);
    expect(String(err ?? "")).not.toMatch(/boom/);
  });
});

describe("Atlas event stream (EventStreamService)", () => {
  function run(tx: unknown, knownSlabs: string[]) {
    const listeners: Array<(m: unknown) => void> = [];
    const ws = { sub: () => {}, onNotification: (cb: (m: unknown) => void) => listeners.push(cb), close: () => {}, get isOpen() { return true; } };
    const svc = new EventStreamService({ ws: ws as any, programId: W, connection: { getAccountInfo: vi.fn(async () => null) } as any, autoIndex: true, knownSlabs });
    return svc.start().then(async () => { for (const l of listeners) await (l as any)({ jsonrpc: "2.0", method: "transactionNotification", params: { result: tx, subscription: 1 } }); await new Promise((r) => setTimeout(r, 20)); });
  }
  const parsed = (logMessages: string[] | null, tag = 10) => ({
    signature: "SIGSTREAM", slot: 99, blockTime: 1_790_000_000,
    transaction: { signatures: ["SIGSTREAM"], message: { instructions: [{ programId: W, accounts: [TAKER, M], data: wrapperData(tag) }] } },
    meta: { err: null, logMessages, innerInstructions: [] },
  });

  it("writes the FILL for a known slab (a separate insert call from the instruction events)", async () => {
    await run(parsed(logs(EX.fill_clipped_tradecpi.b64)), [M]);
    const calls = insertV22Events.mock.calls.map((c) => (c[0] as Array<{ kind: string }>).map((r) => r.kind));
    expect(calls).toContainEqual(["fill_event"]);
  });

  it("not for a slab outside the stream's set", async () => {
    await run(parsed(logs(EX.fill_clipped_tradecpi.b64)), [OTHER]);
    expect(insertV22Events).not.toHaveBeenCalled();
  });

  it("a forged event from another program's frame is never written", async () => {
    const m = Keypair.generate().publicKey.toBase58();
    const forged = [`Program ${W} invoke [1]`, `Program ${m} invoke [2]`, `Program data: ${EX.fill_clipped_tradecpi.b64}`, `Program ${m} success`, `Program ${W} success`];
    await run(parsed(forged), [M]);
    expect(insertV22Events).not.toHaveBeenCalled();
  });
});
