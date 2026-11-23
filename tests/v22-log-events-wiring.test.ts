/**
 * v2.2 wrapper LOG events through the REAL call sites: the poll path (TradeIndexer.processTransaction) and the Atlas
 * stream (EventStreamService). Each asserts the row is written for a registered v2.2 market and NOT written when the call
 * site's registry says otherwise; the trade path under it is not what is being tested.
 *
 * The insertV22Events mock keeps the real V22EventWriteError export (without it, recordV22LogEvents' `instanceof` check
 * throws a different error and the write_failed branch is unreachable). The liquidation parser, the instruction adapter
 * and the blocklist are wrapped pass-throughs, so a test can make the trade path or the recording prep throw, or block
 * a slab.
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
const h = vi.hoisted(() => ({ insertV22Events: vi.fn(async (rows: unknown[]) => rows.length), blocked: new Set<string>() }));
const { insertV22Events } = h;
vi.mock("../src/db/insertV22Events.js", async (orig) => ({ ...(await orig<typeof import("../src/db/insertV22Events.js")>()), insertV22Events: h.insertV22Events }));
vi.mock("../src/blocklist.js", async (orig) => {
  const actual = await orig<typeof import("../src/blocklist.js")>();
  return { ...actual, isBlockedSlab: (s: string | null | undefined) => (s != null && h.blocked.has(s)) || actual.isBlockedSlab(s) };
});
vi.mock("../src/parsers/percolatorTxParser.js", async (orig) => {
  const actual = await orig<typeof import("../src/parsers/percolatorTxParser.js")>();
  return { ...actual, parsePercolatorLiquidations: vi.fn(actual.parsePercolatorLiquidations) };
});
vi.mock("../src/parsers/v22Events.js", async (orig) => {
  const actual = await orig<typeof import("../src/parsers/v22Events.js")>();
  return { ...actual, rawInstructionsFromParsedTx: vi.fn(actual.rawInstructionsFromParsedTx) };
});

import { TradeIndexerPolling } from "../src/services/TradeIndexer.js";
import { EventStreamService } from "../src/services/EventStreamService.js";
import { LAYOUT_V21, LAYOUT_V22, buildMarket } from "./helpers/v22Fixtures.js";
import { noteMarketLayout, resetMarketLayoutNotes } from "../src/layout/marketVersions.js";
import { resetLogEventStats } from "../src/parsers/v22FillEvents.js";
import { V22EventWriteError } from "../src/db/insertV22Events.js";
import { parsePercolatorLiquidations } from "../src/parsers/percolatorTxParser.js";
import { rawInstructionsFromParsedTx } from "../src/parsers/v22Events.js";

const EX = JSON.parse(readFileSync(new URL("./fixtures/v22-fill-events-doc-examples.json", import.meta.url), "utf8")).examples as Record<string, { b64: string }>;
const M = new PublicKey(new Uint8Array(32).fill(1)).toBase58(); // the examples' market
const OTHER = new PublicKey(new Uint8Array(32).fill(5)).toBase58();
const TAKER = new PublicKey(new Uint8Array(32).fill(2)).toBase58();
const SIG88 = encodeBase58(new Uint8Array(64).fill(7)); // a real-length signature: a sliced key would not be the row's key
const wrapperData = (tag: number): string => encodeBase58(new Uint8Array([tag, 0, 0, 0]));
const logs = (...t: string[]): string[] => [`Program ${W} invoke [1]`, ...t.map((x) => `Program data: ${x}`), `Program ${W} consumed 1 of 2 compute units`, `Program ${W} success`];

beforeEach(() => {
  insertV22Events.mockClear(); vi.mocked(parsePercolatorLiquidations).mockClear(); vi.mocked(rawInstructionsFromParsedTx).mockClear();
  h.blocked.clear(); resetMarketLayoutNotes(); resetLogEventStats();
});

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

  it("a recording failure never reaches the trade path (a plain insert rejection: no throw, no cursor hold)", async () => {
    insertV22Events.mockRejectedValueOnce(new Error("boom"));
    const pass: Pass = { earlier: new Set(), incomplete: false };
    const err = await run(web3(logs(EX.fill_clipped_tradecpi.b64)), M, SIG88, pass);
    expect(err).toBeNull();
    expect(insertV22Events).toHaveBeenCalledTimes(1);
    expect(pass.writeFailed).toBeUndefined(); // an unexpected bug is not a DB write failure
  });

  type Pass = { earlier: Set<string>; incomplete: boolean; writeFailed?: boolean; failedSigs?: Set<string> };
  const tx = (logMessages: string[] | null, accounts = [TAKER, M], err: unknown = null) => ({
    ...web3(logMessages),
    meta: { err, logMessages, innerInstructions: [] },
    transaction: { message: { instructions: [{ programId: new PublicKey(W), accounts: accounts.map((k) => new PublicKey(k)), data: wrapperData(10) }] } },
  });
  const run = (t: unknown, slab = M, sig = SIG88, pass: Pass = { earlier: new Set(), incomplete: false }) =>
    (new TradeIndexerPolling() as any).processTransaction(t, sig, slab, new Set([W]), pass).then(() => null, (e: unknown) => e);
  const rowsOf = (call = 0) => insertV22Events.mock.calls[call][0] as Array<Record<string, any>>;

  it("a trade-path failure after the events does not lose them (events are recorded first)", async () => {
    vi.mocked(parsePercolatorLiquidations).mockImplementationOnce(() => { throw new Error("trade path down"); });
    const err = await run(web3(logs(EX.fill_clipped_tradecpi.b64)));
    expect(String(err)).toMatch(/trade path down/);
    expect(insertV22Events).toHaveBeenCalledTimes(1);
    expect(rowsOf()[0]).toMatchObject({ kind: "fill_event", slab_address: M });
  });

  it("a failed transaction records no events", async () => {
    await run(tx(logs(EX.fill_clipped_tradecpi.b64), [TAKER, M], { InstructionError: [0, "Custom"] }));
    expect(insertV22Events).not.toHaveBeenCalled();
  });

  it("a v22_events write failure holds the cursor for this signature but keeps the pass complete", async () => {
    insertV22Events.mockRejectedValueOnce(new V22EventWriteError("upsert failed", 1, "57014"));
    const pass: Pass = { earlier: new Set(), incomplete: false };
    const err = await run(web3(logs(EX.fill_clipped_tradecpi.b64)), M, SIG88, pass);
    expect(err).toBeNull();
    expect(pass.writeFailed).toBe(true);
    expect([...(pass.failedSigs ?? [])]).toEqual([SIG88]);
    expect(pass.incomplete).toBe(false); // keepComplete: later tag 44 closes stay resolvable
  });

  it("a successful write leaves the pass untouched (control)", async () => {
    const pass: Pass = { earlier: new Set(), incomplete: false };
    await run(web3(logs(EX.fill_clipped_tradecpi.b64)), M, SIG88, pass);
    expect(insertV22Events).toHaveBeenCalledTimes(1);
    expect(pass.writeFailed).toBeUndefined();
    expect(pass.failedSigs).toBeUndefined();
  });

  it("the row carries the full signature, the slot and the block time", async () => {
    await run(web3(logs(EX.fill_clipped_tradecpi.b64)));
    expect(rowsOf()[0]).toMatchObject({ signature: SIG88, slot: 99, block_time: new Date(1_790_000_000 * 1000).toISOString() });
  });

  it("a blocked slab gets no events even when it is the polled slab", async () => {
    h.blocked.add(M);
    await run(web3(logs(EX.fill_clipped_tradecpi.b64)));
    expect(insertV22Events).not.toHaveBeenCalled();
  });

  it("truncated logs naming two v2.2 markets: only the polled slab gets a marker", async () => {
    noteMarketLayout(M, buildMarket(LAYOUT_V22, { slots: 1 }));
    noteMarketLayout(OTHER, buildMarket(LAYOUT_V22, { slots: 1 }));
    await run(tx([...logs(EX.fill_clipped_tradecpi.b64), "Log truncated"], [TAKER, M, OTHER]));
    expect(rowsOf().map((r) => [r.kind, r.slab_address])).toEqual([["events_unknown", M]]);
  });

  it("truncated logs on a market NOT known to be v2.2 (unread or VERSION 18): no marker", async () => {
    await run(web3([...logs(EX.fill_clipped_tradecpi.b64), "Log truncated"]));
    noteMarketLayout(M, buildMarket(LAYOUT_V21, { slots: 1 }));
    await run(web3([...logs(EX.fill_clipped_tradecpi.b64), "Log truncated"]));
    expect(insertV22Events).not.toHaveBeenCalled();
  });

  it("a throw while preparing the recording (instruction adapter) is swallowed; the trade path still runs", async () => {
    vi.mocked(rawInstructionsFromParsedTx).mockImplementationOnce(() => { throw new Error("adapter bug"); });
    const err = await run(web3(logs(EX.fill_clipped_tradecpi.b64)));
    expect(err).toBeNull();
    expect(insertV22Events).not.toHaveBeenCalled();
    expect(parsePercolatorLiquidations).toHaveBeenCalledTimes(1); // trade path reached
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
