/**
 * v2.2 liquidations: parseLiquidation is a no-op (no instruction declares a liquidation), but the wrapper emits a REDUCE log
 * event with reason 3 ("liquidation") for the crank's forced close. That event must still leave an `is_liquidation` marker in
 * `trades` (the row traderNetPosition's guard looks for), via the shared recordV22LogEvents glue all three paths use.
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
const { insertTradeRows } = vi.hoisted(() => ({ insertTradeRows: vi.fn(async (_rows: unknown[]) => [] as unknown[]) }));
vi.mock("../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(async () => true), insertTradeRows, tradeKey: () => "k" }));
vi.mock("../src/db/storedLegs.js", async (orig) => ({ ...(await orig<typeof import("../src/db/storedLegs.js")>()), fetchStoredLegs: vi.fn(async () => []), fetchStoredLegsMany: vi.fn(async () => null) }));
const { insertV22Events } = vi.hoisted(() => ({ insertV22Events: vi.fn(async (rows: unknown[]) => rows.length) }));
vi.mock("../src/db/insertV22Events.js", () => ({ insertV22Events }));

import { TradeIndexerPolling } from "../src/services/TradeIndexer.js";
import { EventStreamService } from "../src/services/EventStreamService.js";
import { recordV22LogEvents } from "../src/services/v22LogEvents.js";
import { decodeBase58 } from "../src/lib/base58.js";
import { resetMarketLayoutNotes } from "../src/layout/marketVersions.js";
import { resetLogEventStats } from "../src/parsers/v22FillEvents.js";

const EX = JSON.parse(readFileSync(new URL("./fixtures/v22-fill-events-doc-examples.json", import.meta.url), "utf8")).examples as Record<string, { b64: string }>;
const M = new PublicKey(new Uint8Array(32).fill(1)).toBase58();
const OTHER = new PublicKey(new Uint8Array(32).fill(5)).toBase58();
const TAKER = new PublicKey(new Uint8Array(32).fill(2)).toBase58();
const CRANK_TAG = 5;
const crankData = (): string => encodeBase58(new Uint8Array([CRANK_TAG, 7, 0, 0, 0, 0, 0, 0, 0, 0]));
const logs = (...t: string[]): string[] => [`Program ${W} invoke [1]`, ...t.map((x) => `Program data: ${x}`), `Program ${W} consumed 1 of 2 compute units`, `Program ${W} success`];

/** The doc's REDUCE example with its reason byte (offset 109) set. */
const reduceWithReason = (reason: number): string => {
  const b = Buffer.from(EX.reduce_rebalance.b64, "base64");
  b[109] = reason;
  return b.toString("base64");
};
const LIQ = reduceWithReason(3);

const markers = (): Array<Record<string, unknown>> => insertTradeRows.mock.calls.flatMap((c) => c[0] as Array<Record<string, unknown>>).filter((r) => r.is_liquidation === true);

beforeEach(() => { insertV22Events.mockClear(); insertTradeRows.mockClear(); resetMarketLayoutNotes(); resetLogEventStats(); });

describe("v2.2 liquidation (crank + REDUCE reason 3) leaves an is_liquidation marker", () => {
  const web3 = (logMessages: string[]) => ({
    slot: 99, blockTime: 1_790_000_000,
    meta: { err: null, logMessages, innerInstructions: [] },
    transaction: { message: { instructions: [{ programId: new PublicKey(W), accounts: [TAKER, M].map((k) => new PublicKey(k)), data: crankData() }] } },
  });
  const poll = (tx: unknown, slab: string) => (new TradeIndexerPolling() as any).processTransaction(tx, "SIGLIQ", slab, new Set([W])).catch(() => undefined);

  it("poll path: marker row for the liquidated portfolio", async () => {
    await poll(web3(logs(LIQ)), M);
    const ev = (insertV22Events.mock.calls[0][0] as Array<{ kind: string; actor: string; detail: { reason_name: string } }>)[0];
    expect(ev).toMatchObject({ kind: "reduce_event", detail: { reason_name: "liquidation" } });
    expect(markers()).toEqual([
      expect.objectContaining({ slab_address: M, trader: ev.actor, side: null, size: null, price: null, tx_signature: "SIGLIQ", leg_index: 1000, is_liquidation: true }),
    ]);
  });

  it("poll path: a non-liquidation reduce (rebalance, reason 1) writes no marker", async () => {
    await poll(web3(logs(EX.reduce_rebalance.b64)), M);
    expect(insertV22Events).toHaveBeenCalledTimes(1);
    expect(markers()).toEqual([]);
  });

  it("poll path: another slab's poll writes no marker", async () => {
    await poll(web3(logs(LIQ)), OTHER);
    expect(markers()).toEqual([]);
  });

  it("Atlas stream: marker row for a known slab, none for an unknown one", async () => {
    const run = async (slabs: string[]) => {
      const listeners: Array<(m: unknown) => void> = [];
      const ws = { sub: () => {}, onNotification: (cb: (m: unknown) => void) => listeners.push(cb), close: () => {}, get isOpen() { return true; } };
      const svc = new EventStreamService({ ws: ws as any, programId: W, connection: { getAccountInfo: vi.fn(async () => null) } as any, autoIndex: true, knownSlabs: slabs });
      const tx = { signature: "SIGSTREAM", slot: 99, blockTime: 1_790_000_000, transaction: { signatures: ["SIGSTREAM"], message: { instructions: [{ programId: W, accounts: [TAKER, M], data: crankData() }] } }, meta: { err: null, logMessages: logs(LIQ), innerInstructions: [] } };
      await svc.start();
      for (const l of listeners) await (l as any)({ jsonrpc: "2.0", method: "transactionNotification", params: { result: tx, subscription: 1 } });
      await new Promise((r) => setTimeout(r, 20));
    };
    await run([OTHER]);
    expect(markers()).toEqual([]);
    await run([M]);
    expect(markers()).toEqual([expect.objectContaining({ slab_address: M, tx_signature: "SIGSTREAM", is_liquidation: true, side: null, size: null })]);
  });

  it("webhook path (shared glue, logs present): marker row; two liquidations get distinct leg_index", async () => {
    await recordV22LogEvents({
      signature: "SIGHOOK", err: null, logMessages: logs(LIQ, LIQ),
      wrapperInstructions: [{ programId: W, accounts: [TAKER, M], data: decodeBase58(crankData())!, ixIndex: 0, innerIndex: null } as any],
      wrapperIds: new Set([W]), isKnownMarket: (s) => s === M,
    });
    const m = markers();
    expect(m.map((r) => r.leg_index)).toEqual([1000, 1001]);
    expect(m.every((r) => r.tx_signature === "SIGHOOK" && r.slab_address === M)).toBe(true);
  });

  it("a marker-insert failure never throws out of the glue", async () => {
    insertTradeRows.mockRejectedValueOnce(new Error("boom"));
    await expect(recordV22LogEvents({
      signature: "SIGERR", err: null, logMessages: logs(LIQ),
      wrapperInstructions: [{ programId: W, accounts: [TAKER, M], data: decodeBase58(crankData())!, ixIndex: 0, innerIndex: null } as any],
      wrapperIds: new Set([W]), isKnownMarket: (s) => s === M,
    })).resolves.toBeTruthy();
  });
});
