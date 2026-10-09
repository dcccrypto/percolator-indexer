/**
 * The Helius webhook reads v2.2 LOG events only when the delivery carries the logs (the enhanced payload normally does not):
 * with `logMessages` present the wrapper's FILL is recorded for a registered market; without them nothing is attempted and
 * nothing is claimed (no events_unknown marker for a feed that never carries logs).
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import { Keypair, PublicKey } from "@solana/web3.js";
import { encodeBase58 } from "../../src/lib/base58.js";

const W = vi.hoisted(() => "FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD");
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return { ...actual, config: { ...actual.config, allProgramIds: ["FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD"], webhookSecret: "s3cret" }, eventBus: { publish: vi.fn() }, getConnection: vi.fn(() => ({ getAccountInfo: vi.fn(async () => null) })), withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()), captureException: vi.fn() };
});
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(), insertTradeRows: vi.fn(async () => []), tradeKey: () => "k" }));
const { insertV22Events } = vi.hoisted(() => ({ insertV22Events: vi.fn(async (rows: unknown[]) => rows.length) }));
vi.mock("../../src/db/insertV22Events.js", () => ({ insertV22Events }));

import { webhookRoutes } from "../../src/routes/webhook.js";
import { noteMarketLayout, resetMarketLayoutNotes } from "../../src/layout/marketVersions.js";
import { LAYOUT_V22, buildMarket } from "../helpers/v22Fixtures.js";

const EX = JSON.parse(readFileSync(new URL("../fixtures/v22-fill-events-doc-examples.json", import.meta.url), "utf8")).examples as Record<string, { b64: string }>;
const M = new PublicKey(new Uint8Array(32).fill(1)).toBase58();
const TAKER = new PublicKey(new Uint8Array(32).fill(2)).toBase58();
const MATCHER = Keypair.generate().publicKey.toBase58();

const tx = (extra: Record<string, unknown>) => ({
  signature: "SIGWH", slot: 123, timestamp: 1_790_000_000, transactionError: null,
  instructions: [{ programId: W, accounts: [TAKER, M], data: encodeBase58(new Uint8Array([10, 0, 0, 0])), innerInstructions: [] }],
  innerInstructions: [],
  ...extra,
});
const send = (body: unknown[], known: string[]) => {
  const app = webhookRoutes({ getMarkets: () => new Map(known.map((k) => [k, {}])) } as never);
  return app.fetch(new Request("http://localhost/webhook/trades", { method: "POST", headers: { "Content-Type": "application/json", authorization: "s3cret" }, body: JSON.stringify(body) }));
};
const logs = (...t: string[]): string[] => [`Program ${W} invoke [1]`, ...t.map((x) => `Program data: ${x}`), `Program ${W} success`];
const written = () => insertV22Events.mock.calls.flatMap((c) => c[0] as Array<{ kind: string }>).map((r) => r.kind);

beforeEach(() => { insertV22Events.mockClear(); resetMarketLayoutNotes(); });

describe("POST /webhook/trades: v2.2 log events", () => {
  it("logMessages present: the FILL is recorded for a registered market", async () => {
    await send([tx({ logMessages: logs(EX.fill_clipped_tradecpi.b64) })], [M]);
    expect(written()).toEqual(["fill_event"]);
  });

  it("logMessages absent (the enhanced payload): nothing is attempted, no marker, no claim", async () => {
    noteMarketLayout(M, buildMarket(LAYOUT_V22, { slots: 1 }));
    await send([tx({})], [M]);
    expect(written()).toEqual([]);
  });

  it("an unregistered market is not recorded", async () => {
    await send([tx({ logMessages: logs(EX.fill_clipped_tradecpi.b64) })], ["SomeOtherAccount"]);
    expect(written()).toEqual([]);
  });

  it("a forged line from a CPI'd program (the matcher) is not attributed to the wrapper", async () => {
    const forged = [`Program ${W} invoke [1]`, `Program ${MATCHER} invoke [2]`, `Program data: ${EX.fill_clipped_tradecpi.b64}`, `Program ${MATCHER} success`, `Program ${W} success`];
    await send([tx({ logMessages: forged })], [M]);
    expect(written()).toEqual([]);
  });

  it("a failed transaction (transactionError) records nothing", async () => {
    await send([tx({ transactionError: { InstructionError: [0, "x"] }, logMessages: logs(EX.fill_clipped_tradecpi.b64) })], [M]);
    expect(written()).toEqual([]);
  });
});
