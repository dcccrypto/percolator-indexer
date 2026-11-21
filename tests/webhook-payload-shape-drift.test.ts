/**
 * Webhook payload-shape drift (report J-20261121-IDX-REPORT-WEBHOOKPAYLOADSHAPEDRIFT). One test per drifted delivery
 * shape that dropped or changed a stored row on the webhook path; every one failed on 3061d3e (the control passed).
 * Two draft cases are NOT here because PR #397 (trade rows rebuilt from the chain tx) covers them: E2 trades (matcher
 * CPI found in tx-level groups) and E5 (shifted account list). With the payload alone those now write nothing.
 * Mock pattern: tests/matcher-fill-webhook.test.ts (real route, real parsers; DB writers captured).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";

const H = vi.hoisted(() => ({ PROGRAMS: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB", "6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM"] }));
const getAccountInfo = vi.fn();

vi.mock("../src/parsers/lotScale.js", async (orig) => {
  const actual = await orig<typeof import("../src/parsers/lotScale.js")>();
  return { ...actual, resolveLotExp: async (s: string, a: number) => actual.knownLotExp(s, a) ?? 0 };
});
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: H.PROGRAMS, webhookSecret: "s3cret" },
    eventBus: { publish: vi.fn(), emit: vi.fn(), on: vi.fn() },
    getConnection: vi.fn(() => ({ getAccountInfo })),
    withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    captureException: vi.fn(),
  };
});
const recordSkipped = vi.fn(async () => undefined);
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (...a: unknown[]) => recordSkipped(...(a as [])) }));
vi.mock("../src/db/insertTradeRow.js", () => ({
  tradeKey: (r: any) => `${r.tx_signature}|${r.asset_index}|${r.leg_index}`,
  insertTradeRow: vi.fn(),
  insertTradeRows: vi.fn(async (rows: any[]) => rows),
}));
vi.mock("../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async () => null),
}));
const v22Rows: any[] = [];
vi.mock("../src/db/insertV22Events.js", async (orig) => ({
  ...(await orig<typeof import("../src/db/insertV22Events.js")>()),
  insertV22Events: vi.fn(async (rows: any[]) => { v22Rows.push(...rows); return rows.length; }),
}));
import { insertTradeRows } from "../src/db/insertTradeRow.js";
import { webhookRoutes } from "../src/routes/webhook.js";
import { rawInstructionsFromEnhancedTx } from "../src/parsers/v22Events.js";

const enhanced = JSON.parse(readFileSync(new URL("./fixtures/tradecpi/helius-enhanced.json", import.meta.url), "utf8")).txs as Record<string, any>;
const classic = (n: string) => JSON.parse(readFileSync(new URL(`./fixtures/tradecpi/${n}.json`, import.meta.url), "utf8"));
const drift = JSON.parse(readFileSync(new URL("./fixtures/shape-drift.json", import.meta.url), "utf8"));
const clone = <T>(x: T): T => JSON.parse(JSON.stringify(x));

async function post(body: unknown) {
  const res = await webhookRoutes().fetch(new Request("http://x/webhook/trades", { method: "POST", headers: { "content-type": "application/json", authorization: "s3cret" }, body: JSON.stringify(body) }));
  const rows = vi.mocked(insertTradeRows).mock.calls.flatMap((c) => c[0] as any[]).filter((r) => !r.is_liquidation);
  return { status: res.status, rows };
}
// The #213 "partial" TradeCpi: canonically ONE row, size 483 (executed), matcher_exact. Its wire (requested) size is 29041225.
const PARTIAL = classic("partial");
const ctxReturnBytes = (hex: string) => Buffer.from(hex.padEnd(640, "0"), "hex");
const canonical = () => clone(enhanced[PARTIAL.sig]);
const wrapperIx = (tx: any) => tx.instructions.find((i: any) => H.PROGRAMS.includes(i.programId) && i.data.length > 100);
/** Helius RAW webhook item (getTransaction encoding "json") built from the enhanced fixture: index-based, no top-level signature. */
function rawJsonOf(e: any): any {
  const keys: string[] = [];
  const at = (k: string) => { let i = keys.indexOf(k); if (i < 0) { keys.push(k); i = keys.length - 1; } return i; };
  const ix = (i: any) => ({ programIdIndex: at(i.programId), accounts: (i.accounts ?? []).map(at), data: i.data, stackHeight: null });
  const instructions = e.instructions.map(ix);
  const innerInstructions = e.instructions.map((i: any, index: number) => ({ index, instructions: (i.innerInstructions ?? []).map(ix) })).filter((g: any) => g.instructions.length);
  return { blockTime: e.timestamp, slot: e.slot, version: "legacy", meta: { err: null, innerInstructions, logMessages: [], loadedAddresses: { writable: [], readonly: [] } }, transaction: { signatures: [e.signature], message: { accountKeys: keys, instructions, recentBlockhash: "11111111111111111111111111111111" } } };
}

describe("webhook payload-shape drift", () => {
  beforeEach(() => {
    vi.mocked(insertTradeRows).mockClear(); recordSkipped.mockClear(); v22Rows.length = 0;
    getAccountInfo.mockReset(); getAccountInfo.mockResolvedValue({ data: ctxReturnBytes(PARTIAL.ctxReturnHex) });
  });

  it("control (E0, documented enhanced shape): one row, executed size 483, matcher_exact", async () => {
    const r = await post([canonical()]);
    expect(r.status).toBe(200);
    expect(r.rows).toEqual([expect.objectContaining({ tx_signature: PARTIAL.sig, size: "483", size_provenance: "matcher_exact" })]);
  });

  it("R1-R4/R6 (Helius RAW webhook item, any encoding): a TradeCpi is stored or the delivery is refused, never 200 + nothing", async () => {
    const r = await post([rawJsonOf(canonical())]);
    // e708fc5: 200 {"received":1} and zero rows: the route only knows top-level `signature` / `instructions`.
    expect(r.rows.length > 0 || r.status >= 400).toBe(true);
  });

  it("E10 (raw item mixed into an enhanced delivery): the raw item is not silently acknowledged", async () => {
    const other = clone(drift.fill);
    const r = await post([other, rawJsonOf(canonical())]);
    expect(r.rows.some((x) => x.tx_signature === PARTIAL.sig) || r.status >= 400).toBe(true);
  });

  it("E1 (instructions[].innerInstructions absent): the wire REQUEST size is not written as the fill", async () => {
    const tx = canonical();
    for (const i of tx.instructions) delete i.innerInstructions;
    const r = await post([tx]);
    // e708fc5: 200 and size 29041225 legacy_unverified (60,000x the executed 483), written permanently (ignoreDuplicates).
    expect(r.rows.filter((x) => x.size === "29041225")).toEqual([]);
    // Nothing is written from the payload (the poll path stores the fill); the skip is recorded, not silent.
    expect(r.status).toBe(200);
    expect(r.rows).toEqual([]);
    expect(recordSkipped).toHaveBeenCalledWith([expect.objectContaining({ signature: PARTIAL.sig, source: "trade-indexer" })]);
  });

  it("E2 (v22_events): a flat CPI group keeps the outer ix_index (stream/poll key), not 1000+group", async () => {
    const tx = clone(drift.v22ix);
    tx.innerInstructions = tx.instructions.map((i: any, index: number) => ({ index, instructions: i.innerInstructions ?? [] })).filter((g: any) => g.instructions.length);
    for (const i of tx.instructions) delete i.innerInstructions;
    await post([tx]);
    // canonical (and EventStreamService / poll): (sig, ix_index 4, inner_index 0). e708fc5 webhook: ix_index 1000.
    expect(v22Rows.map((r) => [r.kind, r.ix_index, r.inner_index])).toEqual([["insurance_units_init", 4, 0]]);
  });

  it("E3 (account entries as jsonParsed {pubkey} objects): the fill is stored, not a 400 for the whole delivery", async () => {
    const tx = canonical();
    const obj = (a: string) => ({ pubkey: a, signer: false, writable: false, source: "transaction" });
    tx.instructions = tx.instructions.map((i: any) => ({ ...i, accounts: i.accounts.map(obj), innerInstructions: (i.innerInstructions ?? []).map((n: any) => ({ ...n, accounts: (n.accounts ?? []).map(obj) })) }));
    const r = await post([tx]);
    expect(r.status).toBe(200);
    expect(r.rows).toEqual([expect.objectContaining({ size: "483" })]);
  });

  it("E4/E9/R5 (one unusable item: a null account, a null tx, a bare string): the OTHER txs of the delivery are still stored", async () => {
    const bad = canonical();
    wrapperIx(bad).accounts[3] = null; // an unresolved lookup-table address
    for (const odd of [bad, null, "notATx"]) {
      vi.mocked(insertTradeRows).mockClear();
      const r = await post([clone(drift.fill), odd, canonical()]);
      // e708fc5: 400 "Invalid transaction format" for the whole delivery -> 0 rows (Helius redelivers the same 400).
      expect(r.rows.some((x) => x.tx_signature === PARTIAL.sig)).toBe(true);
    }
  });

  it("E6 (failed tx carrying only RPC-style meta.err, no transactionError): no phantom fill", async () => {
    const tx = canonical();
    delete tx.transactionError;
    tx.meta = { err: { InstructionError: [2, { Custom: 1 }] } };
    // CPIs kept, so only the meta.err fallback stops the row (without it: one 483 matcher_exact row for a FAILED tx).
    const r = await post([tx]);
    expect(r.rows).toEqual([]);
  });

  it("E7 (blockTime instead of timestamp): v22_events.block_time is still set", async () => {
    const tx = clone(drift.v22ix);
    tx.blockTime = tx.timestamp;
    delete tx.timestamp;
    await post([tx]);
    expect(v22Rows).toHaveLength(1);
    expect(v22Rows[0].block_time ?? v22Rows[0].blockTimeSec ?? null).not.toBeNull();
  });

  it("E8 (logs nested as meta.logMessages, RPC style): the v2.2 log events are recorded like top-level logMessages", async () => {
    const top = clone(drift.fill); top.logMessages = drift.fillLogMessages;
    await post([top]);
    const want = v22Rows.map((r) => r.kind);
    expect(want).toContain("fill_event"); // sanity: the top-level form records it
    v22Rows.length = 0;
    const nested = clone(drift.fill); nested.meta = { logMessages: drift.fillLogMessages };
    await post([nested]);
    expect(v22Rows.map((r) => r.kind)).toEqual(want);
  });

  it("per item: a POISON key in one item still rejects the whole request (SEC #127), nothing is written", async () => {
    const raw = `[${JSON.stringify(clone(drift.fill))},{"__proto__":{"x":1},"signature":"${PARTIAL.sig}","instructions":[]},${JSON.stringify(canonical())}]`;
    const res = await webhookRoutes().fetch(new Request("http://x/webhook/trades", { method: "POST", headers: { "content-type": "application/json", authorization: "s3cret" }, body: raw }));
    expect(res.status).toBe(400);
    expect(vi.mocked(insertTradeRows)).not.toHaveBeenCalled();
  });

  it("per item: a poison key hidden in a {pubkey} account entry, or in an item that is unusable for another reason, still rejects the whole request", async () => {
    const inAccount = canonical();
    wrapperIx(inAccount).accounts[0] = JSON.parse(`{"pubkey":"${wrapperIx(inAccount).accounts[0]}","constructor":1}`);
    const asArrayItem = `[${JSON.stringify(clone(drift.fill))},[{"__proto__":{"x":1}}]]`;
    const behindError = `[${JSON.stringify(clone(drift.fill))},{"signature":"${PARTIAL.sig}","instructions":[{"programId":5}],"accountData":[{"__proto__":{"x":1}}]}]`;
    for (const body of [JSON.stringify([inAccount]), behindError, asArrayItem]) {
      const res = await webhookRoutes().fetch(new Request("http://x/webhook/trades", { method: "POST", headers: { "content-type": "application/json", authorization: "s3cret" }, body }));
      expect(res.status).toBe(400);
    }
    expect(vi.mocked(insertTradeRows)).not.toHaveBeenCalled();
  });

  it("per item: an unusable item that names a signature is recorded in skipped_signatures; the delivery answers 200 with the counts", async () => {
    const bad = canonical();
    wrapperIx(bad).accounts[3] = null;
    const res = await webhookRoutes().fetch(new Request("http://x/webhook/trades", { method: "POST", headers: { "content-type": "application/json", authorization: "s3cret" }, body: JSON.stringify([clone(drift.fill), bad]) }));
    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ received: 1, rejected: 1 });
    expect(recordSkipped).toHaveBeenCalledWith([expect.objectContaining({ signature: PARTIAL.sig, source: "trade-indexer", error: expect.stringContaining("not a resolved address") })]);
  });

  it("per item: a delivery with NO usable item answers 400 (422 when the items are raw) before any read or write", async () => {
    expect((await post([null, "notATx"])).status).toBe(400);
    expect((await post([rawJsonOf(canonical()), rawJsonOf(canonical())])).status).toBe(422);
    expect(vi.mocked(insertTradeRows)).not.toHaveBeenCalled();
    expect(getAccountInfo).not.toHaveBeenCalled();
  });

  it("v22 flat groups: a group without a usable index falls back to 1000 + its position", () => {
    const tx = clone(drift.v22ix);
    const groups = tx.instructions.map((i: any, index: number) => ({ index, instructions: i.innerInstructions ?? [] })).filter((g: any) => g.instructions.length);
    for (const i of tx.instructions) delete i.innerInstructions;
    const progs = new Set(H.PROGRAMS);
    const keyed = (gs: unknown) => rawInstructionsFromEnhancedTx({ ...tx, innerInstructions: gs }, progs).filter((r) => r.innerIndex >= 0).map((r) => r.ixIndex);
    expect(new Set(keyed(groups))).toEqual(new Set(groups.filter((g: any) => g.instructions.some((n: any) => progs.has(n.programId))).map((g: any) => g.index)));
    const noIndex = groups.map(({ index: _i, ...g }: any) => g);
    expect(keyed(noIndex).length).toBeGreaterThan(0);
    expect(keyed(noIndex).every((ix) => ix >= 1000)).toBe(true);
  });
});
