/**
 * Recorded devnet TradeCpi transactions (devnet replay J-20261120, ~/agent-work/scratch-idx-replay/txs) whose matcher
 * context had moved on before it was read. The tx's own trusted wrapper `fill_event` log says what executed:
 *  - 4HMi5a7n / 36uqXXiT (market 6YppKdUi): ZERO flag, executed_q 0, fee_atoms 0. No trade happened.
 *  - 1yPKapEE (market A8XsQeov): executed_q 373405 at price_e6 72168379, fee_atoms 140130.
 * Both ingestion paths must write the same row: no phantom trade for a zero fill, the executed size, and the same fee.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import { PublicKey } from "@solana/web3.js";

const H = vi.hoisted(() => ({ PROGRAM: "6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM" }));
const getAccountInfo = vi.fn();
vi.mock("../src/parsers/lotScale.js", async (orig) => {
  const actual = await orig<typeof import("../src/parsers/lotScale.js")>();
  return { ...actual, resolveLotExp: async (s: string, a: number) => actual.knownLotExp(s, a) ?? 0 };
});
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return { ...actual, config: { ...actual.config, allProgramIds: [H.PROGRAM] }, getConnection: vi.fn(() => ({ getAccountInfo })), withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()) };
});
const recordSkipped = vi.fn(async () => undefined);
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: (...a: unknown[]) => recordSkipped(...(a as [])), assertSkippedSignatureSinkReady: vi.fn() }));
const rowsOut: any[] = [];
vi.mock("../src/db/insertTradeRow.js", () => ({
  tradeKey: (r: any) => `${r.tx_signature}|${r.asset_index}|${r.leg_index}`,
  insertTradeRow: vi.fn(async (r: any) => { rowsOut.push(r); return true; }),
  insertTradeRows: vi.fn(async (rows: any[]) => { rowsOut.push(...rows); return rows; }),
}));
vi.mock("../src/db/insertV22Events.js", async (orig) => ({ ...(await orig<typeof import("../src/db/insertV22Events.js")>()), insertV22Events: vi.fn(async () => undefined), insertV22EventsWithRetry: vi.fn(async () => undefined) }));
vi.mock("../src/db/storedLegs.js", async (orig) => ({
  ...(await orig<typeof import("../src/db/storedLegs.js")>()),
  fetchStoredLegs: vi.fn(async () => []),
  fetchStoredLegsMany: vi.fn(async () => null),
}));
import { TradeIndexerPolling } from "../src/services/TradeIndexer.js";
import { EventStreamService } from "../src/services/EventStreamService.js";

const DIR = new URL("./fixtures/replay/", import.meta.url);
const tx = (prefix: string) => {
  const f = readdirSync(DIR).find((n) => n.startsWith(prefix))!;
  return { sig: f.slice(0, -5), raw: JSON.parse(readFileSync(new URL(f, DIR), "utf8")) };
};
const market = (raw: any): string => raw.transaction.message.instructions.find((i: any) => i.programId === H.PROGRAM && i.data.length > 100).accounts[1];

/** What web3.js getParsedTransaction(s) returns: every pubkey field as a PublicKey. */
function hydrate(raw: any): any {
  const t = structuredClone(raw);
  const pk = (s: any) => (typeof s === "string" ? new PublicKey(s) : s);
  const ix = (i: any) => { i.programId = pk(i.programId); if (Array.isArray(i.accounts)) i.accounts = i.accounts.map(pk); return i; };
  const m = t.transaction.message;
  m.accountKeys = m.accountKeys.map((k: any) => (typeof k === "string" ? { pubkey: pk(k), signer: false, writable: false } : { ...k, pubkey: pk(k.pubkey) }));
  m.instructions = m.instructions.map(ix);
  for (const g of t.meta?.innerInstructions ?? []) g.instructions = g.instructions.map(ix);
  return t;
}

async function poll(prefix: string): Promise<any[]> {
  const { sig, raw } = tx(prefix);
  const idx: any = new TradeIndexerPolling();
  await idx.processTransaction(hydrate(raw), sig, market(raw), new Set([H.PROGRAM]));
  await idx.flushPending?.({ earlier: new Set(), incomplete: false });
  return rowsOut.splice(0);
}

async function stream(prefix: string): Promise<any[]> {
  const { raw } = tx(prefix);
  let cb: (m: any) => void = () => {};
  const ws: any = { sub: () => {}, onNotification: (c: any) => { cb = c; }, close: () => {}, isOpen: true };
  const svc = new EventStreamService({ ws, programId: H.PROGRAM, connection: { getAccountInfo } as any, autoIndex: true, knownSlabs: [market(raw)] });
  await svc.start();
  cb({ method: "transactionNotification", params: { result: structuredClone(raw) } });
  await new Promise((r) => setTimeout(r, 100));
  return rowsOut.splice(0);
}

describe("TradeCpi with an unreadable matcher context: the tx's trusted fill_event decides", () => {
  beforeEach(() => {
    rowsOut.length = 0;
    recordSkipped.mockClear();
    getAccountInfo.mockReset();
    getAccountInfo.mockResolvedValue(null); // the matcher context no longer holds the answer (as in the replay)
  });

  for (const prefix of ["4HMi5a7n", "36uqXXiT"]) {
    it(`poll path: ZERO fill_event (${prefix}) stores no trade`, async () => {
      expect(await poll(prefix)).toEqual([]);
      expect(recordSkipped).not.toHaveBeenCalled();
    });
    it(`stream path: ZERO fill_event (${prefix}) stores no trade`, async () => {
      expect(await stream(prefix)).toEqual([]);
      expect(recordSkipped).not.toHaveBeenCalled();
    });
  }

  it("poll path: executed_q and fee_atoms from the fill_event (fee 140130 atoms = 0.14013)", async () => {
    expect(await poll("1yPKapEE")).toEqual([expect.objectContaining({ side: "long", size: "373405", price: 72.168379, fee: 0.14013, size_provenance: "matcher_exact" })]);
  });

  it("stream path: the same row, including the fee (was hardcoded 0)", async () => {
    expect(await stream("1yPKapEE")).toEqual([expect.objectContaining({ side: "long", size: "373405", price: 72.168379, fee: 0.14013, size_provenance: "matcher_exact" })]);
  });

  it("poll and stream store an identical row for the same tx", async () => {
    const strip = (r: any) => ({ ...r });
    const p = (await poll("1yPKapEE")).map(strip);
    const s = (await stream("1yPKapEE")).map(strip);
    expect(s).toEqual(p);
  });
});
