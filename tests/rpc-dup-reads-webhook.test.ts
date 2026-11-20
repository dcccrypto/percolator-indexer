/**
 * D1 (idx-rpc-calls-per-cycle report): one webhook delivery carries many TradeCpi txs against the same
 * few LP matcher contexts. The context is read once per delivery, not once per CPI fill, and the stored
 * rows are the same as with a fresh read per tx.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";

const getAccountInfo = vi.fn();
const getAccountInfoAndContext = vi.fn();

vi.mock("../src/parsers/lotScale.js", async (orig) => {
  const actual = await orig<typeof import("../src/parsers/lotScale.js")>();
  return { ...actual, resolveLotExp: async (s: string, a: number) => actual.knownLotExp(s, a) ?? 0 };
});
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    config: { ...actual.config, allProgramIds: ["ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB"], webhookSecret: "s3cret" },
    eventBus: { publish: vi.fn(), emit: vi.fn(), on: vi.fn() },
    getConnection: vi.fn(() => ({ getAccountInfo, getAccountInfoAndContext })),
    withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()),
    captureException: vi.fn(),
  };
});
vi.mock("../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(async () => undefined) }));
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
import { insertTradeRows } from "../src/db/insertTradeRow.js";
import { webhookRoutes } from "../src/routes/webhook.js";

const enhanced = JSON.parse(readFileSync(new URL("./fixtures/tradecpi/helius-enhanced.json", import.meta.url), "utf8")).txs as Record<string, any>;
const classic = (n: string) => JSON.parse(readFileSync(new URL(`./fixtures/tradecpi/${n}.json`, import.meta.url), "utf8"));
const ctxReturnBytes = (hex: string) => Buffer.from(hex.padEnd(640, "0"), "hex");

const f = classic("partial");
const B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
/** The recorded partial-fill tx, re-signed and moved `k` slots later: same matcher context, same req_id. */
function clone(k: number) {
  const tx = JSON.parse(JSON.stringify(enhanced[f.sig]));
  if (k > 0) {
    const last = f.sig[f.sig.length - 1];
    tx.signature = f.sig.slice(0, -1) + B58[(B58.indexOf(last) + k) % 58];
    tx.slot += k;
  }
  return tx;
}
const N = 5;
const delivery = () => Array.from({ length: N }, (_, k) => clone(k));
const reads = () => getAccountInfo.mock.calls.length + getAccountInfoAndContext.mock.calls.length;
const data = () => ({ data: ctxReturnBytes(f.ctxReturnHex) });

async function post(txs: unknown[]) {
  const res = await webhookRoutes().fetch(new Request("http://x/webhook/trades", { method: "POST", headers: { "content-type": "application/json", authorization: "s3cret" }, body: JSON.stringify(txs) }));
  return res.status;
}
const rows = () => vi.mocked(insertTradeRows).mock.calls.flatMap((c) => c[0] as any[]).filter((r) => !r.is_liquidation);
const strip = (r: any) => { const { inserted_at, created_at, ...rest } = r; return rest; };

describe("webhook D1: one matcher-context read per context per delivery", () => {
  beforeEach(() => {
    vi.mocked(insertTradeRows).mockClear();
    getAccountInfo.mockReset().mockResolvedValue(data());
    getAccountInfoAndContext.mockReset().mockImplementation(async () => ({ context: { slot: enhanced[f.sig].slot + 1000 }, value: data() }));
  });

  it(`${N} CPI fills on one context in one delivery cost 1 read (was ${N}); rows equal one-tx-per-delivery`, async () => {
    expect(await post(delivery())).toBe(200);
    expect(reads()).toBe(1);
    const batched = rows().map(strip);
    expect(batched).toHaveLength(N);
    expect(batched.every((r) => r.size_provenance === "matcher_exact" && r.size === "483")).toBe(true);

    // Reference: each tx in its own delivery (a fresh read per tx, as before).
    vi.mocked(insertTradeRows).mockClear();
    getAccountInfoAndContext.mockClear();
    for (const tx of delivery()) expect(await post([tx])).toBe(200);
    expect(reads()).toBe(N);
    expect(rows().map(strip)).toEqual(batched);
  });

  it("a read answered at a slot before a later tx is not reused for it", async () => {
    // The node answers at the first tx's slot + 1: valid for clones 0 and 1, not for 2..4.
    getAccountInfoAndContext.mockImplementation(async () => ({ context: { slot: enhanced[f.sig].slot + 1 }, value: data() }));
    expect(await post(delivery())).toBe(200);
    expect(reads()).toBe(1 + (N - 2));
  });

  it("a failed read is not memoised: the delivery answers 500 and nothing is reused", async () => {
    getAccountInfoAndContext.mockRejectedValue(new Error("429"));
    expect(await post(delivery())).toBe(500);
    expect(rows()).toEqual([]);
    expect(reads()).toBe(N); // every tx tried its own read
  });
});
