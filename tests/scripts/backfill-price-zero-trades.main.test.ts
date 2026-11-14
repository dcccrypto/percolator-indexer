/**
 * Behaviour tests for the price=0 backfill's main(): which rows it selects,
 * what it writes, and how it copes with Helius and DB failures.
 * Supabase, Helius (global fetch) and the SDK layout lookup are all mocked.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

interface UpdateCall {
  patch: any;
  filters: [string, any][];
}

let trades: any[] = [];
let selectError: any = null;
let updateCalls: UpdateCall[] = [];
let selectRanges: [number, number][] = [];
const SERVER_MAX_ROWS = 1000;
let updateOutcome: (call: UpdateCall) => Promise<any> | any = () => ({ error: null });
let remaining: number | null = 0;
let countError: any = null;
let heliusKey = "test-key";
let rpcUrl = "https://api.devnet.solana.com";

function makeSupabase() {
  return {
    from: (_t: string) => ({
      select: (_cols: string, opts?: any) => {
        if (opts?.head) {
          const q: any = {
            eq: () => q,
            then: (res: any) => res({ count: remaining, error: countError }),
          };
          return q;
        }
        // Like PostgREST, an un-ranged select returns at most 1000 rows.
        const page = (from: number, to: number) => {
          selectRanges.push([from, to]);
          return Promise.resolve({
            data: trades === null ? null : trades.slice(from, to + 1),
            error: selectError,
          });
        };
        const q: any = {
          eq: () => q,
          not: () => q,
          order: () => q,
          range: (from: number, to: number) => page(from, to),
          then: (res: any, rej: any) => page(0, SERVER_MAX_ROWS - 1).then(res, rej),
        };
        return q;
      },
      update: (patch: any) => {
        const call: UpdateCall = { patch, filters: [] };
        updateCalls.push(call);
        const q: any = {
          eq: (c: string, v: any) => {
            call.filters.push([c, v]);
            return q;
          },
          select: () => q,
          then: (res: any, rej: any) =>
            Promise.resolve()
              .then(() => updateOutcome(call))
              .then((out) => {
                // default: the update matched the row it targeted
                if (out && out.data === undefined && !out.error) {
                  const id = call.filters.find(([k]) => k === "id")?.[1];
                  return { ...out, data: [{ id }] };
                }
                return out;
              })
              .then(res, rej),
        };
        return q;
      },
    }),
  };
}

vi.mock("@percolatorct/shared", () => ({
  getSupabase: () => makeSupabase(),
  getNetwork: () => "devnet",
  config: {
    get heliusApiKey() {
      return heliusKey;
    },
    get rpcUrl() {
      return rpcUrl;
    },
  },
}));

vi.mock("@percolatorct/sdk", () => ({
  detectSlabLayout: (len: number) => {
    if (len === 2048) return { engineOff: 640, engineMarkPriceOff: 400, configMarkEwmaOff: null };
    if (len === 1024) return { engineOff: 0, engineMarkPriceOff: -1, configMarkEwmaOff: 100 };
    if (len === 512) return { engineOff: 0, engineMarkPriceOff: 508, configMarkEwmaOff: null };
    return null;
  },
}));

const SLAB = "SlabAddress1111111111111111111111111111111";

function blob(len: number, off: number, v: bigint): string {
  const raw = new Uint8Array(len);
  new DataView(raw.buffer).setBigUint64(off, v, true);
  return Buffer.from(raw).toString("base64");
}

function enhanced(sig: string, data: any) {
  return { signature: sig, accountData: [{ account: SLAB, data }] };
}

function trade(id: string, sig: string, slab = SLAB) {
  return { id, slab_address: slab, tx_signature: sig, price: 0 };
}

let fetchMock: ReturnType<typeof vi.fn>;
let logs: string[];

function jsonResponse(body: any) {
  return { ok: true, status: 200, json: async () => body, text: async () => "" };
}

async function loadMain(argv: string[] = []) {
  vi.resetModules();
  process.argv = ["node", "test-runner", ...argv];
  return (await import("../../src/scripts/backfill-price-zero-trades.js")) as any;
}

const origArgv = process.argv;

beforeEach(() => {
  trades = [];
  selectError = null;
  updateCalls = [];
  selectRanges = [];
  updateOutcome = () => ({ error: null });
  remaining = 0;
  countError = null;
  heliusKey = "test-key";
  rpcUrl = "https://api.devnet.solana.com";
  logs = [];
  fetchMock = vi.fn();
  vi.stubGlobal("fetch", fetchMock);
  for (const m of ["log", "warn", "error"] as const) {
    vi.spyOn(console, m).mockImplementation((...a: any[]) => {
      logs.push(a.join(" "));
    });
  }
});

afterEach(() => {
  process.argv = origArgv;
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("backfill main()", () => {
  it("refuses to run without a Helius key", async () => {
    heliusKey = "";
    const { main } = await loadMain();
    await expect(main()).rejects.toThrow("HELIUS_API_KEY is required");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("surfaces a failed trades query", async () => {
    selectError = new Error("db down");
    const { main } = await loadMain();
    await expect(main()).rejects.toThrow("db down");
  });

  it("does nothing when no price=0 trades exist", async () => {
    const { main } = await loadMain();
    await main();
    expect(fetchMock).not.toHaveBeenCalled();
    expect(updateCalls).toHaveLength(0);
    expect(logs.join("\n")).toContain("Nothing to backfill");
  });

  it("treats a null data result as empty", async () => {
    trades = null as any;
    const { main } = await loadMain();
    await main();
    expect(logs.join("\n")).toContain("Nothing to backfill");
  });

  it("updates each trade sharing a tx and only overwrites rows still at 0", async () => {
    trades = [trade("t1", "sigA"), trade("t2", "sigA"), trade("t3", "sigB")];
    fetchMock.mockResolvedValue(
      jsonResponse([
        enhanced("sigA", blob(2048, 1040, 2_500_000n)),
        // array-form data with the V1.x layout (engine field absent, ewma used)
        enhanced("sigB", [blob(1024, 100, 3_000_000n), "base64"]),
      ]),
    );
    remaining = 0;
    const { main } = await loadMain();
    await main();

    // one batched POST for the three trades over two unique signatures
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const body = JSON.parse(fetchMock.mock.calls[0][1].body);
    expect(body.transactions).toEqual(["sigA", "sigB"]);
    expect(fetchMock.mock.calls[0][0]).toContain("api-devnet.helius-rpc.com");

    const byId = updateCalls.map((c) => [c.filters.find(([k]) => k === "id")![1], c.patch.price]);
    expect(byId.sort()).toEqual([["t1", 2.5], ["t2", 2.5], ["t3", 3]]);
    for (const c of updateCalls) {
      expect(c.filters).toContainEqual(["network", "devnet"]);
      expect(c.filters).toContainEqual(["price", 0]);
    }
    expect(logs.join("\n")).toContain("3 updated, 0 DB error(s), 0 skipped");
  });

  it("uses the mainnet Helius host when the RPC is not devnet", async () => {
    rpcUrl = "https://api.mainnet-beta.solana.com";
    trades = [trade("t1", "sigA")];
    fetchMock.mockResolvedValue(jsonResponse([enhanced("sigA", blob(2048, 1040, 1_000_000n))]));
    const { main } = await loadMain();
    await main();
    expect(fetchMock.mock.calls[0][0]).toContain("api-mainnet.helius-rpc.com");
  });

  it("dry run reports prices but writes nothing", async () => {
    trades = [trade("t1", "sigA")];
    fetchMock.mockResolvedValue(jsonResponse([enhanced("sigA", blob(2048, 1040, 2_000_000n))]));
    const { main } = await loadMain(["--dry-run"]);
    await main();
    expect(updateCalls).toHaveLength(0);
    expect(logs.join("\n")).toContain("DRY RUN — no DB writes performed");
  });

  it("skips trades whose tx Helius did not return or has no usable slab data", async () => {
    trades = [
      trade("missing", "sigMissing"),
      trade("nodata", "sigNoData"),
      trade("garbage", "sigGarbage"),
      trade("othersl", "sigOther", "OtherSlab"),
      trade("tiny", "sigTiny"),
      trade("zero", "sigZero"),
      trade("huge", "sigHuge"),
      trade("unknownlen", "sigLen"),
      trade("nonstr", "sigNonStr"),
    ];
    fetchMock.mockResolvedValue(
      jsonResponse([
        { signature: "sigNoData", accountData: [{ account: SLAB }] },
        { signature: "sigGarbage", accountData: [{ account: SLAB, data: 42 }] },
        enhanced("sigOther", blob(2048, 1040, 2_000_000n)),
        // layout whose mark-price offset runs past the buffer end
        enhanced("sigTiny", blob(512, 0, 1n)),
        enhanced("sigZero", blob(2048, 1040, 0n)),
        enhanced("sigHuge", blob(2048, 1040, 1_000_000_000_000n)),
        enhanced("sigLen", blob(333, 0, 5n)),
        { signature: "sigNonStr", accountData: [{ account: SLAB, data: [7] }] },
        { noSignature: true },
        null,
      ]),
    );
    const { main } = await loadMain();
    await main();
    expect(updateCalls).toHaveLength(0);
    expect(logs.join("\n")).toContain("0 to update, 9 skipped");
    expect(logs.join("\n")).toContain("Nothing to update");
  });

  it("splits more than 100 signatures into batches and survives a failed batch", async () => {
    trades = Array.from({ length: 150 }, (_, i) => trade(`t${i}`, `sig${i}`));
    fetchMock
      .mockResolvedValueOnce(
        jsonResponse(
          Array.from({ length: 100 }, (_, i) => enhanced(`sig${i}`, blob(2048, 1040, 1_000_000n))),
        ),
      )
      .mockResolvedValueOnce({ ok: false, status: 429, text: async () => "slow down" });
    const { main } = await loadMain();
    await main();
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(updateCalls).toHaveLength(100);
    const out = logs.join("\n");
    expect(out).toContain("Helius 429: slow down");
    expect(out).toContain("1 batch failure(s)");
    expect(out).toContain("100 updated");
    expect(out).toContain("50 skipped");
  });

  it("paces concurrent batches (more than 3 chunks)", async () => {
    vi.useFakeTimers();
    try {
      trades = Array.from({ length: 301 }, (_, i) => trade(`t${i}`, `sig${i}`));
      fetchMock.mockImplementation(async (_u: string, init: any) => {
        const sigs: string[] = JSON.parse(init.body).transactions;
        return jsonResponse(sigs.map((s) => enhanced(s, blob(2048, 1040, 1_000_000n))));
      });
      const { main } = await loadMain();
      const p = main();
      await vi.advanceTimersByTimeAsync(0);
      expect(fetchMock).toHaveBeenCalledTimes(3); // 4th chunk waits for the 200 ms pause
      await vi.advanceTimersByTimeAsync(200);
      await p;
      expect(fetchMock).toHaveBeenCalledTimes(4);
      expect(updateCalls).toHaveLength(301);
    } finally {
      vi.useRealTimers();
    }
  });

  it("redacts the api key when the transport fails or the body is unreadable", async () => {
    trades = [trade("t1", "sigA"), trade("t2", "sigB")];
    // 1 chunk => one fetch only; exercise transport error first
    fetchMock.mockRejectedValueOnce(new Error("connect ECONNRESET https://x/v0?api-key=SECRET123 end"));
    let mod = await loadMain();
    await mod.main();
    let out = logs.join("\n");
    expect(out).toContain("api-key=REDACTED");
    expect(out).not.toContain("SECRET123");
    expect(out).not.toContain("test-key");

    // non-Error rejection, and a non-ok response whose body cannot be read
    logs.length = 0;
    fetchMock.mockRejectedValueOnce("plain string api-key=SECRET456");
    mod = await loadMain();
    await mod.main();
    expect(logs.join("\n")).not.toContain("SECRET456");
    expect(logs.join("\n")).toContain("Helius batch fetch failed");

    logs.length = 0;
    fetchMock.mockResolvedValueOnce({
      ok: false,
      status: 500,
      text: async () => {
        throw new Error("boom");
      },
    });
    mod = await loadMain();
    await mod.main();
    expect(logs.join("\n")).toContain("Helius 500: (no body)");
  });

  it("counts rejected and error-returning updates without aborting the rest", async () => {
    trades = [trade("t1", "sigA"), trade("t2", "sigB"), trade("t3", "sigC"), trade("t4", "sigD")];
    fetchMock.mockResolvedValue(
      jsonResponse(["sigA", "sigB", "sigC", "sigD"].map((s) => enhanced(s, blob(2048, 1040, 1_000_000n)))),
    );
    let n = 0;
    updateOutcome = () => {
      n++;
      if (n === 1) throw new Error("conn lost");
      if (n === 2) throw "bare rejection";
      if (n === 3) return { error: { message: "rls denied" } };
      return { error: null };
    };
    remaining = 3;
    const { main } = await loadMain();
    await main();
    const out = logs.join("\n");
    expect(out).toContain("DB update failed: conn lost");
    expect(out).toContain("DB update failed: bare rejection");
    expect(out).toContain("DB error: rls denied");
    expect(out).toContain("1 updated, 3 DB error(s)");
    expect(out).toContain("Remaining price=0 rows in DB: 3");
    expect(out).toContain("manual recovery");
  });

  it("reports when the verification count fails ", async () => {
    trades = [trade("t1", "sigA")];
    fetchMock.mockResolvedValue(jsonResponse([enhanced("sigA", blob(2048, 1040, 1_000_000n))]));
    countError = { message: "count failed" };
    remaining = null;
    const { main } = await loadMain();
    await main();
    const out = logs.join("\n");
    expect(out).toContain("Could not verify remaining zeros: count failed");
    expect(out).not.toContain("Remaining price=0 rows");
  });

  it("pages through more than 1000 price=0 rows (2500 mocked)", async () => {
    trades = Array.from({ length: 2500 }, (_, i) => trade(`t${i}`, `sig${i}`));
    fetchMock.mockImplementation(async (_u: string, init: any) => {
      const sigs: string[] = JSON.parse(init.body).transactions;
      return jsonResponse(sigs.map((s) => enhanced(s, blob(2048, 1040, 1_000_000n))));
    });
    const { main } = await loadMain();
    await main();
    const out = logs.join("\n");
    expect(out).toContain("Found 2500 price=0 trade(s)");
    expect(updateCalls).toHaveLength(2500);
    expect(new Set(updateCalls.map((c) => c.filters.find(([k]) => k === "id")![1])).size).toBe(2500);
    expect(out).toContain("2500 updated");
  }, 60_000);

  it("stops paging after a short final page", async () => {
    trades = Array.from({ length: 1000 }, (_, i) => trade(`t${i}`, `sig${i}`));
    fetchMock.mockImplementation(async (_u: string, init: any) => {
      const sigs: string[] = JSON.parse(init.body).transactions;
      return jsonResponse(sigs.map((s) => enhanced(s, blob(2048, 1040, 1_000_000n))));
    });
    const { main } = await loadMain();
    await main();
    // exactly one full page, then an empty page proves there is no more
    expect(selectRanges).toEqual([[0, 999], [1000, 1999]]);
    expect(updateCalls).toHaveLength(1000);
  }, 60_000);

  it("does not count an update that matched no row as fixed", async () => {
    trades = [trade("t1", "sigA"), trade("t2", "sigB"), trade("t3", "sigC")];
    fetchMock.mockResolvedValue(
      jsonResponse(["sigA", "sigB", "sigC"].map((s) => enhanced(s, blob(2048, 1040, 1_000_000n)))),
    );
    // t2 was already fixed by someone else: the price=0 guard matches nothing
    updateOutcome = (call) => {
      const id = call.filters.find(([k]) => k === "id")![1];
      return { error: null, data: id === "t2" ? [] : [{ id }] };
    };
    const { main } = await loadMain();
    await main();
    const out = logs.join("\n");
    expect(out).toContain("2 updated, 0 DB error(s), 0 skipped");
    expect(out).toContain("1 matched no row");
  });

  it("does not auto-run when imported", async () => {
    await loadMain();
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
