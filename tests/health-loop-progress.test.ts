/**
 * HEALTH-STALL (J-20261123-IDX-REPORT-HEALTHWIRINGSTALLLIVENESS): /health must turn unhealthy when a polling loop
 * runs but makes no progress, not only when it hangs.
 *
 * Measured at 2395015 with the real src/index.ts, TradeIndexerPolling, StatsCollector, healthCache, loopLiveness and
 * rpcDeadline (fake RPC, fake Supabase, fake timers): with every trade-poll read held (getSignaturesForAddress never
 * answers, the 60 s per-call deadline rejects it), or with every StatsCollector tick throwing, /health stayed 200 ok
 * for 30 fake minutes. Both loops called loopLiveness.beat() in a `finally` (and the trade poll also after a market
 * whose reads had failed), so a failed pass counted as progress. A loop now beats only when a pass made progress.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const h = vi.hoisted(() => {
  const noop = () => {};
  let seq = 0;
  return {
    logger: { info: noop, warn: noop, error: noop, debug: noop },
    fault: "none" as "none" | "trade-reads-hang" | "stats-throws",
    marketRows: true,
    slab: "",
    fetch: null as null | ((req: Request) => Promise<Response>),
    nextSig: () => `fakesig${++seq}`.padEnd(88, "1"),
  };
});

function fakeConnection(): object {
  const held = () => new Promise(() => {});
  const impl: Record<string, (...a: any[]) => Promise<unknown>> = {
    getSlot: async () => 123,
    getSignaturesForAddress: async () => {
      if (h.fault === "trade-reads-hang") return held();
      return [{ signature: h.nextSig(), slot: 1000, err: null, blockTime: Math.floor(Date.now() / 1000), memo: null }];
    },
    getParsedTransactions: async (sigs: string[]) => {
      if (h.fault === "trade-reads-hang") return held();
      return sigs.map((s) => ({
        slot: 1000, blockTime: Math.floor(Date.now() / 1000),
        transaction: { signatures: [s], message: { accountKeys: [], instructions: [] } },
        meta: { err: null, logMessages: [], innerInstructions: [] },
      }));
    },
    getMultipleAccountsInfo: async (keys: unknown[]) => keys.map(() => null),
  };
  return new Proxy({ rpcEndpoint: "http://fake-rpc.invalid", commitment: "confirmed" } as Record<string, unknown>, {
    get(t, p) {
      if (p in t) return t[p as string];
      if (p === "then") return undefined;
      return (typeof p === "string" && impl[p]) || (async () => null);
    },
  });
}

/** Chainable PostgREST-like builder; `markets` reads return the one fake market (first keyset page only). */
function fakeSupabase(): object {
  const builder = (table: string) => {
    const st = { gt: false, head: false };
    const b: any = new Proxy({}, {
      get(_t, p) {
        if (p === "then") {
          return (res: (v: unknown) => void) => {
            if (table === "markets" && h.marketRows && !st.gt && !st.head) {
              return res({ data: [{ slab_address: h.slab, indexer_excluded: false, keeper_status: null }], error: null });
            }
            return res({ data: st.head ? null : [], error: null, count: 1 });
          };
        }
        return (...args: any[]) => {
          if (p === "gt") st.gt = true;
          if (p === "select" && args[1]?.head) st.head = true;
          return b;
        };
      },
    });
    return b;
  };
  return { from: (t: string) => builder(t), rpc: () => builder("rpc") };
}
const conn = fakeConnection();
const supa = fakeSupabase();

vi.mock("dotenv/config", () => ({}));
vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return {
    ...actual,
    config: { ...actual.config, webhookUrl: "", rpcUrl: "", programId: "11111111111111111111111111111111", allProgramIds: ["11111111111111111111111111111111"] },
    createLogger: () => h.logger,
    initSentry: () => {},
    captureException: () => {},
    addBreadcrumb: () => {},
    getSupabase: () => supa,
    getConnection: () => conn,
    withRetry: async (fn: () => Promise<unknown>) => fn(),
    sendCriticalAlert: async () => {},
    sendInfoAlert: async () => {},
  };
});
vi.mock("@hono/node-server", () => ({
  serve: (opts: { fetch: (req: Request) => Promise<Response> }, cb?: (i: { port: number }) => void) => {
    h.fetch = opts.fetch;
    cb?.({ port: 3002 });
    return { close: () => {} };
  },
}));
vi.mock("../src/services/MarketDiscovery.js", () => ({
  MarketDiscovery: class {
    getMarkets() {
      if (h.fault === "stats-throws") throw new Error("fake: every stats tick throws");
      return new Map([[h.slab, {}]]);
    }
    stop() {}
  },
}));
vi.mock("../src/services/discoveryWiring.js", () => ({ startDiscovery: async () => {} }));
vi.mock("../src/services/HeliusWebhookManager.js", () => ({ HeliusWebhookManager: class { async start() {} stop() {} } }));
vi.mock("../src/lib/skippedSignatures.js", () => ({ assertSkippedSignatureSinkReady: () => {}, recordSkippedSignatures: async () => {} }));

let added: Function[] = [];

async function loadIndex(): Promise<void> {
  const before = process.listeners("unhandledRejection");
  const { Keypair } = await import("@solana/web3.js");
  h.slab = Keypair.generate().publicKey.toBase58();
  await import("../src/index.ts");
  await vi.advanceTimersByTimeAsync(0);
  added = process.listeners("unhandledRejection").filter((l) => !before.includes(l));
}

async function health(): Promise<{ code: number; status: string }> {
  // The probes time out on (fake) timers, so keep the clock moving while the request is open.
  let out: { code: number; status: string } | null = null;
  const p = h.fetch!(new Request("http://localhost/health")).then(async (r) => {
    out = { code: r.status, status: ((await r.json()) as { status: string }).status };
  });
  for (let i = 0; i < 60 && !out; i++) await vi.advanceTimersByTimeAsync(250);
  await p;
  return out!;
}

/** Advance `min` fake minutes, sampling /health every 15 s; returns the seconds until the first non-200, or null. */
async function secondsUntilUnhealthy(min: number): Promise<number | null> {
  for (let s = 15; s <= min * 60; s += 15) {
    await vi.advanceTimersByTimeAsync(15_000);
    if ((await health()).code !== 200) return s;
  }
  return null;
}

beforeEach(() => {
  vi.resetModules();
  vi.useFakeTimers({ now: new Date("2026-11-23T00:00:00Z") });
  vi.stubEnv("NETWORK", "devnet");
  vi.stubEnv("NODE_ENV", "test");
  vi.stubEnv("HEALTH_CACHE_TTL_MS", "0");
  vi.stubEnv("LP_VAULT_INDEXER_ENABLED", "false");
  vi.stubEnv("HELIUS_ATLAS_WS_URL", "");
  vi.stubEnv("SENTRY_DSN", "");
  h.fault = "none";
  h.marketRows = true;
  h.fetch = null;
});

afterEach(() => {
  vi.clearAllTimers();
  for (const l of added) process.removeListener("unhandledRejection", l as never);
  added = [];
  process.removeAllListeners("SIGTERM");
  process.removeAllListeners("SIGINT");
  vi.useRealTimers();
  vi.unstubAllEnvs();
});

// The first test pays the real index.ts import, so the default 10 s is too tight under load.
describe("HEALTH-STALL: /health reflects loop progress, not only loop activity", { timeout: 120_000 }, () => {
  it("control: a healthy indexer stays 200 ok for 30 minutes", async () => {
    await loadIndex();
    expect(await secondsUntilUnhealthy(30)).toBeNull();
  });

  it("control: no markets to poll is not a stall (200 ok for 30 minutes)", async () => {
    h.marketRows = false;
    await loadIndex();
    expect(await secondsUntilUnhealthy(30)).toBeNull();
  });

  it("every trade-poll read held (deadline rejects each): 503 degraded within the 15-min budget, ok again after", async () => {
    await loadIndex();
    expect(await secondsUntilUnhealthy(3)).toBeNull(); // warm: first passes ran
    h.fault = "trade-reads-hang";
    // Before the fix: null after 30 minutes (each failed pass beat in `finally`, and each failed market beat too).
    const t = await secondsUntilUnhealthy(30);
    expect(t).not.toBeNull();
    expect(t!).toBeLessThanOrEqual(15 * 60 + 60);
    expect((await health()).status).toBe("degraded");
    h.fault = "none";
    await vi.advanceTimersByTimeAsync(6 * 60_000); // the next 5-min poll pass succeeds
    expect(await health()).toEqual({ code: 200, status: "ok" });
  });

  it("every StatsCollector tick throws: 503 degraded within the 10-min budget, ok again after", async () => {
    await loadIndex();
    expect(await secondsUntilUnhealthy(3)).toBeNull();
    h.fault = "stats-throws";
    // Before the fix: null after 30 minutes (collect() beat in `finally` even when it threw).
    const t = await secondsUntilUnhealthy(30);
    expect(t).not.toBeNull();
    expect(t!).toBeLessThanOrEqual(10 * 60 + 60);
    expect((await health()).status).toBe("degraded");
    h.fault = "none";
    await vi.advanceTimersByTimeAsync(2 * 60_000);
    expect(await health()).toEqual({ code: 200, status: "ok" });
  });
});
