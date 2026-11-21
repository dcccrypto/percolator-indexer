/**
 * D-1 (J-20261121-IDX-FIX-DBMONITORERROR): the DB monitor in src/index.ts must
 * alert when the DB is lost.
 *
 * supabase-js resolves `{ data: null, error }` instead of throwing when the DB
 * or PostgREST is down. The old monitor only had a catch, so it stayed quiet.
 * This test imports the REAL src/index.ts (every service, the HTTP server and
 * @percolatorct/shared mocked) and drives its 30 s interval with fake timers.
 *
 * It also pins the explicit unhandledRejection policy: index.ts installs its
 * own listener, so a stray rejection is logged and the process stays up whether
 * or not SENTRY_DSN is set.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const h = vi.hoisted(() => {
  const logger = { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() };
  return {
    logger,
    probe: vi.fn(),
    sendCriticalAlert: vi.fn(async () => {}),
    sendInfoAlert: vi.fn(async () => {}),
    captureException: vi.fn(),
    initSentry: vi.fn(),
    config: { webhookUrl: "", rpcUrl: "", programId: "FAKE", allProgramIds: [] as string[] },
    getSlot: vi.fn(async () => 1),
    startupQuery: vi.fn(async (): Promise<{ data: unknown; error: unknown }> => ({ data: [], error: null })),
    serve: vi.fn(),
    fetch: null as null | ((req: Request) => Promise<Response>),
    eventStreamStart: vi.fn(async () => {}),
    addKnownSlab: vi.fn(),
    createAtlasWs: vi.fn(() => ({})),
  };
});

vi.mock("dotenv/config", () => ({}));
vi.mock("@percolatorct/shared", () => ({
  get config() { return h.config; },
  createLogger: () => h.logger,
  initSentry: h.initSentry,
  captureException: h.captureException,
  getSupabase: () => ({
    from: () => ({
      // monitor: select("id", { head: true }) is awaited directly; startup: select(...).limit(1)
      select: () => {
        const p = h.probe();
        return Object.assign(Promise.resolve(p), { limit: () => h.startupQuery() });
      },
    }),
  }),
  getConnection: () => ({ getSlot: h.getSlot }),
  sendCriticalAlert: h.sendCriticalAlert,
  sendInfoAlert: h.sendInfoAlert,
  createAtlasWs: h.createAtlasWs,
}));
vi.mock("@hono/node-server", () => ({
  serve: (opts: { fetch: (req: Request) => Promise<Response>; port: number }, cb?: (info: { port: number }) => void) => {
    h.fetch = opts.fetch;
    h.serve(opts.port);
    cb?.({ port: opts.port });
    return { close: vi.fn() };
  },
}));
vi.mock("../src/services/MarketDiscovery.js", () => ({
  MarketDiscovery: class { getMarkets() { return new Map([["FAKE_SLAB", {}]]); } stop() {} },
}));
vi.mock("../src/services/discoveryWiring.js", () => ({ startDiscovery: vi.fn(async () => {}) }));
vi.mock("../src/services/StatsCollector.js", () => ({ StatsCollector: class { start() {} stop() {} } }));
vi.mock("../src/services/TradeIndexer.js", () => ({ TradeIndexerPolling: class { start() {} stop() {} } }));
vi.mock("../src/services/HeliusWebhookManager.js", () => ({
  HeliusWebhookManager: class { async start() {} async stop() {} },
}));
vi.mock("../src/services/EventStreamService.js", () => ({
  EventStreamService: class { start() { return h.eventStreamStart(); } addKnownSlab(s: string) { h.addKnownSlab(s); } },
}));
vi.mock("../src/services/LpVaultIndexer.js", () => ({ LpVaultIndexer: class { start() {} stop() {} } }));
vi.mock("../src/lpVault/chain.js", () => ({ RpcLpVaultChain: class {} }));
vi.mock("../src/lpVault/store.js", () => ({ SupabaseLpVaultStore: class {} }));
vi.mock("../src/routes/webhook.js", async () => {
  const { Hono } = await import("hono");
  return { webhookRoutes: () => new Hono() };
});
vi.mock("../src/lib/skippedSignatures.js", () => ({ assertSkippedSignatureSinkReady: () => {} }));

const MONITOR_MS = 30_000;
const FAKE_PG_ERROR = { message: "FAKE fetch failed", code: "FAKE_CODE", details: null, hint: null };

let listenersBefore: Function[];
let added: Function[] = [];

async function loadIndex(): Promise<void> {
  listenersBefore = process.listeners("unhandledRejection");
  await import("../src/index.ts");
  // let start() run to its end (all awaited mocks resolve immediately)
  await vi.advanceTimersByTimeAsync(0);
  added = process.listeners("unhandledRejection").filter((l) => !listenersBefore.includes(l));
}

const lostAlerts = () =>
  h.sendCriticalAlert.mock.calls.filter((c) => String(c[0]).includes("database connection lost"));
const restoredAlerts = () =>
  h.sendInfoAlert.mock.calls.filter((c) => String(c[0]).includes("database connection restored"));
const checkFailedLogs = () =>
  h.logger.error.mock.calls.filter((c) => c[0] === "DB connection check failed");

beforeEach(async () => {
  vi.resetModules();
  vi.clearAllMocks();
  vi.useFakeTimers();
  vi.stubEnv("LP_VAULT_INDEXER_ENABLED", "false");
  vi.stubEnv("HELIUS_ATLAS_WS_URL", "");
  vi.stubEnv("NODE_ENV", "test");
  h.probe.mockReturnValue({ count: 3, data: null, error: null });
  h.config = { webhookUrl: "", rpcUrl: "", programId: "FAKE", allProgramIds: [] };
  h.fetch = null;
});

function afterEachCleanup(): void {
  vi.clearAllTimers();
  for (const l of added) process.removeListener("unhandledRejection", l as never);
  added = [];
  process.removeAllListeners("SIGTERM");
  process.removeAllListeners("SIGINT");
}

afterEach(() => {
  afterEachCleanup();
  vi.useRealTimers();
  vi.unstubAllEnvs();
});

describe("D-1: index.ts DB monitor", () => {
  it("alerts 'connection lost' once when supabase resolves { error }, and 'restored' when it recovers", async () => {
    await loadIndex();
    h.probe.mockReturnValue({ count: null, data: null, error: FAKE_PG_ERROR, status: 0 });

    await vi.advanceTimersByTimeAsync(MONITOR_MS);
    expect.soft(lostAlerts(), "no 'connection lost' alert on a { error } result").toHaveLength(1);
    expect.soft(checkFailedLogs(), "no 'DB connection check failed' log on a { error } result").toHaveLength(1);
    expect.soft(JSON.stringify(lostAlerts()[0]?.[1] ?? [])).toContain("FAKE fetch failed");

    // Still down: logs every tick, alerts only once.
    await vi.advanceTimersByTimeAsync(MONITOR_MS);
    expect.soft(lostAlerts()).toHaveLength(1);
    expect.soft(checkFailedLogs()).toHaveLength(2);

    // Back up: exactly one restored alert.
    h.probe.mockReturnValue({ count: 3, data: null, error: null, status: 200 });
    await vi.advanceTimersByTimeAsync(MONITOR_MS);
    await vi.advanceTimersByTimeAsync(MONITOR_MS);
    expect(restoredAlerts()).toHaveLength(1);
  });

  it("alerts when the probe hangs past its timeout (a hung fetch must not silence the monitor)", async () => {
    await loadIndex();
    h.probe.mockReturnValue(new Promise(() => {}));
    await vi.advanceTimersByTimeAsync(MONITOR_MS + 10_000);
    expect(lostAlerts()).toHaveLength(1);
    expect(JSON.stringify(lostAlerts()[0]?.[1] ?? [])).toContain("timed out");
  });

  it("still alerts when the probe throws (pre-existing path)", async () => {
    await loadIndex();
    h.probe.mockImplementation(() => {
      throw new Error("FAKE thrown");
    });
    await vi.advanceTimersByTimeAsync(MONITOR_MS);
    expect(lostAlerts()).toHaveLength(1);
  });

  it("stays quiet while the DB is healthy", async () => {
    await loadIndex();
    await vi.advanceTimersByTimeAsync(MONITOR_MS * 3);
    expect(lostAlerts()).toHaveLength(0);
    expect(restoredAlerts()).toHaveLength(0);
    expect(checkFailedLogs()).toHaveLength(0);
  });
});

describe("explicit unhandledRejection policy", () => {
  it("index.ts installs its own listener (so exit-vs-stay-up no longer depends on SENTRY_DSN), which logs and does not exit", async () => {
    const exitSpy = vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);
    try {
      await loadIndex();
      expect(added, "index.ts registered no unhandledRejection listener").toHaveLength(1);

      const cyclic: Record<string, unknown> = { name: "FAKE" };
      cyclic.self = cyclic;
      for (const reason of [new Error("FAKE stray rejection"), cyclic, undefined]) {
        expect(() => (added[0] as (r: unknown) => void)(reason)).not.toThrow();
      }
      const logged = h.logger.error.mock.calls.filter((c) => String(c[0]).includes("Unhandled promise rejection"));
      expect(logged).toHaveLength(3);
      expect(logged[0][1]).toMatchObject({ message: "FAKE stray rejection" });
      expect(exitSpy).not.toHaveBeenCalled();
    } finally {
      exitSpy.mockRestore();
    }
  });
});

// Boot, HTTP surface and shutdown of the real index.ts (same mocks). These exist so importing
// index.ts above does not leave most of it unmeasured; each asserts behaviour, not just lines.
const errorLogs = (needle: string) => h.logger.error.mock.calls.filter((c) => String(c[0]).includes(needle));
const warnLogs = (needle: string) => h.logger.warn.mock.calls.filter((c) => String(c[0]).includes(needle));
const req = (path: string, init?: RequestInit) => h.fetch!(new Request(`http://indexer.test${path}`, init));

describe("index.ts boot", () => {
  it("starts every enabled service and the HTTP server, then sends the startup alert", async () => {
    vi.stubEnv("LP_VAULT_INDEXER_ENABLED", "true");
    vi.stubEnv("INDEXER_RPC_POLLING_ENABLED", "false");
    vi.stubEnv("HELIUS_ATLAS_WS_URL", "wss://atlas.fake");
    vi.stubEnv("INDEXER_PORT", "99999");
    vi.stubEnv("TRUSTED_PROXY_DEPTH", "not-a-number");
    await loadIndex();
    expect(h.serve).toHaveBeenCalledWith(3002);
    expect(warnLogs("Invalid INDEXER_PORT")).toHaveLength(1);
    expect(h.logger.error.mock.calls.some((c) => String(c[0]).includes("TRUSTED_PROXY_DEPTH"))).toBe(true);
    expect(h.eventStreamStart).toHaveBeenCalledTimes(1);
    expect(h.logger.info.mock.calls.some((c) => c[0] === "RPC fallback pollers disabled")).toBe(true);
    expect(h.sendInfoAlert.mock.calls.some((c) => c[0] === "Indexer service started")).toBe(true);
    expect(h.addKnownSlab).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(60_000); // known-slab refresh: discovery's slabs reach the event stream
    expect(h.addKnownSlab).toHaveBeenCalledWith("FAKE_SLAB");
  });

  it("keeps running with webhook + polling when the Atlas WS fails to start", async () => {
    vi.stubEnv("HELIUS_ATLAS_WS_URL", "wss://atlas.fake");
    h.eventStreamStart.mockRejectedValueOnce(new Error("FAKE ws down"));
    await loadIndex();
    expect(errorLogs("Atlas WS failed to start")).toHaveLength(1);
    expect(h.captureException).toHaveBeenCalledWith(expect.any(Error), { tags: { context: "atlas-ws-start" } });
    expect(h.serve).toHaveBeenCalled();
  });

  it("logs a startup DB { error } and a startup DB throw without crashing", async () => {
    h.startupQuery.mockResolvedValueOnce({ data: null, error: { message: "FAKE", code: "X" } });
    await loadIndex();
    expect(errorLogs("Supabase connection test FAILED")).toHaveLength(1);

    afterEachCleanup();
    vi.resetModules();
    vi.clearAllMocks();
    h.startupQuery.mockRejectedValueOnce(new Error("FAKE threw"));
    await loadIndex();
    expect(errorLogs("Supabase connection test threw")).toHaveLength(1);
    expect(h.serve).toHaveBeenCalled();
  });

  it.each([
    ["webhookUrl", "http://hook.fake/x", "development", "WEBHOOK_URL uses non-HTTPS", "warn"],
    ["webhookUrl", "not a url", "development", "WEBHOOK_URL is not a valid URL", "error"],
    ["webhookUrl", "http://hook.fake/x", "production", "WEBHOOK_URL must use HTTPS", "fatal"],
    ["rpcUrl", "http://rpc.fake", "development", "RPC_URL uses non-HTTPS", "warn"],
    ["rpcUrl", "not a url", "development", "RPC_URL is not a valid URL", "error"],
    ["rpcUrl", "http://rpc.fake", "production", "RPC_URL must use HTTPS", "fatal"],
  ] as const)("%s=%s under NODE_ENV=%s -> %s", async (key, value, nodeEnv, needle, kind) => {
    vi.stubEnv("NODE_ENV", nodeEnv);
    h.config[key] = value;
    await loadIndex();
    if (kind === "warn") expect(warnLogs(needle)).toHaveLength(1);
    if (kind === "error") expect(errorLogs(needle)).toHaveLength(1);
    if (kind === "fatal") {
      const failed = errorLogs("Failed to start indexer");
      expect(failed).toHaveLength(1);
      expect(String(failed[0][1]?.error)).toContain(needle);
      expect(h.serve).not.toHaveBeenCalled();
    }
  });

  it("an invalid NODE_ENV fails start() but the process stays alive", async () => {
    vi.stubEnv("NODE_ENV", "staging");
    await loadIndex();
    expect(errorLogs("Invalid NODE_ENV configuration")).toHaveLength(1);
    expect(h.captureException).toHaveBeenCalledWith(expect.any(Error), { tags: { context: "indexer-startup" } });
  });
});

describe("index.ts HTTP surface", () => {
  it("/health is 200 when both probes pass and 503 when the RPC probe fails or hangs", async () => {
    vi.stubEnv("HEALTH_CACHE_TTL_MS", "0");
    await loadIndex();
    let res = await req("/health");
    expect(res.status).toBe(200);
    expect(res.headers.get("x-frame-options")).toBe("DENY");
    expect(await res.json()).toMatchObject({ status: "ok" });

    h.getSlot.mockRejectedValueOnce(new Error("FAKE rpc down"));
    await vi.advanceTimersByTimeAsync(1);
    res = await req("/health");
    expect(res.status).toBe(503);
    expect(warnLogs("RPC health check failed")).toHaveLength(1);

    h.getSlot.mockReturnValueOnce(new Promise(() => {}));
    h.probe.mockImplementationOnce(() => Promise.reject(new Error("FAKE db down")));
    await vi.advanceTimersByTimeAsync(1);
    const pending = req("/health");
    await vi.advanceTimersByTimeAsync(6_000);
    res = await pending;
    expect(res.status).toBe(503);
    expect(warnLogs("DB health check failed")).toHaveLength(1);
    expect(String(warnLogs("RPC health check failed")[1]?.[1]?.error)).toContain("timed out");
  });

  it("/webhook/trades rejects non-POST with 405 and rate-limits POSTs", async () => {
    vi.stubEnv("WEBHOOK_RATE_LIMIT_PER_MIN", "1");
    await loadIndex();
    expect((await req("/webhook/trades")).status).toBe(405);
    const first = await req("/webhook/trades", { method: "POST", body: "{}" });
    expect(first.status).not.toBe(429);
    const second = await req("/webhook/trades", { method: "POST", body: "{}" });
    expect(second.status).toBe(429);
    expect(warnLogs("Rate limit exceeded")).toHaveLength(1);
    // After the 60 s window the same client is allowed again. (Coverage-only for the 5-min prune timer:
    // the window expiry alone would let this through, so this does not prove pruning.)
    await vi.advanceTimersByTimeAsync(5 * 60_000);
    expect((await req("/webhook/trades", { method: "POST", body: "{}" })).status).not.toBe(429);
  });
});

describe("index.ts shutdown", () => {
  it("SIGTERM stops services, clears the DB monitor, drains and exits 0; a second signal is ignored", async () => {
    const exitSpy = vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);
    try {
      await loadIndex();
      const onTerm = process.listeners("SIGTERM").at(-1) as () => void;
      onTerm();
      await vi.advanceTimersByTimeAsync(0);
      const onInt = process.listeners("SIGINT").at(-1) as () => void;
      onInt();
      await vi.advanceTimersByTimeAsync(3_000);
      expect(exitSpy).toHaveBeenCalledWith(0);
      expect(warnLogs("Shutdown already in progress")).toHaveLength(1);
      // The monitor interval was cleared: a lost DB after shutdown raises nothing.
      h.probe.mockReturnValue({ error: FAKE_PG_ERROR });
      await vi.advanceTimersByTimeAsync(MONITOR_MS * 2);
      expect(lostAlerts()).toHaveLength(0);
    } finally {
      exitSpy.mockRestore();
    }
  });

  it("exits 1 when a shutdown step throws", async () => {
    const exitSpy = vi.spyOn(process, "exit").mockImplementation((() => undefined) as never);
    try {
      await loadIndex();
      h.sendInfoAlert.mockRejectedValueOnce(new Error("FAKE discord"));
      (process.listeners("SIGTERM").at(-1) as () => void)();
      await vi.advanceTimersByTimeAsync(0);
      expect(exitSpy).toHaveBeenCalledWith(1);
      expect(h.captureException).toHaveBeenCalledWith(expect.any(Error), { tags: { context: "indexer-shutdown" } });
    } finally {
      exitSpy.mockRestore();
    }
  });
});
