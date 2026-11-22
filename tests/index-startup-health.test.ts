/**
 * Hung-RPC liveness (J-20261121-IDX-FIX-HUNGRPCLIVENESS), S5 of the report: with the RPC hung at boot, startDiscovery
 * never finished and the HTTP server never listened (connection refused for 8 min). index.ts now listens first:
 * /health answers 503 "starting" and webhook deliveries get 503 + Retry-After until startup completes, then /health
 * reflects loop liveness. Same harness as tests/index-db-monitor.test.ts (real src/index.ts, everything else mocked).
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
    esOpts: null as null | Record<string, any>,
    discoveryDone: Promise.resolve() as Promise<void>,
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
vi.mock("../src/services/discoveryWiring.js", () => ({ startDiscovery: () => h.discoveryDone }));
vi.mock("../src/services/StatsCollector.js", () => ({ StatsCollector: class { start() {} stop() {} } }));
vi.mock("../src/services/TradeIndexer.js", () => ({ TradeIndexerPolling: class { start() {} stop() {} } }));
vi.mock("../src/services/HeliusWebhookManager.js", () => ({
  HeliusWebhookManager: class { async start() {} async stop() {} },
}));
vi.mock("../src/services/EventStreamService.js", () => ({
  EventStreamService: class {
    constructor(opts: Record<string, any>) { h.esOpts = opts; }
    start() { return h.eventStreamStart(); }
    addKnownSlab(s: string) { h.addKnownSlab(s); }
  },
}));
vi.mock("../src/services/LpVaultIndexer.js", () => ({ LpVaultIndexer: class { start() {} stop() {} } }));
vi.mock("../src/lpVault/chain.js", () => ({ RpcLpVaultChain: class {} }));
vi.mock("../src/lpVault/store.js", () => ({ SupabaseLpVaultStore: class {} }));
vi.mock("../src/routes/webhook.js", async () => {
  const { Hono } = await import("hono");
  return { webhookRoutes: () => new Hono() };
});
vi.mock("../src/lib/skippedSignatures.js", () => ({ assertSkippedSignatureSinkReady: () => {} }));


let listenersBefore: Function[];
let added: Function[] = [];

async function loadIndex(): Promise<void> {
  listenersBefore = process.listeners("unhandledRejection");
  await import("../src/index.ts");
  await vi.advanceTimersByTimeAsync(0);
  added = process.listeners("unhandledRejection").filter((l) => !listenersBefore.includes(l));
}

async function get(path: string, init?: RequestInit): Promise<{ code: number; body: any }> {
  expect(h.fetch, "HTTP server not listening").toBeTruthy();
  const res = await h.fetch!(new Request(`http://localhost${path}`, init));
  return { code: res.status, body: await res.json().catch(() => null) };
}

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
  vi.useFakeTimers();
  vi.stubEnv("LP_VAULT_INDEXER_ENABLED", "false");
  vi.stubEnv("HELIUS_ATLAS_WS_URL", "");
  vi.stubEnv("NODE_ENV", "test");
  vi.stubEnv("HEALTH_CACHE_TTL_MS", "0");
  h.probe.mockReturnValue({ count: 3, data: null, error: null });
  h.config = { webhookUrl: "", rpcUrl: "", programId: "FAKE", allProgramIds: [] };
  h.fetch = null;
  h.discoveryDone = Promise.resolve();
  h.esOpts = null;
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

// The first test pays the real index.ts import (~10 s at load 30), so the default 10 s is too tight here.
describe("HUNG-RPC: /health before the first discovery finishes", { timeout: 60_000 }, () => {
  it("listens while discovery is held: /health 503 starting, webhook 503 + Retry-After; then 200 ok once started", async () => {
    let release!: () => void;
    h.discoveryDone = new Promise<void>((r) => { release = r; });
    await loadIndex();
    await vi.advanceTimersByTimeAsync(10 * 60_000); // discovery held for 10 minutes

    // Before the fix: serve() ran only after startDiscovery resolved, so nothing listened here.
    expect(h.serve).toHaveBeenCalledTimes(1);
    const health = await get("/health");
    expect(health.code).toBe(503);
    expect(health.body?.status).toBe("starting");
    const hook = await h.fetch!(new Request("http://localhost/webhook/trades", { method: "POST", body: "[]" }));
    expect(hook.status).toBe(503);
    expect(hook.headers.get("retry-after")).toBe("30");

    release();
    await vi.advanceTimersByTimeAsync(0);
    const after = await get("/health");
    expect(after.code).toBe(200);
    expect(after.body?.status).toBe("ok");
  });

  it("reports degraded (503) when a registered loop makes no progress within its budget", async () => {
    await loadIndex();
    const { loopLiveness } = await import("../src/lib/loopLiveness.ts");
    loopLiveness.register("tradePoll", 60_000);
    expect((await get("/health")).code).toBe(200);
    await vi.advanceTimersByTimeAsync(61_000);
    const r = await get("/health");
    expect(r.code).toBe(503);
    expect(r.body?.status).toBe("degraded");
    expect(h.logger.warn.mock.calls.some((c) => String(c[0]).startsWith("Loop liveness"))).toBe(true);
    loopLiveness.beat("tradePoll");
    expect((await get("/health")).code).toBe(200);
    loopLiveness.unregister("tradePoll");
  });
});

// The Atlas WS wiring in index.ts: ATLAS_WS_SILENCE_MS parsing and the reconnect() closure EventStreamService calls to
// replace a silent or closed socket (src/index.ts, the HELIUS_ATLAS_WS_URL block).
describe("HUNG-RPC: Atlas WS silence/reconnect wiring", { timeout: 60_000 }, () => {
  it("reconnect() builds a fresh socket with createAtlasWs and hands it to the stream", async () => {
    vi.stubEnv("HELIUS_ATLAS_WS_URL", "wss://atlas.invalid");
    await loadIndex();
    expect(h.eventStreamStart).toHaveBeenCalledTimes(1);
    expect(h.esOpts, "EventStreamService not constructed").toBeTruthy();
    expect(h.createAtlasWs).toHaveBeenCalledTimes(1);
    const firstWs = h.esOpts!.ws;
    expect(firstWs).toBe(h.createAtlasWs.mock.results[0].value);

    const next = h.esOpts!.reconnect();
    expect(h.createAtlasWs).toHaveBeenCalledTimes(2);
    expect(next).toBe(h.createAtlasWs.mock.results[1].value);
    expect(next).not.toBe(firstWs);
  });

  it.each([
    ["blank", "", 300_000, false],
    ["0 turns the check off", "0", 0, false],
    ["a valid value", "120000", 120_000, false],
    ["below the 1 s minimum", "500", 300_000, true],
    ["not an integer", "5m", 300_000, true],
  ])("ATLAS_WS_SILENCE_MS %s -> silenceMs", async (_label, raw, expected, warns) => {
    vi.stubEnv("HELIUS_ATLAS_WS_URL", "wss://atlas.invalid");
    vi.stubEnv("ATLAS_WS_SILENCE_MS", raw);
    await loadIndex();
    expect(h.esOpts?.silenceMs).toBe(expected);
    const warned = h.logger.warn.mock.calls.some((c) => String(c[0]) === "Invalid ATLAS_WS_SILENCE_MS, falling back to default");
    expect(warned).toBe(warns);
  });
});
