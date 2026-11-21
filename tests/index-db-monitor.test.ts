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
  };
});

vi.mock("dotenv/config", () => ({}));
vi.mock("@percolatorct/shared", () => ({
  config: { webhookUrl: "", rpcUrl: "", programId: "FAKE", allProgramIds: [] },
  createLogger: () => h.logger,
  initSentry: h.initSentry,
  captureException: h.captureException,
  getSupabase: () => ({
    from: () => ({
      // monitor: select("id", { head: true }) is awaited directly; startup: select(...).limit(1)
      select: () => {
        const p = h.probe();
        return Object.assign(Promise.resolve(p), { limit: () => Promise.resolve({ data: [], error: null }) });
      },
    }),
  }),
  getConnection: () => ({ getSlot: async () => 1 }),
  sendCriticalAlert: h.sendCriticalAlert,
  sendInfoAlert: h.sendInfoAlert,
  createAtlasWs: vi.fn(),
}));
vi.mock("@hono/node-server", () => ({ serve: vi.fn(() => ({ close: vi.fn() })) }));
vi.mock("../src/services/MarketDiscovery.js", () => ({
  MarketDiscovery: class { getMarkets() { return new Map(); } stop() {} },
}));
vi.mock("../src/services/discoveryWiring.js", () => ({ startDiscovery: vi.fn(async () => {}) }));
vi.mock("../src/services/StatsCollector.js", () => ({ StatsCollector: class { start() {} stop() {} } }));
vi.mock("../src/services/TradeIndexer.js", () => ({ TradeIndexerPolling: class { start() {} stop() {} } }));
vi.mock("../src/services/HeliusWebhookManager.js", () => ({
  HeliusWebhookManager: class { async start() {} async stop() {} },
}));
vi.mock("../src/services/EventStreamService.js", () => ({ EventStreamService: class {} }));
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
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.unstubAllEnvs();
  for (const l of added) process.removeListener("unhandledRejection", l as never);
  added = [];
  process.removeAllListeners("SIGTERM");
  process.removeAllListeners("SIGINT");
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
