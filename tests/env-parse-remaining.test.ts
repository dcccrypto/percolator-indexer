import { describe, it, expect, vi, afterEach } from "vitest";

const warn = vi.fn();
const error = vi.fn();
vi.mock("@percolatorct/shared", async (orig) => {
  const real = await orig<Record<string, unknown>>().catch(() => ({}));
  return { ...real, createLogger: () => ({ debug() {}, info() {}, warn, error }) };
});

afterEach(() => { vi.unstubAllEnvs(); vi.resetModules(); warn.mockClear(); error.mockClear(); });

const tradeEnv = async (env: Record<string, string>) => {
  vi.resetModules();
  for (const [k, v] of Object.entries(env)) vi.stubEnv(k, v);
  return import("../src/lib/indexerEnv.js");
};

describe("A: INDEXER_MAX_WRITE_HOLDS", () => {
  it.each(["-3", "2.5", "0", "", "10s", "1e3", "101"])("%j falls back to 5", async (v) => {
    vi.resetModules();
    vi.stubEnv("INDEXER_MAX_WRITE_HOLDS", v);
    const m = await import("../src/services/TradeIndexer.js");
    expect(m.MAX_WRITE_HOLDS).toBe(5);
  });
  it("valid value is kept", async () => {
    vi.resetModules();
    vi.stubEnv("INDEXER_MAX_WRITE_HOLDS", "7");
    expect((await import("../src/services/TradeIndexer.js")).MAX_WRITE_HOLDS).toBe(7);
  });
});

describe("B: TRADE_POLL_INTERVAL_MS", () => {
  it.each(["5", "999", "10000000000", "2147483648", "-1", "10s", "1e3"])("%j -> 300000", async (v) => {
    const m = await tradeEnv({});
    expect(m.parsePollIntervalMs(v)).toBe(300_000);
  });
  it("accepts 1000 and 2^31-1", async () => {
    const m = await tradeEnv({});
    expect(m.parsePollIntervalMs("1000")).toBe(1000);
    expect(m.parsePollIntervalMs("2147483647")).toBe(2147483647);
  });
});

describe("C: write outage alert windows", () => {
  it("a value beyond one day (alert silently disabled) falls back", async () => {
    const m = await tradeEnv({});
    expect(m.parseWriteOutageAlertMs("1000000000000000")).toBe(60_000);
    expect(m.parseWriteOutageRealertMs("1000000000000000")).toBe(600_000);
    expect(m.parseWriteOutageAlertMs("120000")).toBe(120_000);
  });
});

describe("D: TRADE_RPC_MIN_INTERVAL_MS", () => {
  it.each(["99999999", "-1", "10s", "1e3", "2.5"])("%j -> 125", async (v) => {
    expect((await tradeEnv({})).parseRpcMinIntervalMs(v)).toBe(125);
  });
  it("0 (pacing off, documented) and 500 are kept", async () => {
    const m = await tradeEnv({});
    expect(m.parseRpcMinIntervalMs("0")).toBe(0);
    expect(m.parseRpcMinIntervalMs("500")).toBe(500);
  });
});

describe("E: TRADECPI_DELIVERY_BUDGET_MS", () => {
  it.each(["-5", "Infinity", "1", "99", "60001", "10s"])("%j -> 8000", async (v) => {
    expect((await tradeEnv({})).parseDeliveryBudgetMs(v, 8000)).toBe(8000);
  });
  it("valid kept, unset -> default", async () => {
    const m = await tradeEnv({});
    expect(m.parseDeliveryBudgetMs("2000", 8000)).toBe(2000);
    expect(m.parseDeliveryBudgetMs(undefined, 8000)).toBe(8000);
  });
});

describe("G: TRADECPI_UNVERIFIED_SIZE", () => {
  it("unrecognised value logs an error but still resolves to request", async () => {
    vi.resetModules();
    const { unverifiedSizePolicy } = await import("../src/parsers/matcherFill.js");
    expect(unverifiedSizePolicy({ TRADECPI_UNVERIFIED_SIZE: "skp" })).toBe("request");
    expect(error).toHaveBeenCalled();
    expect(unverifiedSizePolicy({ TRADECPI_UNVERIFIED_SIZE: "skip " })).toBe("skip");
    expect(unverifiedSizePolicy({})).toBe("request");
  });
});
