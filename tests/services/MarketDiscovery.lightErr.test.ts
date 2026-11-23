// DISCL-1 (IDX-REPORT-DISCOVERYLIGHTDEVNETREAL): the per-program warn in MarketDiscovery.discoverLight never fired
// for a failed scan, because the real discoverV17Markets turned the error into [] ("0 markets"). Here nothing in
// the discovery path is mocked: real discoverV17Markets, real deadlineConnection, real web3.js Connection against a
// local fault server. Only the shared package (connection + logger) is stubbed.
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { Connection, PublicKey } from "@solana/web3.js";

const PROG_A = "6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM";
const h = vi.hoisted(() => ({
  conn: null as unknown,
  log: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

vi.mock("@percolatorct/shared", () => ({
  config: { allProgramIds: ["6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM"] },
  createLogger: vi.fn(() => h.log),
  captureException: vi.fn(),
  sendCriticalAlert: vi.fn(),
  getConnection: vi.fn(() => h.conn),
  getPrimaryConnection: vi.fn(() => h.conn),
  getFallbackConnection: vi.fn(() => h.conn),
}));

import { MarketDiscovery } from "../../src/services/MarketDiscovery.js";
import { LAYOUT_V22, buildMarket, pk } from "../helpers/v22Fixtures.js";
import { startFaultRpc, type FaultMode, type FaultRpc } from "../helpers/faultRpc.js";

const PROGRAM = new PublicKey(PROG_A);
const market = { key: pk(), data: buildMarket(LAYOUT_V22, { slots: 1 }), owner: PROGRAM };
let rpc: FaultRpc;
let d: MarketDiscovery;
let orig: string | undefined;

const lightWarns = () => h.log.warn.mock.calls.filter((c) => c[0] === "Light discovery failed on program");

beforeAll(async () => {
  rpc = await startFaultRpc();
  h.conn = new Connection(rpc.url, { commitment: "confirmed", disableRetryOnRateLimit: true });
});
afterAll(() => rpc.close());
beforeEach(() => {
  orig = process.env.MARKETS_FILTER;
  delete process.env.MARKETS_FILTER;
  vi.clearAllMocks();
  rpc.mode = "none"; rpc.faultsLeft = Infinity; rpc.accounts = [market]; rpc.gpaCalls = 0;
  d = new MarketDiscovery();
});
afterEach(() => {
  d.stop();
  if (orig === undefined) delete process.env.MARKETS_FILTER; else process.env.MARKETS_FILTER = orig;
});

describe("discoverLight surfaces a failed light scan (DISCL-1)", () => {
  it("clean: finds and notifies the market, no warn", async () => {
    const fn = vi.fn();
    d.onDiscovered(fn);
    const fresh = await d.discoverLight();
    await new Promise((r) => setImmediate(r));
    expect(fresh.map((m) => m.slabAddress.toBase58())).toEqual([market.key.toBase58()]);
    expect(fn).toHaveBeenCalledTimes(1);
    expect(lightWarns()).toHaveLength(0);
    expect(h.log.info).toHaveBeenCalledWith("Light discovery found new markets", { count: 1 });
  });

  for (const mode of ["http429", "rpc429", "truncated"] as FaultMode[]) {
    it(`${mode}: warns with the program id and the error (was: no log line, silent 0 markets), adds nothing`, async () => {
      rpc.mode = mode;
      const fn = vi.fn();
      d.onDiscovered(fn);
      await expect(d.discoverLight()).resolves.toEqual([]);
      await new Promise((r) => setImmediate(r));
      const w = lightWarns();
      expect(w).toHaveLength(1);
      expect(w[0][1]).toMatchObject({ programId: PROG_A, error: expect.any(String) });
      expect((w[0][1] as { error: string }).error.length).toBeGreaterThan(0);
      expect(fn).not.toHaveBeenCalled();
      expect(d.getMarkets().size).toBe(0);
    }, 30_000); // http429: deadlineConnection rebuilds the Connection with web3's default 429 retries (~7.5 s real time)
  }

  it("a faulted pass then a clean pass on the same instance: warn once, then the market is found", async () => {
    rpc.mode = "rpc429"; rpc.faultsLeft = 1;
    await expect(d.discoverLight()).resolves.toEqual([]);
    expect(lightWarns()).toHaveLength(1);
    const fresh = await d.discoverLight();
    expect(fresh.map((m) => m.slabAddress.toBase58())).toEqual([market.key.toBase58()]);
    expect(lightWarns()).toHaveLength(1);
    expect(rpc.gpaCalls).toBe(2);
  });
});
