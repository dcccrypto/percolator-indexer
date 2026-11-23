/**
 * RS-1: a market that is Resolved on chain must not be registered as `active`.
 *
 * Two real devnet v2.2 (VERSION 19) market accounts, recorded at slot 525,470,260
 * (tests/fixtures/rs1-resolved-market-devnet-20261123.fixture.json): 25xGmJg5 has market-group mode byte 1 (Resolved),
 * 294orabC has mode 0 (Live, the control). They go through the REAL discovery parser (discoverV17Markets, full and light
 * pass) and the REAL registration path (StatsCollector.doSyncMarkets); only the DB and RPC are faked.
 *
 * Before the fix discovery hard-coded `header.resolved: false` and doSyncMarkets hard-coded `status: "active"`, so the
 * Resolved market got an `active` row. markets.status allows active|closed|paused; a Resolved market is `paused`
 * (still listed, so users can close and withdraw; `closed` would hide it).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const H = vi.hoisted(() => ({ conn: null as any, rows: [] as any[] }));

vi.mock("@percolatorct/shared", async (orig) => {
  const real: any = await orig();
  return {
    ...real,
    getConnection: () => H.conn,
    getNetwork: () => "devnet",
    getSupabase: () => {
      throw new Error("no DB in this test");
    },
  };
});
vi.mock("../../src/db/marketRows.js", () => ({ getMarketRows: async () => [] }));
vi.mock("../../src/db/insertMarketRow.js", async (orig) => {
  const real: any = await orig();
  return { ...real, insertMarketRow: vi.fn(async (row: any) => void H.rows.push(row)) };
});

import { PublicKey } from "@solana/web3.js";
import { discoverV17Markets } from "../../src/v17/discovery.js";
import * as layoutResolve from "../../src/layout/resolve.js";
import { StatsCollector } from "../../src/services/StatsCollector.js";

const FIXTURE = JSON.parse(
  readFileSync(join(dirname(fileURLToPath(import.meta.url)), "../fixtures/rs1-resolved-market-devnet-20261123.fixture.json"), "utf8"),
) as {
  program: string;
  markets: Array<{ slab: string; data: string; mint: string; mintParsed: any; mode: number; expectStatus: "active" | "paused" }>;
};
const PROGRAM_ID = new PublicKey(FIXTURE.program);
const RESOLVED = FIXTURE.markets.find((m) => m.mode === 1)!;
const LIVE = FIXTURE.markets.find((m) => m.mode === 0)!;

const accounts = FIXTURE.markets.map((m) => ({ pubkey: new PublicKey(m.slab), data: new Uint8Array(Buffer.from(m.data, "base64")) }));
const parsedByMint = new Map(FIXTURE.markets.map((m) => [m.mint, m.mintParsed]));

beforeEach(() => {
  H.rows.length = 0;
  H.conn = {
    rpcEndpoint: "http://127.0.0.1:0", // not helius -> DAS lookup returns null without a request
    getProgramAccounts: async (_p: unknown, o: any) =>
      accounts.map((a) => ({
        pubkey: a.pubkey,
        account: {
          data: Buffer.from(o?.dataSlice ? a.data.subarray(o.dataSlice.offset, o.dataSlice.offset + o.dataSlice.length) : a.data),
          owner: PROGRAM_ID,
          lamports: 1,
          executable: false,
        },
      })),
    getSignaturesForAddress: async () => [],
    getTransaction: async () => null,
    getParsedAccountInfo: async (pk: PublicKey) => ({ context: { slot: 1 }, value: parsedByMint.get(pk.toBase58()) ?? null }),
    getAccountInfo: async () => null,
  };
});

async function register(light: boolean) {
  const discovered = await discoverV17Markets(H.conn, PROGRAM_ID, undefined, light ? { light: true } : {});
  expect(discovered).toHaveLength(FIXTURE.markets.length);
  const provider = { getMarkets: () => new Map(discovered.map((m) => [m.slabAddress.toBase58(), { market: m }])) };
  await (new StatsCollector(provider as any) as any).doSyncMarkets();
  expect(H.rows).toHaveLength(FIXTURE.markets.length);
  return { discovered, row: (slab: string) => H.rows.find((r) => r.slab_address === slab) };
}

describe("RS-1: Resolved devnet market is not registered as active", () => {
  it("fixture: one Resolved (mode 1) and one Live (mode 0) market", () => {
    expect(RESOLVED?.slab).toBe("25xGmJg5ZaAckSXCnnvhSwsvzJqi82UqpR82UFtsZiNC");
    expect(LIVE?.slab).toBe("294orabCvLGRKSfiiqxQT5F4owssHzHbuK4kqTuobmbb");
  });

  it("isMarketResolved reads the market-group mode byte", () => {
    expect(layoutResolve.isMarketResolved(Buffer.from(RESOLVED.data, "base64"), "test")).toBe(true);
    expect(layoutResolve.isMarketResolved(Buffer.from(LIVE.data, "base64"), "test")).toBe(false);
  });

  for (const light of [false, true]) {
    const pass = light ? "light (dataSlice) pass" : "full pass";

    it(`${pass}: discovery sets header.resolved from the mode byte`, async () => {
      const { discovered } = await register(light);
      const bySlab = new Map(discovered.map((m) => [m.slabAddress.toBase58(), m]));
      expect(bySlab.get(RESOLVED.slab)!.header.resolved).toBe(true);
      expect(bySlab.get(LIVE.slab)!.header.resolved).toBe(false);
    });

    it(`${pass}: the Resolved market row is "paused", the Live one stays "active"`, async () => {
      const { row } = await register(light);
      expect(row(RESOLVED.slab).status).toBe("paused");
      expect(row(LIVE.slab).status).toBe("active");
      for (const m of FIXTURE.markets) expect(row(m.slab).status).toBe(m.expectStatus);
    });
  }
});
