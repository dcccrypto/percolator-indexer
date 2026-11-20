/**
 * v2.2 `markets` row values vs the decoded account (report J-20261120-IDX-REPORT-DIFF-MARKETROWVSCHAIN).
 *
 * Two real devnet v2.2 (VERSION 19) market accounts, recorded 2026-11-20 (tests/fixtures/v22-markets-20261120.fixture.json),
 * go through the REAL discovery parser (discoverV17Markets) and the REAL registration path (StatsCollector.doSyncMarkets);
 * only the DB and RPC are faked.
 *
 * Before the fix (feat/v22-indexer b5d6029) discovery handed registration stub params:
 *   - trading_fee_bps   was 0 (chain trade_fee_base_bps 30 / 5)
 *   - max_leverage      was 20 from the 500 bps stub (chain floor(10000/im) 10 / 5)
 * Guards that must keep passing:
 *   - oracle_authority  = the asset-0 oracle profile authority (a fix that nulls it whenever configV17 is set breaks this)
 *   - decimals / initial_price_e6
 */
import { describe, it, expect, vi, beforeAll } from "vitest";
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
import { StatsCollector } from "../../src/services/StatsCollector.js";

const PROGRAM_ID = new PublicKey("6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM");
const FIXTURE = JSON.parse(
  readFileSync(join(dirname(fileURLToPath(import.meta.url)), "../fixtures/v22-markets-20261120.fixture.json"), "utf8"),
) as {
  markets: Array<{
    slab: string;
    data: string;
    mint: string;
    mintParsed: any;
    expect: {
      decimals: number;
      oracle_authority: string;
      initial_margin_bps: number;
      max_leverage: number;
      trading_fee_bps: number;
      initial_price_e6: number;
    };
  }>;
};

const rowFor = (slab: string) => H.rows.find((r) => r.slab_address === slab);

beforeAll(async () => {
  const accounts = FIXTURE.markets.map((m) => ({
    pubkey: new PublicKey(m.slab),
    account: { data: Buffer.from(m.data, "base64"), owner: PROGRAM_ID, lamports: 1, executable: false },
  }));
  const parsedByMint = new Map(FIXTURE.markets.map((m) => [m.mint, m.mintParsed]));
  H.conn = {
    rpcEndpoint: "http://127.0.0.1:0", // not helius -> DAS lookup returns null without a request
    getProgramAccounts: async () => accounts,
    getSignaturesForAddress: async () => [],
    getTransaction: async () => null,
    getParsedAccountInfo: async (pk: PublicKey) => ({ context: { slot: 1 }, value: parsedByMint.get(pk.toBase58()) ?? null }),
    getAccountInfo: async (pk: PublicKey) => {
      const a = accounts.find((x) => x.pubkey.equals(pk));
      return a ? a.account : null;
    },
  };
  const discovered = await discoverV17Markets(H.conn, PROGRAM_ID);
  expect(discovered).toHaveLength(FIXTURE.markets.length);
  const provider = { getMarkets: () => new Map(discovered.map((m) => [m.slabAddress.toBase58(), { market: m }])) };
  await (new StatsCollector(provider as any) as any).doSyncMarkets();
  expect(H.rows).toHaveLength(FIXTURE.markets.length);
});

describe("v2.2 markets row values match the decoded account (devnet recording 2026-11-20)", () => {
  for (const m of FIXTURE.markets) {
    describe(m.slab.slice(0, 8), () => {
      it(`trading_fee_bps is the wrapper config trade_fee_base_bps (${m.expect.trading_fee_bps}), not the v12 params stub 0`, () => {
        expect(rowFor(m.slab).trading_fee_bps).toBe(m.expect.trading_fee_bps);
      });

      it(`max_leverage is floor(10000 / initial_margin_bps ${m.expect.initial_margin_bps}) = ${m.expect.max_leverage}, not the 500 bps stub (20x)`, () => {
        expect(rowFor(m.slab).max_leverage).toBe(m.expect.max_leverage);
      });

      it("oracle_authority stays the asset-0 oracle profile authority (guard for the fix)", () => {
        expect(rowFor(m.slab).oracle_authority).toBe(m.expect.oracle_authority);
      });

      it("decimals and initial_price_e6 match the mint and the oracle profile (guard)", () => {
        expect(rowFor(m.slab).decimals).toBe(m.expect.decimals);
        expect(rowFor(m.slab).initial_price_e6).toBe(m.expect.initial_price_e6);
      });
    });
  }
});
