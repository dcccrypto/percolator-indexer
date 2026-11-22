/**
 * Auto-close / re-enable on v17 markets: "accounts alone count as live" (tests/reenable-v17.test.ts) must hold for
 * real wrapper accounts, not only for the pure predicate.
 *
 * parseV17AccountStats hard-coded numUsedAccounts = 0, so a v17 market with portfolios but a dust vault was
 * auto-closed by the sweep and never re-enabled. Seen on devnet at slot ~525.13M: 7 markets, e.g. v2.2
 * 3vW8VARn... (vault 340,000, 4 portfolios, materialized_portfolio_count 4) and v2.1 4UwWLQWG... (vault 0, 1 portfolio).
 *
 * Real code path: real StatsCollector.collect(), real layout/resolve + SDK parsers on SDK-table-stamped accounts.
 * Only the DB (recorder) and the RPC connection (returns the stamped accounts) are fakes.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { Keypair } from "@solana/web3.js";
import { LAYOUT_V21, LAYOUT_V22, type LayoutTable } from "@percolatorct/sdk";
import { buildMarket } from "../helpers/v22Fixtures.js";

const h = vi.hoisted(() => ({
  accounts: new Map<string, Uint8Array>(),
  updates: [] as Array<{ patch: Record<string, unknown>; slabs: string[] }>,
}));

vi.mock("@percolatorct/shared", () => {
  const supabase = {
    from: () => {
      let w: { patch: Record<string, unknown>; slabs: string[] } | null = null;
      const c: any = {};
      for (const m of ["select", "eq", "neq", "is", "not", "limit"]) c[m] = () => c;
      c.update = (patch: Record<string, unknown>) => ((w = { patch, slabs: [] }), h.updates.push(w), c);
      c.in = (_k: string, list: string[]) => (w?.slabs.push(...list), c);
      c.then = (res: any, rej: any) => Promise.resolve(w ? { error: null } : { data: [], error: null }).then(res, rej);
      return c;
    },
  };
  const connection = {
    getMultipleAccountsInfo: async (keys: Array<{ toBase58(): string }>) =>
      keys.map((k) => {
        const data = h.accounts.get(k.toBase58());
        return data ? { data: Buffer.from(data) } : null;
      }),
  };
  const logger = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} };
  return {
    config: {},
    getConnection: () => connection,
    getSupabase: () => supabase,
    getNetwork: () => "devnet",
    withRetry: (fn: () => Promise<unknown>) => fn(),
    createLogger: () => logger,
    captureException: () => {},
    addBreadcrumb: () => {},
  };
});

const { StatsCollector } = await import("../../src/services/StatsCollector.js");

function market(layout: LayoutTable, vault: bigint, materialized: bigint): string {
  const slab = Keypair.generate().publicKey.toBase58();
  h.accounts.set(slab, buildMarket(layout, { vault, materialized }));
  return slab;
}

async function collect(slabs: string[], excluded: boolean) {
  const markets = new Map(slabs.map((s) => [s, { market: {} as never }]));
  const sc: any = new StatsCollector({ getMarkets: () => markets });
  sc._running = true;
  sc.syncMarkets = async () => slabs.map((s) => ({ slab_address: s, indexer_excluded: excluded, keeper_status: null }));
  await sc.collect();
  const revived = h.updates.filter((u) => u.patch.indexer_excluded === false).flatMap((u) => u.slabs);
  const closed = h.updates.filter((u) => u.patch.status === "closed").flatMap((u) => u.slabs);
  return { revived, closed };
}

beforeEach(() => {
  h.accounts.clear();
  h.updates.length = 0;
});

describe.each([
  ["v2.2", LAYOUT_V22],
  ["v2.1", LAYOUT_V21],
])("StatsCollector auto-close / re-enable on a %s market", (_name, layout) => {
  it("re-enables an excluded dust-vault market that has portfolios (accounts alone are live)", async () => {
    const withAccounts = market(layout, 340_000n, 4n); // devnet 3vW8VARn...
    const empty = market(layout, 340_000n, 0n);
    const { revived } = await collect([withAccounts, empty], true);
    expect(revived).toEqual([withAccounts]);
  });

  it("does not auto-close a dust-vault market that has portfolios; still closes a genuine dust market", async () => {
    const withAccounts = market(layout, 0n, 1n); // devnet 4UwWLQWG...
    const empty = market(layout, 1_000_000n, 0n); // the creation-seed boundary stays closable
    const { closed } = await collect([withAccounts, empty], false);
    expect(closed).toEqual([empty]);
  });

  it("a funded market with no portfolios is live on both paths (vault source)", async () => {
    const funded = market(layout, 1_200_000_000n, 0n);
    expect((await collect([funded], true)).revived).toEqual([funded]);
    h.updates.length = 0;
    expect((await collect([funded], false)).closed).toEqual([]);
  });
});
