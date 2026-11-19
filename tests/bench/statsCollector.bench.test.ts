/**
 * StatsCollector cycle bench (test-only). One full cycle for 50 markets with the
 * Supabase client and the RPC connection replaced by counting stubs. Records DB
 * calls, RPC calls and wall ms (median of 10) and asserts the rows written.
 *
 * Run: npx vitest run --maxWorkers=1 tests/bench/statsCollector.bench.test.ts
 *
 * Fixture (50 markets discovered on chain):
 *   - 35 healthy, registered
 *   - 10 indexer_excluded in DB, still dust on chain (stay excluded)
 *   -  3 indexer_excluded in DB, funded again on chain (get re-enabled)
 *   -  2 registered, dust, not yet closed (auto-closed by the sweep)
 * Plus one volume rollup (rpc + upsert) per cycle.
 */
import { describe, it, expect, vi } from 'vitest';
import { Keypair } from '@solana/web3.js';

const h = vi.hoisted(() => ({
  counts: { db: 0, rpc: 0 },
  writes: [] as unknown[],
  slabs: { dust: new Set<string>(), live: new Set<string>() },
  rpcResult: { data: [] as unknown[], error: null },
  dbRows: [] as unknown[],
  logger: { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} },
}));

vi.mock('@percolatorct/shared', () => {
  const chain = (table: string) => {
    h.counts.db++;
    const rec: Record<string, unknown> = { table };
    const c: any = {};
    for (const m of ['select', 'is', 'not', 'limit', 'neq']) c[m] = () => c;
    c.eq = (col: string, v: unknown) => {
      rec[col] = v;
      return c;
    };
    c.in = (col: string, v: unknown) => {
      rec[col] = v;
      return c;
    };
    c.update = (patch: unknown) => {
      rec.op = 'update';
      rec.patch = patch;
      h.writes.push(rec);
      return c;
    };
    c.upsert = (payload: unknown) => {
      h.writes.push({ table, op: 'upsert', payload });
      return Promise.resolve({ error: null });
    };
    c.then = (res: any, rej: any) => Promise.resolve({ data: [], error: null }).then(res, rej);
    return c;
  };
  const key = (k: any) => k.toBase58();
  const acct = (k: string) => ({ data: Buffer.from(h.slabs.dust.has(k) ? 'dust' : 'live') });
  return {
    createLogger: () => h.logger,
    getNetwork: () => 'devnet',
    getConnection: () => ({
      rpcEndpoint: 'https://devnet.example',
      getAccountInfo: async (k: any) => (h.counts.rpc++, acct(key(k))),
      getMultipleAccountsInfo: async (ks: any[]) => (h.counts.rpc++, ks.map((k) => acct(key(k)))),
      getParsedAccountInfo: async () => (h.counts.rpc++, { value: null }),
    }),
    getSupabase: () => ({
      from: (t: string) => chain(t),
      rpc: async () => (h.counts.db++, h.rpcResult),
    }),
    withRetry: async (fn: any) => fn(),
    addBreadcrumb: () => {},
    captureException: () => {},
  };
});
vi.mock('@percolatorct/sdk', () => ({
  parseEngine: (d: Uint8Array) =>
    Buffer.from(d).toString() === 'dust'
      ? { vault: 0n, numUsedAccounts: 0 }
      : { vault: 50_000_000n, numUsedAccounts: 4 },
  detectDexType: () => null,
  parseDexPool: () => null,
  parseWrapperConfigV17: () => null,
  parseAssetOracleProfileV17: () => null,
  V17_HEADER_LEN: 16,
  V17_ASSET_ORACLE_PROFILE_LEN: 400,
}));
vi.mock('../../src/db/marketRows.js', () => ({ getMarketRows: async () => (h.counts.db++, h.dbRows) }));
vi.mock('../../src/db/insertMarketRow.js', () => ({ insertMarketRow: async () => {}, updateAutoMarketMetadata: async () => true }));
vi.mock('../../src/db/autoMarketFields.js', () => ({ findSlabCreation: async () => ({}), resolveAutoMarketFields: () => ({}), v17InitialMarginBps: () => null }));
vi.mock('../../src/services/tokenMetadata.js', () => ({ fetchDasTokenMetadata: async () => null, placeholderIdentity: () => ({}) }));
vi.mock('../../src/services/dexscreener.js', () => ({ resolveIdentitiesByCa: async () => new Map(), chunkForDexScreener: (x: unknown[]) => [x] }));
vi.mock('../../src/layout/resolve.js', () => ({ hasWrapperMagic: () => false, readMarketGroupFields: () => null, reportUnknownLayout: () => {} }));
vi.mock('../../src/layout/marketVersions.js', () => ({ noteMarketLayout: () => {} }));
vi.mock('../../src/blocklist.js', () => ({ isBlockedSlab: () => false, setDbRetiredSlabs: () => {} }));

import { StatsCollector } from '../../src/services/StatsCollector.js';

const slab = () => Keypair.generate().publicKey.toBase58();

function fixture() {
  const names = Array.from({ length: 50 }, slab).sort();
  const healthy = names.slice(0, 35);
  const exclDust = names.slice(35, 45);
  const exclLive = names.slice(45, 48);
  const closable = names.slice(48, 50);
  h.slabs.dust = new Set([...exclDust, ...closable]);
  h.dbRows = names.map((s) => ({
    slab_address: s,
    indexer_excluded: exclDust.includes(s) || exclLive.includes(s),
    keeper_status: null,
  }));
  h.rpcResult = {
    data: healthy.map((s, i) => ({ slab_address: s, volume_24h: String(1000 + i), trade_count_24h: i })),
    error: null,
  };
  const markets = new Map(names.map((s) => [s, { market: {} }]));
  return { markets };
}

/** Normalise rows so the comparison is independent of write ORDER and of updated_at timestamps. */
export function normalisedWrites(): unknown[] {
  const rows: string[] = [];
  for (const w of h.writes as any[]) {
    if (w.op === 'upsert') {
      for (const p of w.payload) rows.push(JSON.stringify({ t: w.table, k: 'upsert', ...p, updated_at: 'X' }));
    } else if (w.op === 'update') {
      const slabs: string[] = Array.isArray(w.slab_address) ? w.slab_address : [w.slab_address];
      for (const s of slabs) rows.push(JSON.stringify({ t: w.table, k: 'update', slab: s, patch: w.patch }));
    }
  }
  return rows.sort();
}

async function cycle() {
  const { markets } = fixture();
  h.counts.db = 0;
  h.counts.rpc = 0;
  h.writes = [];
  const sc: any = new StatsCollector({ getMarkets: () => markets } as any);
  sc._running = true;
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] });
  const t0 = performance.now();
  const p = Promise.all([sc.collect(), sc.syncVolumeForAllDBMarkets()]);
  await vi.advanceTimersByTimeAsync(1000);
  await p;
  const ms = performance.now() - t0;
  vi.useRealTimers();
  return { db: h.counts.db, rpc: h.counts.rpc, ms, rows: normalisedWrites(), markets };
}

describe('StatsCollector cycle bench (50 markets)', () => {
  it('measures DB calls, RPC calls and wall ms per cycle (median of 10)', async () => {
    const runs = [];
    for (let i = 0; i < 10; i++) runs.push(await cycle());
    const med = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
    const out = { db: med(runs.map((r) => r.db)), rpc: med(runs.map((r) => r.rpc)), ms: +med(runs.map((r) => r.ms)).toFixed(2) };
    process.stderr.write(`BENCH statsCollector 50 markets: db=${out.db} rpc=${out.rpc} ms=${out.ms}\n`);
    expect(out.db).toBeGreaterThan(0);
  });

  it('writes exactly the expected rows (equality across refactors)', async () => {
    const r = await cycle();
    const slabs = [...r.markets.keys()];
    const patches = r.rows.map((x) => JSON.parse(x as string));
    const closes = patches.filter((p) => p.k === 'update' && p.patch.status === 'closed').map((p) => p.slab).sort();
    const reenables = patches.filter((p) => p.k === 'update' && p.patch.status === 'active').map((p) => p.slab).sort();
    const upserts = patches.filter((p) => p.k === 'upsert');
    expect(closes).toEqual([...slabs.slice(48, 50)].sort());
    expect(reenables).toEqual([...slabs.slice(45, 48)].sort());
    expect(upserts).toHaveLength(35);
    expect(patches).toHaveLength(35 + 2 + 3);
    // Golden: full multiset of rows, slab names replaced by index so it is stable.
    const idx = new Map(slabs.map((s, i) => [s, i]));
    const golden = patches
      .map((p) => JSON.stringify({ ...p, slab: undefined, slab_address: undefined, i: idx.get(p.slab ?? p.slab_address) }))
      .sort();
    expect(golden).toMatchSnapshot();
  });
});
