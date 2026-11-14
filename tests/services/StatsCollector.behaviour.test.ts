/**
 * StatsCollector behaviour tests (IDX-COV3).
 *
 * Covers the paths the older StatsCollector.test.ts cannot reach because its
 * @percolatorct/shared mock has no getNetwork: the 24h volume rollup, the DexScreener
 * metadata refresh, the registration edge cases, the excluded-market re-enable pass,
 * the janitorial sweep and the env-driven intervals. DB, RPC and timers are all fakes.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { Keypair, PublicKey } from '@solana/web3.js';

const h = vi.hoisted(() => {
  const state = {
    rpcResult: { data: [] as unknown, error: null as null | { message: string } },
    selectResult: { data: [] as unknown, error: null as null | { message: string } },
    upsertResult: { error: null as null | { message: string } },
    updateResult: { error: null as null | { message: string } },
    updateThrows: false,
    updates: [] as Array<{ table: string; patch: unknown }>,
    upserts: [] as unknown[],
  };
  return {
    state,
    getAccountInfo: vi.fn(),
    getMultipleAccountsInfo: vi.fn(),
    getParsedAccountInfo: vi.fn(),
    getConnection: vi.fn(),
    addBreadcrumb: vi.fn(),
    captureException: vi.fn(),
    getMarketRows: vi.fn(),
    insertMarketRow: vi.fn(),
    updateAutoMarketMetadata: vi.fn(),
    findSlabCreation: vi.fn(),
    resolveAutoMarketFields: vi.fn(),
    v17InitialMarginBps: vi.fn(),
    fetchDasTokenMetadata: vi.fn(),
    resolveIdentitiesByCa: vi.fn(),
    hasWrapperMagic: vi.fn(),
    readMarketGroupFields: vi.fn(),
    reportUnknownLayout: vi.fn(),
    noteMarketLayout: vi.fn(),
    isBlockedSlab: vi.fn(),
    setDbRetiredSlabs: vi.fn(),
    parseEngine: vi.fn(),
    parseWrapperConfigV17: vi.fn(),
    parseAssetOracleProfileV17: vi.fn(),
    detectDexType: vi.fn(),
    parseDexPool: vi.fn(),
    logger: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
  };
});

vi.mock('@percolatorct/shared', () => {
  // Chainable + awaitable stand-in for the PostgREST builder.
  const chain = (table: string, op: { kind: string }) => {
    const c: any = {};
    for (const m of ['select', 'eq', 'is', 'not', 'limit', 'neq']) c[m] = () => c;
    c.update = (patch: unknown) => {
      op.kind = 'update';
      h.state.updates.push({ table, patch });
      return c;
    };
    c.upsert = (payload: unknown) => {
      h.state.upserts.push(payload);
      return Promise.resolve(h.state.upsertResult);
    };
    c.then = (res: any, rej: any) => {
      if (op.kind === 'update') {
        if (h.state.updateThrows) return Promise.reject(new Error('db down')).then(res, rej);
        return Promise.resolve(h.state.updateResult).then(res, rej);
      }
      return Promise.resolve(h.state.selectResult).then(res, rej);
    };
    return c;
  };
  return {
    createLogger: () => h.logger,
    getConnection: h.getConnection,
    getNetwork: () => 'devnet',
    getSupabase: () => ({
      from: (t: string) => chain(t, { kind: 'select' }),
      rpc: vi.fn(async () => h.state.rpcResult),
    }),
    withRetry: async (fn: any) => fn(),
    addBreadcrumb: h.addBreadcrumb,
    captureException: h.captureException,
  };
});

vi.mock('@percolatorct/sdk', () => ({
  parseEngine: h.parseEngine,
  detectDexType: h.detectDexType,
  parseDexPool: h.parseDexPool,
  parseWrapperConfigV17: h.parseWrapperConfigV17,
  parseAssetOracleProfileV17: h.parseAssetOracleProfileV17,
  V17_HEADER_LEN: 16,
  V17_ASSET_ORACLE_PROFILE_LEN: 400,
}));
vi.mock('../../src/db/marketRows.js', () => ({ getMarketRows: h.getMarketRows }));
vi.mock('../../src/db/insertMarketRow.js', () => ({
  insertMarketRow: h.insertMarketRow,
  updateAutoMarketMetadata: h.updateAutoMarketMetadata,
}));
vi.mock('../../src/db/autoMarketFields.js', () => ({
  findSlabCreation: h.findSlabCreation,
  resolveAutoMarketFields: h.resolveAutoMarketFields,
  v17InitialMarginBps: h.v17InitialMarginBps,
}));
vi.mock('../../src/services/tokenMetadata.js', () => ({
  fetchDasTokenMetadata: h.fetchDasTokenMetadata,
  placeholderIdentity: (slab: string) => ({ symbol: 'UNKNOWN', name: `Unnamed ${slab.slice(0, 4)}` }),
}));
vi.mock('../../src/services/dexscreener.js', () => ({
  resolveIdentitiesByCa: h.resolveIdentitiesByCa,
  chunkForDexScreener: <T>(items: T[], size = 30) => {
    const out: T[][] = [];
    for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
    return out;
  },
}));
vi.mock('../../src/layout/resolve.js', () => ({
  hasWrapperMagic: h.hasWrapperMagic,
  readMarketGroupFields: h.readMarketGroupFields,
  reportUnknownLayout: h.reportUnknownLayout,
}));
vi.mock('../../src/layout/marketVersions.js', () => ({ noteMarketLayout: h.noteMarketLayout }));
vi.mock('../../src/blocklist.js', () => ({
  isBlockedSlab: h.isBlockedSlab,
  setDbRetiredSlabs: h.setDbRetiredSlabs,
}));

import {
  StatsCollector,
  AUTOCLOSE_INTERVAL_MS,
  PLACEHOLDER_REGISTRATION_GRACE_MS,
} from '../../src/services/StatsCollector.js';

const newKey = () => Keypair.generate().publicKey;
const slab = () => newKey().toBase58();
const MINT = newKey();

function market(over: Record<string, any> = {}) {
  return {
    market: {
      programId: newKey(),
      config: { collateralMint: MINT, indexFeedId: newKey() },
      params: { initialMarginBps: 1000n },
      ...over,
    },
  };
}
/** A hyperp market: all-zero indexFeedId. */
function hyperp(dexPool: PublicKey | null) {
  return market({ config: { collateralMint: MINT, indexFeedId: new PublicKey(new Uint8Array(32)), dexPool } });
}

function make(markets: Map<string, any> = new Map()) {
  const provider = { getMarkets: vi.fn(() => markets) };
  const sc = new StatsCollector(provider as any);
  (sc as any)._running = true;
  return { sc: sc as any, provider, markets };
}

const lastInsert = () => h.insertMarketRow.mock.calls.at(-1)![0];

beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
  // clearAllMocks keeps implementations; reset the ones tests override so they cannot leak.
  for (const f of [h.noteMarketLayout, h.readMarketGroupFields, h.parseWrapperConfigV17, h.parseAssetOracleProfileV17,
    h.getAccountInfo, h.getMultipleAccountsInfo, h.insertMarketRow, h.getMarketRows, h.detectDexType, h.parseDexPool, h.v17InitialMarginBps]) f.mockReset();
  Object.assign(h.state, {
    rpcResult: { data: [], error: null },
    selectResult: { data: [], error: null },
    upsertResult: { error: null },
    updateResult: { error: null },
    updateThrows: false,
    updates: [],
    upserts: [],
  });
  h.getConnection.mockReturnValue({
    getAccountInfo: h.getAccountInfo,
    getMultipleAccountsInfo: h.getMultipleAccountsInfo,
    getParsedAccountInfo: h.getParsedAccountInfo,
    getSignaturesForAddress: vi.fn(),
    getTransaction: vi.fn(),
    rpcEndpoint: 'https://devnet.example',
  });
  h.getMarketRows.mockResolvedValue([]);
  h.getParsedAccountInfo.mockResolvedValue({ value: null });
  h.findSlabCreation.mockResolvedValue({ creator: 'Creator', blockTime: null });
  h.resolveAutoMarketFields.mockReturnValue({
    deployer: 'Deployer',
    oracle_authority: 'Oracle',
    initial_price_e6: 1,
    trading_fee_bps: 10,
  });
  h.fetchDasTokenMetadata.mockResolvedValue(null);
  h.isBlockedSlab.mockReturnValue(false);
  h.hasWrapperMagic.mockReturnValue(false);
  h.updateAutoMarketMetadata.mockResolvedValue(true);
  h.resolveIdentitiesByCa.mockResolvedValue(new Map());
  h.parseEngine.mockReturnValue({ vault: 5_000_000n, numUsedAccounts: 3 });
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllEnvs();
});

describe('syncVolumeForAllDBMarkets (volume traders see)', () => {
  it('does nothing when stopped', async () => {
    const { sc } = make();
    sc._running = false;
    await sc.syncVolumeForAllDBMarkets();
    expect(h.state.upserts).toHaveLength(0);
  });

  it('writes volume and trade count in ONE bulk upsert', async () => {
    h.state.rpcResult = {
      data: [
        { slab_address: 'AAAAAAAAAAAA', volume_24h: '1500', trade_count_24h: '7' },
        { slab_address: 'BBBBBBBBBBBB', volume_24h: 20, trade_count_24h: 1 },
        { slab_address: 'CCCCCCCCCCCC', volume_24h: null, trade_count_24h: null },
      ],
      error: null,
    };
    const { sc } = make();
    await sc.syncVolumeForAllDBMarkets();
    expect(h.state.upserts).toHaveLength(1);
    const rows = h.state.upserts[0] as any[];
    expect(rows.map((r) => [r.slab_address, r.volume_24h, r.trade_count_24h])).toEqual([
      ['AAAAAAAAAAAA', 1500, 7],
      ['BBBBBBBBBBBB', 20, 1],
      ['CCCCCCCCCCCC', 0, 0],
    ]);
    expect(rows[0].network).toBe('devnet');
    expect(Number.isNaN(Date.parse(rows[0].updated_at))).toBe(false);
  });

  it('keeps a volume above MAX_SAFE_INTEGER as an exact string instead of rounding it', async () => {
    const big = '123456789012345678901234';
    h.state.rpcResult = {
      data: [{ slab_address: 'AAAAAAAAAAAA', volume_24h: big, trade_count_24h: 1 }],
      error: null,
    };
    const { sc } = make();
    await sc.syncVolumeForAllDBMarkets();
    expect((h.state.upserts[0] as any[])[0].volume_24h).toBe(big);
    expect(h.logger.warn).toHaveBeenCalledWith(expect.stringContaining('MAX_SAFE_INTEGER'), expect.anything());
  });

  it('skips the upsert when the rollup RPC errors', async () => {
    h.state.rpcResult = { data: null, error: { message: 'boom' } };
    const { sc } = make();
    await sc.syncVolumeForAllDBMarkets();
    expect(h.state.upserts).toHaveLength(0);
    expect(h.logger.warn).toHaveBeenCalledWith(expect.stringContaining('rollup RPC failed'), { error: 'boom' });
  });

  it('skips the upsert when no market traded', async () => {
    h.state.rpcResult = { data: null, error: null };
    const { sc } = make();
    await sc.syncVolumeForAllDBMarkets();
    expect(h.state.upserts).toHaveLength(0);
  });

  it('logs and returns when the upsert fails (FK miss), and can run again afterwards', async () => {
    h.state.rpcResult = { data: [{ slab_address: 'AAAAAAAAAAAA', volume_24h: 1, trade_count_24h: 1 }], error: null };
    h.state.upsertResult = { error: { message: 'fk violation' } };
    const { sc } = make();
    await sc.syncVolumeForAllDBMarkets();
    expect(h.logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('bulk upsert failed'),
      { error: 'fk violation', markets: 1 },
    );
    h.state.upsertResult = { error: null };
    await sc.syncVolumeForAllDBMarkets();
    expect(h.logger.info).toHaveBeenCalledWith('Volume sync complete', { marketsUpdated: 1 });
  });

  it('swallows a thrown error and releases the in-flight flag', async () => {
    h.state.rpcResult = { data: [{ slab_address: 'AAAAAAAAAAAA', volume_24h: 1, trade_count_24h: 1 }], error: null };
    h.state.upsertResult = null as any; // destructuring null throws
    const { sc } = make();
    await sc.syncVolumeForAllDBMarkets();
    expect(h.logger.warn).toHaveBeenCalledWith(expect.stringContaining('syncVolumeForAllDBMarkets failed'), expect.anything());
    expect(sc._syncingVolume).toBe(false);
  });

  it('ignores an overlapping call while one is in flight', async () => {
    const { sc } = make();
    sc._syncingVolume = true;
    await sc.syncVolumeForAllDBMarkets();
    expect(h.state.upserts).toHaveLength(0);
  });
});

describe('refreshMarketMetadata (DexScreener identity)', () => {
  it('does nothing when stopped or already running', async () => {
    const a = make();
    a.sc._running = false;
    await a.sc.refreshMarketMetadata();
    const b = make();
    b.sc._refreshingMetadata = true;
    await b.sc.refreshMarketMetadata();
    expect(h.resolveIdentitiesByCa).not.toHaveBeenCalled();
  });

  it('logs and stops when the candidate query fails', async () => {
    h.state.selectResult = { data: null, error: { message: 'nope' } };
    const { sc } = make();
    await sc.refreshMarketMetadata();
    expect(h.logger.warn).toHaveBeenCalledWith(expect.stringContaining('candidate query failed'), { error: 'nope' });
    expect(h.resolveIdentitiesByCa).not.toHaveBeenCalled();
  });

  it('returns quietly with no candidates (null data too)', async () => {
    h.state.selectResult = { data: null, error: null };
    const { sc } = make();
    await sc.refreshMarketMetadata();
    expect(h.resolveIdentitiesByCa).not.toHaveBeenCalled();
  });

  it('resolves each distinct CA once and fans the identity out to every slab using it', async () => {
    h.state.selectResult = {
      data: [
        { slab_address: 'SLAB1111xxxx', mainnet_ca: 'CA111111xxxx' },
        { slab_address: 'SLAB2222xxxx', mainnet_ca: 'CA111111xxxx' },
        { slab_address: 'SLAB3333xxxx', mainnet_ca: 'CA222222xxxx' },
        { slab_address: 'SLAB4444xxxx', mainnet_ca: 'CA333333xxxx' },
      ],
      error: null,
    };
    h.resolveIdentitiesByCa.mockResolvedValue(
      new Map([
        ['CA111111xxxx', { symbol: 'AAA', name: 'Aaa', logoUrl: 'https://l/a.png' }],
        // symbol only: must not blank name or write a null logo
        ['CA222222xxxx', { symbol: 'BBB', name: null, logoUrl: null }],
        // nothing usable: skipped entirely
        ['CA333333xxxx', { symbol: null, name: null, logoUrl: null }],
      ]),
    );
    const { sc } = make();
    await sc.refreshMarketMetadata();

    expect(h.resolveIdentitiesByCa).toHaveBeenCalledTimes(1);
    expect(h.resolveIdentitiesByCa).toHaveBeenCalledWith(['CA111111xxxx', 'CA222222xxxx', 'CA333333xxxx']);
    expect(h.updateAutoMarketMetadata.mock.calls).toEqual([
      ['SLAB1111xxxx', { symbol: 'AAA', name: 'Aaa', logo_url: 'https://l/a.png' }],
      ['SLAB2222xxxx', { symbol: 'AAA', name: 'Aaa', logo_url: 'https://l/a.png' }],
      ['SLAB3333xxxx', { symbol: 'BBB' }],
    ]);
    expect(h.logger.info).toHaveBeenCalledWith('refreshMarketMetadata complete', {
      candidates: 4,
      resolved: 3,
      updated: 3,
    });
  });

  it('does not count a row a human claimed mid-pass (update matched nothing)', async () => {
    h.state.selectResult = { data: [{ slab_address: 'SLAB1111xxxx', mainnet_ca: 'CA111111xxxx' }], error: null };
    h.resolveIdentitiesByCa.mockResolvedValue(new Map([['CA111111xxxx', { symbol: 'AAA', name: 'A', logoUrl: null }]]));
    h.updateAutoMarketMetadata.mockResolvedValue(false);
    const { sc } = make();
    await sc.refreshMarketMetadata();
    expect(h.logger.info).toHaveBeenCalledWith('refreshMarketMetadata complete', expect.objectContaining({ updated: 0 }));
  });

  it('reports nothing resolved without writing', async () => {
    h.state.selectResult = { data: [{ slab_address: 'SLAB1111xxxx', mainnet_ca: 'CA111111xxxx' }], error: null };
    const { sc } = make();
    await sc.refreshMarketMetadata();
    expect(h.updateAutoMarketMetadata).not.toHaveBeenCalled();
    expect(h.logger.info).toHaveBeenCalledWith('refreshMarketMetadata: nothing resolved', { candidates: 1 });
  });

  it('a DexScreener failure is swallowed and the next pass can run', async () => {
    h.state.selectResult = { data: [{ slab_address: 'SLAB1111xxxx', mainnet_ca: 'CA111111xxxx' }], error: null };
    h.resolveIdentitiesByCa.mockRejectedValueOnce(new Error('429'));
    const { sc } = make();
    await sc.refreshMarketMetadata();
    expect(h.logger.warn).toHaveBeenCalledWith('refreshMarketMetadata failed', { error: '429' });
    expect(sc._refreshingMetadata).toBe(false);
    await sc.refreshMarketMetadata();
    expect(h.resolveIdentitiesByCa).toHaveBeenCalledTimes(2);
  });
});

describe('start / stop timers', () => {
  it('runs the volume sync + metadata refresh every 10 minutes and stops cleanly', async () => {
    const provider = { getMarkets: vi.fn(() => new Map()) };
    const sc: any = new StatsCollector(provider as any);
    const volume = vi.spyOn(sc, 'syncVolumeForAllDBMarkets').mockResolvedValue(undefined);
    const refresh = vi.spyOn(sc, 'refreshMarketMetadata').mockResolvedValue(undefined);
    sc.start();
    await vi.advanceTimersByTimeAsync(30_001);
    expect(volume).toHaveBeenCalledTimes(1); // the 30s warm-up pass
    expect(refresh).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(volume).toHaveBeenCalledTimes(2);
    expect(refresh).toHaveBeenCalledTimes(1);
    sc.stop();
    await vi.advanceTimersByTimeAsync(30 * 60_000);
    expect(volume).toHaveBeenCalledTimes(2);
  });

  it('stop() before the warm-up fires cancels the first volume sync', async () => {
    const sc: any = new StatsCollector({ getMarkets: () => new Map() } as any);
    const volume = vi.spyOn(sc, 'syncVolumeForAllDBMarkets').mockResolvedValue(undefined);
    sc.start();
    sc.stop();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(volume).not.toHaveBeenCalled();
  });
});

describe('registration (doSyncMarkets)', () => {
  it('reads nothing when discovery is empty', async () => {
    const { sc } = make();
    expect(await sc.registerNewMarkets()).toBeUndefined();
    expect(h.getMarketRows).not.toHaveBeenCalled();
  });

  it('feeds keeper_status=retired rows to the blocklist and never registers a blocked slab', async () => {
    const blocked = slab();
    const kept = slab();
    h.isBlockedSlab.mockImplementation((s: string) => s === blocked);
    h.getMarketRows.mockResolvedValue([
      { slab_address: 'RETIRED', keeper_status: 'retired' },
      { slab_address: 'LIVE', keeper_status: 'active' },
    ]);
    const { sc } = make(new Map([[blocked, market()], [kept, market()]]));
    await sc.registerNewMarkets();
    expect(h.setDbRetiredSlabs).toHaveBeenCalledWith(['RETIRED']);
    expect(h.insertMarketRow).toHaveBeenCalledTimes(1);
    expect(lastInsert().slab_address).toBe(kept);
    expect(h.logger.debug).toHaveBeenCalledWith('Skipped blocked slabs during registration', { count: 1 });
  });

  it('registers nothing and drops cached creation lookups once the row exists', async () => {
    const s = slab();
    const { sc } = make(new Map([[s, market()]]));
    sc.creationCache.set(s, { creator: 'x', blockTime: 1 });
    sc.creationCache.set('gone-from-chain', { creator: 'y', blockTime: 1 });
    h.getMarketRows.mockResolvedValue([{ slab_address: s }]);
    await sc.registerNewMarkets();
    expect(h.insertMarketRow).not.toHaveBeenCalled();
    expect(sc.creationCache.size).toBe(0);
  });

  it('a failing markets read is logged and does not throw', async () => {
    h.getMarketRows.mockRejectedValue(new Error('db down'));
    const { sc } = make(new Map([[slab(), market()]]));
    await expect(sc.registerNewMarkets()).resolves.toBeUndefined();
    expect(h.logger.error).toHaveBeenCalledWith('Market sync failed', expect.objectContaining({ error: 'db down' }));
  });

  it('skips a market with no collateral mint, and one with no deployer', async () => {
    const noMint = slab();
    const noDeployer = slab();
    const ok = slab();
    h.resolveAutoMarketFields.mockImplementation((m: any) =>
      m === markets.get(noDeployer)!.market
        ? { deployer: null }
        : { deployer: 'D', oracle_authority: 'O', initial_price_e6: 1, trading_fee_bps: 10 },
    );
    const markets = new Map([
      [noMint, market({ config: { indexFeedId: newKey() } })],
      [noDeployer, market()],
      [ok, market()],
    ]);
    const { sc } = make(markets);
    await sc.registerNewMarkets();
    expect(h.insertMarketRow).toHaveBeenCalledTimes(1);
    expect(lastInsert().slab_address).toBe(ok);
  });

  it('prefers configV17 and a read slab initial margin over the discovery params', async () => {
    const s = slab();
    h.getAccountInfo.mockResolvedValue({ data: new Uint8Array(8) });
    h.v17InitialMarginBps.mockReturnValue(2500n);
    const { sc } = make(new Map([[s, market({ configV17: { collateralMint: MINT, indexFeedId: newKey() } })]]));
    await sc.registerNewMarkets();
    expect(lastInsert().max_leverage).toBe(4);
  });

  it('a missing slab account, or a failing read, falls back to the default margin', async () => {
    const a = slab();
    const b = slab();
    h.getAccountInfo.mockResolvedValueOnce(null).mockRejectedValueOnce(new Error('rpc'));
    const v17 = () => market({ configV17: { collateralMint: MINT, indexFeedId: newKey() }, params: undefined });
    const { sc } = make(new Map([[a, v17()], [b, v17()]]));
    await sc.registerNewMarkets();
    // initialMarginBps 0 -> GH#1748 default leverage
    expect(h.insertMarketRow.mock.calls.map((c) => c[0].max_leverage)).toEqual([10, 10]);
    expect(h.logger.debug).toHaveBeenCalledWith(
      expect.stringContaining('Slab read for initial margin failed'),
      expect.objectContaining({ error: 'rpc' }),
    );
  });

  it('computes leverage from initial margin (rounding down)', async () => {
    const { sc } = make(new Map([[slab(), market({ params: { initialMarginBps: 3000n } })]]));
    await sc.registerNewMarkets();
    expect(lastInsert().max_leverage).toBe(3);
  });

  it('a margin above 100% (leverage 0) falls back to 10x instead of writing 0', async () => {
    const { sc } = make(new Map([[slab(), market({ params: { initialMarginBps: 20000n } })]]));
    await sc.registerNewMarkets();
    expect(lastInsert().max_leverage).toBe(10);
  });

  it('reads decimals from the mint and lets DAS override symbol, name, decimals and logo', async () => {
    h.getParsedAccountInfo.mockResolvedValue({ value: { data: { parsed: { info: { decimals: 6 } } } } });
    h.fetchDasTokenMetadata.mockResolvedValue({ symbol: 'USDC', name: 'USD Coin', decimals: 8, logoUrl: 'https://l/u.png' });
    const { sc } = make(new Map([[slab(), market()]]));
    await sc.registerNewMarkets();
    expect(lastInsert()).toMatchObject({ symbol: 'USDC', name: 'USD Coin', decimals: 8, logo_url: 'https://l/u.png' });
  });

  it('keeps the mint decimals when DAS has no decimals, and 9 when the mint is unparsed', async () => {
    h.getParsedAccountInfo.mockResolvedValueOnce({ value: { data: { parsed: { info: { decimals: 6 } } } } });
    h.fetchDasTokenMetadata.mockResolvedValue({ symbol: 'X', name: null, decimals: null, logoUrl: null });
    const { sc } = make(new Map([[slab(), market()]]));
    await sc.registerNewMarkets();
    expect(lastInsert()).toMatchObject({ symbol: 'X', decimals: 6, logo_url: null });
    expect(lastInsert().name).toMatch(/^Market /);

    h.getParsedAccountInfo.mockResolvedValueOnce({ value: { data: Buffer.alloc(1) } });
    await make(new Map([[slab(), market()]])).sc.registerNewMarkets();
    expect(lastInsert().decimals).toBe(9);
  });

  it('a metadata lookup failure still registers the market with fallbacks', async () => {
    h.getParsedAccountInfo.mockRejectedValue(new Error('rpc'));
    const { sc } = make(new Map([[slab(), market()]]));
    await sc.registerNewMarkets();
    expect(lastInsert().symbol).toBe(MINT.toBase58().slice(0, 8));
    expect(h.logger.debug).toHaveBeenCalledWith(
      expect.stringContaining('Token metadata resolution failed'),
      expect.objectContaining({ error: 'rpc' }),
    );
  });

  it('clamps garbage decimals to 6', async () => {
    h.getParsedAccountInfo.mockResolvedValue({ value: { data: { parsed: { info: { decimals: 77 } } } } });
    const { sc } = make(new Map([[slab(), market()]]));
    await sc.registerNewMarkets();
    expect(lastInsert().decimals).toBe(6);
  });

  it('names a hyperp market after its base asset and uses the base logo', async () => {
    const pool = newKey();
    const baseMint = newKey();
    h.getAccountInfo.mockResolvedValue({ owner: newKey(), data: Buffer.alloc(4) });
    h.detectDexType.mockReturnValue('pumpswap');
    h.parseDexPool.mockReturnValue({ baseMint });
    h.fetchDasTokenMetadata.mockImplementation(async (_rpc: string, mint: string) =>
      mint === baseMint.toBase58()
        ? { symbol: 'SOL', name: 'Solana', decimals: 9, logoUrl: 'https://l/sol.png' }
        : { symbol: 'USDC', name: 'USD Coin', decimals: 6, logoUrl: 'https://l/usdc.png' },
    );
    const { sc } = make(new Map([[slab(), hyperp(pool)]]));
    await sc.registerNewMarkets();
    expect(lastInsert()).toMatchObject({ symbol: 'SOL', name: 'SOL/USDC Perpetual', logo_url: 'https://l/sol.png' });
  });

  it('labels the collateral USDC when the hyperp base resolves but the collateral does not', async () => {
    const pool = newKey();
    const baseMint = newKey();
    h.getAccountInfo.mockResolvedValue({ owner: newKey(), data: Buffer.alloc(4) });
    h.detectDexType.mockReturnValue('pumpswap');
    h.parseDexPool.mockReturnValue({ baseMint });
    h.fetchDasTokenMetadata.mockImplementation(async (_r: string, mint: string) =>
      mint === baseMint.toBase58() ? { symbol: 'BONK', name: 'Bonk', decimals: 5, logoUrl: null } : null,
    );
    const { sc } = make(new Map([[slab(), hyperp(pool)]]));
    await sc.registerNewMarkets();
    expect(lastInsert()).toMatchObject({ symbol: 'BONK', name: 'BONK/USDC Perpetual', logo_url: null });
  });

  it('uses the real collateral symbol (not a fixed USDC) in the hyperp market name', async () => {
    const baseMint = newKey();
    h.getAccountInfo.mockResolvedValue({ owner: newKey(), data: Buffer.alloc(4) });
    h.detectDexType.mockReturnValue('pumpswap');
    h.parseDexPool.mockReturnValue({ baseMint });
    h.fetchDasTokenMetadata.mockImplementation(async (_r: string, mint: string) =>
      mint === baseMint.toBase58()
        ? { symbol: 'SOL', name: 'Solana', decimals: 9, logoUrl: null }
        : { symbol: 'USDT', name: 'Tether', decimals: 6, logoUrl: null },
    );
    const { sc } = make(new Map([[slab(), hyperp(newKey())]]));
    await sc.registerNewMarkets();
    expect(lastInsert().name).toBe('SOL/USDT Perpetual');
  });

  it('an old hyperp market whose pool does not resolve gets the neutral placeholder and no logo', async () => {
    const pool = newKey();
    h.getAccountInfo.mockResolvedValue({ owner: newKey(), data: Buffer.alloc(4) });
    h.detectDexType.mockReturnValue(null); // pool owned by an unknown program
    h.fetchDasTokenMetadata.mockResolvedValue({ symbol: 'USDC', name: 'USD Coin', decimals: 6, logoUrl: 'https://l/usdc.png' });
    h.findSlabCreation.mockResolvedValue({ creator: 'C', blockTime: Math.floor(Date.now() / 1000) - 3600 });
    const { sc } = make(new Map([[slab(), hyperp(pool)]]));
    await sc.registerNewMarkets();
    expect(lastInsert()).toMatchObject({ symbol: 'UNKNOWN', logo_url: null });
  });

  it('a hyperp market with a pool that does not resolve is deferred while it is brand new', async () => {
    const pool = newKey();
    h.getAccountInfo.mockResolvedValue(null);
    h.findSlabCreation.mockResolvedValue({
      creator: 'C',
      blockTime: Math.floor(Date.now() / 1000) - PLACEHOLDER_REGISTRATION_GRACE_MS / 2000,
    });
    const { sc } = make(new Map([[slab(), hyperp(pool)]]));
    await sc.registerNewMarkets();
    expect(h.insertMarketRow).not.toHaveBeenCalled();
    expect(h.logger.info).toHaveBeenCalledWith(
      'Deferring market registration: identity not resolvable yet',
      expect.anything(),
    );
  });

  it('a throwing pool read is logged and the market still falls to the placeholder', async () => {
    h.getAccountInfo.mockRejectedValue(new Error('pool rpc'));
    h.findSlabCreation.mockResolvedValue({ creator: 'C', blockTime: 1 });
    const { sc } = make(new Map([[slab(), hyperp(newKey())]]));
    await sc.registerNewMarkets();
    expect(h.logger.debug).toHaveBeenCalledWith(
      'Hyperp base asset resolution failed',
      expect.objectContaining({ error: 'pool rpc' }),
    );
    expect(lastInsert().symbol).toBe('UNKNOWN');
  });

  it('one failed insert does not stop the next market registering', async () => {
    const a = slab();
    const b = slab();
    h.insertMarketRow.mockRejectedValueOnce(new Error('unique violation'));
    const { sc } = make(new Map([[a, market()], [b, market()]]));
    await sc.registerNewMarkets();
    expect(h.insertMarketRow).toHaveBeenCalledTimes(2);
    expect(h.logger.warn).toHaveBeenCalledWith(
      'Failed to register market',
      { slabAddress: a, error: 'unique violation' },
    );
  });

  it('a non-Error throw during registration is still reported', async () => {
    h.insertMarketRow.mockRejectedValueOnce('string failure');
    const { sc } = make(new Map([[slab(), market()]]));
    await sc.registerNewMarkets();
    expect(h.logger.warn).toHaveBeenCalledWith('Failed to register market', expect.objectContaining({ error: 'string failure' }));
  });
});

describe('collect(): sweep', () => {
  const engineLive = { vault: 50_000_000n, numUsedAccounts: 2 };
  const engineDust = { vault: 1_000_000n, numUsedAccounts: 0 };

  /** Markets already registered, so only the sweep runs. */
  function registered(n = 1) {
    const map = new Map<string, any>();
    for (let i = 0; i < n; i++) map.set(slab(), market());
    h.getMarketRows.mockResolvedValue([...map.keys()].map((s) => ({ slab_address: s })));
    return map;
  }

  it('does nothing when stopped or already collecting', async () => {
    const a = make(registered());
    a.sc._running = false;
    await a.sc.collect();
    const b = make(registered());
    b.sc._collecting = true;
    await b.sc.collect();
    expect(h.getMarketRows).not.toHaveBeenCalled();
  });

  it('warns and skips when discovery has no markets', async () => {
    const { sc } = make();
    await sc.collect();
    expect(h.logger.warn).toHaveBeenCalledWith(expect.stringContaining('0 markets'));
    expect(h.getMultipleAccountsInfo).not.toHaveBeenCalled();
  });

  it('sweeps at most once per AUTOCLOSE interval, then again after it', async () => {
    const markets = registered();
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }]);
    const { sc } = make(markets);
    await sc.collect();
    await sc.collect();
    expect(h.getMultipleAccountsInfo).toHaveBeenCalledTimes(1);
    vi.setSystemTime(Date.now() + AUTOCLOSE_INTERVAL_MS + 1);
    await sc.collect();
    expect(h.getMultipleAccountsInfo).toHaveBeenCalledTimes(2);
  });

  it('marks a slab that no longer exists on chain closed + excluded', async () => {
    const markets = registered();
    h.getMultipleAccountsInfo.mockResolvedValue([null]);
    const { sc } = make(markets);
    await sc.collect();
    expect(h.state.updates).toEqual([{ table: 'markets', patch: { status: 'closed', indexer_excluded: true } }]);
  });

  it('auto-closes an abandoned slab (dust vault, no accounts) once, remembers it, and keeps live ones open', async () => {
    const markets = registered(2);
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }, { data: new Uint8Array(8) }]);
    h.parseEngine.mockReturnValueOnce(engineDust).mockReturnValueOnce(engineLive);
    const { sc } = make(markets);
    await sc.collect();
    expect(h.state.updates).toHaveLength(1);
    const [dustSlab] = [...markets.keys()];
    expect(sc.closedSlabs.has(dustSlab)).toBe(true);
    expect(h.noteMarketLayout).toHaveBeenCalledTimes(2);

    // Next sweep: already remembered, no second write.
    vi.setSystemTime(Date.now() + AUTOCLOSE_INTERVAL_MS + 1);
    h.parseEngine.mockReturnValueOnce(engineDust).mockReturnValueOnce(engineLive);
    await sc.collect();
    expect(h.state.updates).toHaveLength(1);
  });

  it('does not remember a slab when the close write errors, so it retries next sweep', async () => {
    const markets = registered();
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }]);
    h.parseEngine.mockReturnValue(engineDust);
    h.state.updateResult = { error: { message: 'rls' } };
    const { sc } = make(markets);
    await sc.collect();
    expect(sc.closedSlabs.size).toBe(0);
    expect(h.logger.warn).toHaveBeenCalledWith('Auto-close market failed', expect.objectContaining({ error: 'rls' }));
  });

  it('survives a throwing close write', async () => {
    const markets = registered();
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }]);
    h.parseEngine.mockReturnValue(engineDust);
    h.state.updateThrows = true;
    const { sc } = make(markets);
    await sc.collect();
    expect(h.logger.warn).toHaveBeenCalledWith('Auto-close market error', expect.objectContaining({ error: 'db down' }));
    expect(h.logger.info).toHaveBeenCalledWith('StatsCollector sweep complete', expect.objectContaining({ updated: 1, errors: 0 }));
  });

  it('reports an unparseable slab loudly and keeps sweeping the rest', async () => {
    const markets = registered(2);
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }, { data: new Uint8Array(8) }]);
    h.parseEngine.mockImplementationOnce(() => {
      throw new Error('bad length');
    }).mockReturnValueOnce(engineLive);
    const { sc } = make(markets);
    await sc.collect();
    expect(h.reportUnknownLayout).toHaveBeenCalledWith([...markets.keys()][0], expect.any(Error), 'StatsCollector.sweep');
    expect(h.logger.info).toHaveBeenCalledWith('StatsCollector sweep complete', expect.objectContaining({ updated: 1 }));
  });

  it('counts a per-market failure as an error and leaves a breadcrumb', async () => {
    const markets = registered();
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }]);
    h.noteMarketLayout.mockImplementation(() => {
      throw new Error('note failed');
    });
    const { sc } = make(markets);
    await sc.collect();
    expect(h.logger.warn).toHaveBeenCalledWith('StatsCollector: market update failed', expect.objectContaining({ error: 'note failed' }));
    expect(h.addBreadcrumb).toHaveBeenCalledWith('StatsCollector completed with errors', { updated: 0, errors: 1, totalMarkets: 1 });
  });

  it('a failed batch RPC counts every market in the batch and the sweep continues', async () => {
    const markets = registered(26); // two batches: 25 + 1
    h.getMultipleAccountsInfo.mockRejectedValueOnce(new Error('rpc 503')).mockResolvedValueOnce([{ data: new Uint8Array(8) }]);
    const { sc } = make(markets);
    const p = sc.collect();
    await vi.advanceTimersByTimeAsync(500); // inter-batch pause
    await p;
    expect(h.getMultipleAccountsInfo).toHaveBeenCalledTimes(2);
    expect(h.logger.error).toHaveBeenCalledWith('StatsCollector: batch RPC fetch failed', { error: 'rpc 503' });
    expect(h.logger.info).toHaveBeenCalledWith('StatsCollector sweep complete', { updated: 1, errors: 25, totalMarkets: 26 });
  });

  it('prunes remembered closed slabs that left discovery', async () => {
    const markets = registered();
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }]);
    const { sc } = make(markets);
    sc.closedSlabs.add('delisted');
    sc.closedSlabs.add([...markets.keys()][0]);
    await sc.collect();
    expect([...sc.closedSlabs]).toEqual([[...markets.keys()][0]]);
  });

  it('a hard failure is logged, sent to Sentry, and the collector can collect again', async () => {
    const markets = registered();
    h.getConnection.mockImplementationOnce(() => {
      throw new Error('no connection');
    });
    const { sc } = make(markets);
    await sc.collect();
    expect(h.captureException).toHaveBeenCalledWith(expect.any(Error), {
      tags: { context: 'stats-collector-error' },
      extra: { marketsCount: 1 },
    });
    expect(sc._collecting).toBe(false);
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }]);
    vi.setSystemTime(Date.now() + AUTOCLOSE_INTERVAL_MS + 1);
    await sc.collect();
    expect(h.getMultipleAccountsInfo).toHaveBeenCalledTimes(1);
  });

  it('parses a wrapper (v17+) slab through the group-header reader, tolerating a bad oracle profile', async () => {
    const markets = registered(3);
    h.hasWrapperMagic.mockReturnValue(true);
    h.parseWrapperConfigV17.mockReturnValue({
      collateralMint: MINT,
      maxStalenessSecs: 1n,
      confFilterBps: 1,
      invert: 0,
      unitScale: 1,
      markEwmaE6: 5n,
      tradeFeeBps: 10,
      maintenanceFeePerSlot: 0n,
    });
    h.parseAssetOracleProfileV17
      .mockReturnValueOnce({ oracleAuthority: newKey(), oracleTargetPriceE6: 9n })
      .mockImplementationOnce(() => {
        throw new Error('bad profile');
      });
    // 1: profile parses; 2: profile throws; 3: no profile offset.
    h.readMarketGroupFields
      .mockReturnValueOnce({ vault: 9_000_000n, insurance: 1n, cTot: 1n, asset0ProfileOff: 100 })
      .mockReturnValueOnce({ vault: 9_000_000n, insurance: 1n, cTot: 1n, asset0ProfileOff: 100 })
      .mockReturnValueOnce({ vault: 1n, insurance: 0n, cTot: 0n, asset0ProfileOff: null });
    h.getMultipleAccountsInfo.mockResolvedValue([1, 2, 3].map(() => ({ data: new Uint8Array(600) })));
    const { sc } = make(markets);
    await sc.collect();
    expect(h.parseEngine).not.toHaveBeenCalled();
    expect(h.parseWrapperConfigV17).toHaveBeenCalledTimes(3);
    // Only the third market (dust vault; the v17 shape reports 0 used accounts) is auto-closed.
    expect(h.state.updates).toHaveLength(1);
    expect(h.reportUnknownLayout).not.toHaveBeenCalled();
    expect(h.logger.info).toHaveBeenCalledWith('StatsCollector sweep complete', expect.objectContaining({ updated: 3, errors: 0 }));
  });

  it('skips the oracle profile when the account is shorter than the profile', async () => {
    const markets = registered();
    h.hasWrapperMagic.mockReturnValue(true);
    h.parseWrapperConfigV17.mockReturnValue({ maxStalenessSecs: 1n, markEwmaE6: 0n, tradeFeeBps: 1, maintenanceFeePerSlot: 0n });
    h.readMarketGroupFields.mockReturnValue({ vault: 9n, insurance: 0n, cTot: 0n, asset0ProfileOff: 100 });
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(120) }]);
    const { sc } = make(markets);
    await sc.collect();
    expect(h.parseAssetOracleProfileV17).not.toHaveBeenCalled();
  });
});

describe('collect(): re-enabling excluded markets', () => {
  function excluded(slabAddr: string) {
    h.getMarketRows.mockResolvedValue([{ slab_address: slabAddr, indexer_excluded: true }]);
  }

  it('re-enables an excluded market that is back with a funded vault (v17 layout)', async () => {
    const s = slab();
    excluded(s);
    h.hasWrapperMagic.mockReturnValue(true);
    h.parseWrapperConfigV17.mockReturnValue({ maxStalenessSecs: 1n, markEwmaE6: 0n, tradeFeeBps: 1, maintenanceFeePerSlot: 0n });
    h.readMarketGroupFields.mockReturnValue({ vault: 5_000_000n, insurance: 0n, cTot: 0n, asset0ProfileOff: null });
    h.getAccountInfo.mockResolvedValue({ data: Buffer.alloc(600) });
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(600) }]);
    const { sc } = make(new Map([[s, market()]]));
    sc.closedSlabs.add(s);
    await sc.collect();
    expect(h.state.updates[0]).toEqual({ table: 'markets', patch: { indexer_excluded: false, status: 'active' } });
    expect(sc.closedSlabs.has(s)).toBe(false);
    expect(h.getMultipleAccountsInfo).toHaveBeenCalledTimes(1); // swept this very cycle
  });

  it('re-enables on open accounts alone (legacy layout)', async () => {
    const s = slab();
    excluded(s);
    h.getAccountInfo.mockResolvedValue({ data: Buffer.alloc(100) });
    h.parseEngine.mockReturnValue({ vault: 0n, numUsedAccounts: 4 });
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }]);
    const { sc } = make(new Map([[s, market()]]));
    await sc.collect();
    expect(h.state.updates[0].patch).toEqual({ indexer_excluded: false, status: 'active' });
  });

  it('keeps a dusty excluded market hidden and does not sweep it', async () => {
    const s = slab();
    excluded(s);
    h.getAccountInfo.mockResolvedValue({ data: Buffer.alloc(100) });
    h.parseEngine.mockReturnValue({ vault: 1n, numUsedAccounts: 0 });
    const { sc } = make(new Map([[s, market()]]));
    await sc.collect();
    expect(h.state.updates).toHaveLength(0);
    expect(h.getMultipleAccountsInfo).not.toHaveBeenCalled();
    expect(h.logger.info).toHaveBeenCalledWith('StatsCollector: skipping indexer_excluded slabs', { count: 1, slabs: [s] });
  });

  it('keeps it hidden when the slab cannot be read or parsed (and reports an unknown layout)', async () => {
    const a = slab();
    const b = slab();
    const c = slab();
    h.getMarketRows.mockResolvedValue([a, b, c].map((s) => ({ slab_address: s, indexer_excluded: true })));
    h.getAccountInfo
      .mockResolvedValueOnce(null) // account gone
      .mockRejectedValueOnce(new Error('rpc'))
      .mockResolvedValueOnce({ data: Buffer.alloc(10) });
    h.parseEngine.mockImplementation(() => {
      throw new Error('unknown version');
    });
    const { sc } = make(new Map([[a, market()], [b, market()], [c, market()]]));
    await sc.collect();
    expect(h.state.updates).toHaveLength(0);
    expect(h.reportUnknownLayout).not.toHaveBeenCalledWith(a, expect.anything(), expect.anything());
    expect(h.reportUnknownLayout).toHaveBeenCalledWith(b, expect.any(Error), 'StatsCollector.reenable');
    expect(h.reportUnknownLayout).toHaveBeenCalledWith(c, expect.any(Error), 'StatsCollector.reenable');
  });

  it('keeps it excluded when the re-enable write throws, so it is retried next cycle', async () => {
    const s = slab();
    excluded(s);
    h.getAccountInfo.mockResolvedValue({ data: Buffer.alloc(100) });
    h.parseEngine.mockReturnValue({ vault: 9_000_000n, numUsedAccounts: 1 });
    h.state.updateThrows = true;
    const { sc } = make(new Map([[s, market()]]));
    await sc.collect();
    expect(h.logger.warn).toHaveBeenCalledWith('Failed to re-enable excluded market', expect.objectContaining({ error: 'db down' }));
    expect(h.getMultipleAccountsInfo).not.toHaveBeenCalled();
  });

  it('keeps a market excluded when the re-enable write comes back with an error (not a throw)', async () => {
    const s = slab();
    excluded(s);
    h.getAccountInfo.mockResolvedValue({ data: Buffer.alloc(100) });
    h.parseEngine.mockReturnValue({ vault: 9_000_000n, numUsedAccounts: 1 });
    h.state.updateResult = { error: { message: 'rls denied' } };
    const { sc } = make(new Map([[s, market()]]));
    sc.closedSlabs.add(s);
    await sc.collect();
    expect(h.logger.warn).toHaveBeenCalledWith('Failed to re-enable excluded market', expect.objectContaining({ error: 'rls denied' }));
    expect(h.logger.info).not.toHaveBeenCalledWith(expect.stringContaining('Re-enabled'), expect.anything());
    expect(sc.closedSlabs.has(s)).toBe(true);
    expect(h.getMultipleAccountsInfo).not.toHaveBeenCalled(); // still excluded, so not swept
  });

  it('an excluded market that left discovery stays excluded without any RPC read', async () => {
    excluded(slab());
    const { sc } = make(new Map([[slab(), market()]]));
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }]);
    await sc.collect();
    expect(h.getAccountInfo).not.toHaveBeenCalled();
  });

  it('a failing exclusion read is non-fatal: the sweep still runs', async () => {
    const s = slab();
    // syncMarkets fails (returns null), so the exclusion pass re-reads the table, and that read fails too.
    h.getMarketRows.mockRejectedValue(new Error('db'));
    const { sc } = make(new Map([[s, market()]]));
    h.getMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(8) }]);
    await sc.collect();
    expect(h.getMarketRows).toHaveBeenCalledTimes(2);
    expect(h.getMultipleAccountsInfo).toHaveBeenCalledTimes(1);
  });
});

describe('interval env parsing', () => {
  async function load(env: Record<string, string>) {
    vi.resetModules();
    for (const [k, v] of Object.entries(env)) vi.stubEnv(k, v);
    return import('../../src/services/StatsCollector.js');
  }

  it('uses the defaults when unset', async () => {
    const m = await load({ STATS_COLLECT_INTERVAL_MS: '', STATS_AUTOCLOSE_INTERVAL_MS: '' });
    expect(m.COLLECT_INTERVAL_MS).toBe(60_000);
    expect(m.AUTOCLOSE_INTERVAL_MS).toBe(600_000);
  });

  it('accepts valid overrides', async () => {
    const m = await load({ STATS_COLLECT_INTERVAL_MS: '5000', STATS_AUTOCLOSE_INTERVAL_MS: '9000' });
    expect(m.COLLECT_INTERVAL_MS).toBe(5000);
    expect(m.AUTOCLOSE_INTERVAL_MS).toBe(9000);
  });

  it('falls back (and warns) on junk, zero or negative values', async () => {
    for (const bad of ['abc', '0', '-5']) {
      const m = await load({ STATS_COLLECT_INTERVAL_MS: bad, STATS_AUTOCLOSE_INTERVAL_MS: bad });
      expect(m.COLLECT_INTERVAL_MS).toBe(60_000);
      expect(m.AUTOCLOSE_INTERVAL_MS).toBe(600_000);
      vi.unstubAllEnvs();
    }
    expect(h.logger.warn).toHaveBeenCalledWith(expect.stringContaining('STATS_COLLECT_INTERVAL_MS'), { raw: 'abc' });
    expect(h.logger.warn).toHaveBeenCalledWith(expect.stringContaining('STATS_AUTOCLOSE_INTERVAL_MS'), { raw: 'abc' });
  });
});
