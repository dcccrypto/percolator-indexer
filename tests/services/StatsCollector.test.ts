import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { PublicKey, TransactionInstruction, TransactionMessage } from '@solana/web3.js';

// Mock external dependencies
const mockGetAccountInfo = vi.fn();
const mockGetMultipleAccountsInfo = vi.fn();
const mockGetSignaturesForAddress = vi.fn();
const mockGetTransaction = vi.fn();
const mockGetParsedAccountInfo = vi.fn();

// Registration moved off shared.insertMarket to the indexer-local writer, which
// also carries logo_url / metadata_source (see src/db/insertMarketRow.ts).
// vi.hoisted: vi.mock factories are lifted above const declarations, so the spy
// has to be created in the hoisted scope to be referenceable from the factory.
const { insertMarketRowMock } = vi.hoisted(() => ({ insertMarketRowMock: vi.fn() }));
vi.mock('../../src/db/insertMarketRow.js', () => ({
  insertMarketRow: insertMarketRowMock,
}));

vi.mock('@percolatorct/sdk', () => ({
  parseEngine: vi.fn(),
  // v17 desync additions — default to false so existing v12 test paths pass through.
  isV17Account: vi.fn(() => false),
  parseWrapperConfigV17: vi.fn(),
  parseAssetOracleProfileV17: vi.fn(),
  V17_HEADER_LEN: 16,
  V17_MARKET_GROUP_OFF: 448,
  V17_ASSET_ORACLE_PROFILE_LEN: 400,
  detectDexType: vi.fn(() => null),
  parseDexPool: vi.fn(),
}));

vi.mock('@percolatorct/shared', () => ({
  createLogger: vi.fn(() => ({
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  })),
  getConnection: vi.fn(() => ({
    getAccountInfo: mockGetAccountInfo,
    getMultipleAccountsInfo: mockGetMultipleAccountsInfo,
    getParsedAccountInfo: (...a: unknown[]) => mockGetParsedAccountInfo(...a),
    getSignaturesForAddress: mockGetSignaturesForAddress,
    getTransaction: mockGetTransaction,
    rpcEndpoint: 'https://api.devnet.solana.com',
  })),
  upsertMarketStats: vi.fn(),
  insertOraclePrice: vi.fn(),
  getMarkets: vi.fn(async () => []),
  insertMarket: vi.fn(),
  getSupabase: vi.fn(() => ({
    from: vi.fn(() => ({
      insert: vi.fn().mockResolvedValue({ error: null }),
    })),
  })),
  withRetry: vi.fn(async (fn: any) => fn()),
  addBreadcrumb: vi.fn(),
  captureException: vi.fn(),
}));

import { StatsCollector, COLLECT_INTERVAL_MS, PLACEHOLDER_REGISTRATION_GRACE_MS } from '../../src/services/StatsCollector.js';
import type { MarketProvider } from '../../src/services/StatsCollector.js';
import * as core from '@percolatorct/sdk';
import { V17_INITIAL_MARGIN_BPS_OFF } from '../../src/db/autoMarketFields.js';
import * as shared from '@percolatorct/shared';

const SLAB1 = 'FxfD37s1AZTeWfFQps9Zpebi2dNQ9QSSDtfMKdbsfKrD';
const SLAB2 = 'FwfBKZXbYr4vTK23bMFkbgKq3npJ3MSDxEaKmq9Aj4Qn';

function makeEngineState(overrides: Record<string, any> = {}) {
  return {
    totalOpenInterest: 1_000_000_000n,
    vault: 500_000_000n,
    insuranceFund: { balance: 100_000_000n, feeRevenue: 50_000_000n },
    numUsedAccounts: 10,
    fundingRateBpsPerSlotLast: 5n,
    netLpPos: 100_000n,
    lpSumAbs: 200_000n,
    lpMaxAbs: 150_000n,
    lifetimeLiquidations: 5n,
    lifetimeForceCloses: 2n,
    cTot: 1_000_000n,
    pnlPosTot: 500_000n,
    lastCrankSlot: 1000n,
    maxCrankStalenessSlots: 100n,
    fundingIndexQpbE6: 0n,
    ...overrides,
  } as any;
}

function makeMockMarket(slabAddress: string) {
  return {
    market: {
      slabAddress: new PublicKey(slabAddress),
      programId: new PublicKey('11111111111111111111111111111111'),
      config: {
        collateralMint: new PublicKey('So11111111111111111111111111111111111111112'),
        oracleAuthority: new PublicKey('SysvarC1ock11111111111111111111111111111111'),
        authorityPriceE6: 1_500_000n,
        lastEffectivePriceE6: 1_500_000n,
        // Non-zero indexFeedId = admin oracle mode (uses authorityPriceE6)
        indexFeedId: new PublicKey('SysvarC1ock11111111111111111111111111111111'),
      },
      params: { maintenanceMarginBps: 500n, initialMarginBps: 1000n },
      header: { admin: new PublicKey('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL') },
    },
  };
}

function setupParseMocks() {
  vi.mocked(core.parseEngine).mockReturnValue(makeEngineState());
}

describe('StatsCollector', () => {
  let statsCollector: StatsCollector;
  let mockMarketProvider: MarketProvider;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
    mockGetParsedAccountInfo.mockResolvedValue({ value: null });
    mockMarketProvider = { getMarkets: vi.fn(() => new Map()) };
    statsCollector = new StatsCollector(mockMarketProvider);
  });

  afterEach(() => {
    statsCollector.stop();
    vi.useRealTimers();
  });

  describe('start and stop', () => {
    it('should start and call collect after initial delay', async () => {
      const markets = new Map([[SLAB1, makeMockMarket(SLAB1)]]);
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(markets);
      mockGetAccountInfo.mockResolvedValue({ data: new Uint8Array(2048) });
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
      setupParseMocks();

      statsCollector.start();

      // Before initial delay — no calls yet
      expect(insertMarketRowMock).not.toHaveBeenCalled();

      // Advance past 10s initial delay
      await vi.advanceTimersByTimeAsync(10_500);

      // collect() registers newly discovered markets (insertMarketRow) — it no longer
      // writes market_stats itself (that's the slim syncVolumeForAllDBMarkets path).
      expect(insertMarketRowMock).toHaveBeenCalledWith(
        expect.objectContaining({ slab_address: SLAB1 })
      );
    });

    it('should stop timer cleanly', async () => {
      const markets = new Map([[SLAB1, makeMockMarket(SLAB1)]]);
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(markets);
      mockGetAccountInfo.mockResolvedValue({ data: new Uint8Array(2048) });
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
      setupParseMocks();

      statsCollector.start();

      // Advance past initial delay to trigger first collect
      await vi.advanceTimersByTimeAsync(10_500);
      const callCountAfterFirstCollect = insertMarketRowMock.mock.calls.length;
      expect(callCountAfterFirstCollect).toBeGreaterThan(0);

      // Stop the collector
      statsCollector.stop();

      // Advance time by 2 full intervals — no further calls should happen
      await vi.advanceTimersByTimeAsync(60_000);

      expect(insertMarketRowMock.mock.calls.length).toBe(callCountAfterFirstCollect);
    });

    it('should not start twice', async () => {
      const markets = new Map([[SLAB1, makeMockMarket(SLAB1)]]);
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(markets);
      mockGetAccountInfo.mockResolvedValue({ data: new Uint8Array(2048) });
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
      setupParseMocks();

      statsCollector.start();
      statsCollector.start(); // second call should be a no-op

      // Advance past initial delay
      await vi.advanceTimersByTimeAsync(10_500);
      const callsAfterInitial = insertMarketRowMock.mock.calls.length;

      // Advance by exactly one more interval (references the exported const so this
      // test stays in sync if the default / env override changes).
      await vi.advanceTimersByTimeAsync(COLLECT_INTERVAL_MS);
      const callsAfterOneInterval = insertMarketRowMock.mock.calls.length;

      // With double-started timers we'd get 2 extra calls; with single timer we get 1
      expect(callsAfterOneInterval).toBe(callsAfterInitial + 1);
    });
  });

  describe('collect', () => {
    it('should read on-chain data and register the market without writing fat market_stats fields', async () => {
      const markets = new Map([[SLAB1, makeMockMarket(SLAB1)]]);
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(markets);
      mockGetAccountInfo.mockResolvedValue({ data: new Uint8Array(2048) });
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
      setupParseMocks();

      statsCollector.start();
      await vi.advanceTimersByTimeAsync(10_500);

      // collect() still registers newly discovered markets...
      expect(insertMarketRowMock).toHaveBeenCalledWith(
        expect.objectContaining({ slab_address: SLAB1 })
      );

      // ...but the fat live-state mirror (OI, insurance, vault, funding rate,
      // liquidation counters, etc.) is no longer written to market_stats — the
      // frontend reads that data live from chain now. The only surviving
      // market_stats writer is syncVolumeForAllDBMarkets (its own timer), which
      // writes only slab_address/volume_24h/trade_count_24h.
      expect(shared.upsertMarketStats).not.toHaveBeenCalledWith(
        expect.objectContaining({ open_interest_long: expect.anything() })
      );
      expect(shared.upsertMarketStats).not.toHaveBeenCalledWith(
        expect.objectContaining({ insurance_fund: expect.anything() })
      );
      expect(shared.upsertMarketStats).not.toHaveBeenCalledWith(
        expect.objectContaining({ total_accounts: expect.anything() })
      );
      expect(shared.upsertMarketStats).not.toHaveBeenCalledWith(
        expect.objectContaining({ total_open_interest: expect.anything() })
      );
    });

    it('does NOT write oracle_prices anymore (reduction 2026-07-26 — read live from chain)', async () => {
      const markets = new Map([[SLAB1, makeMockMarket(SLAB1)]]);
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(markets);
      mockGetAccountInfo.mockResolvedValue({ data: new Uint8Array(2048) });
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
      setupParseMocks();

      statsCollector.start();
      await vi.advanceTimersByTimeAsync(10_500);

      // oracle_prices / oi_history / insurance_history are no longer indexed —
      // the frontend reads price/OI/insurance live via parseMarketGroupV17OI etc.
      expect(shared.insertOraclePrice).not.toHaveBeenCalled();
    });

    it('should handle errored markets gracefully and continue', async () => {
      const markets = new Map([
        [SLAB1, makeMockMarket(SLAB1)],
        [SLAB2, makeMockMarket(SLAB2)],
      ]);
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(markets);
      // getMultipleAccountsInfo returns both in one batch — first null (error), second valid
      mockGetMultipleAccountsInfo.mockResolvedValue([null, { data: new Uint8Array(2048) }]);
      mockGetAccountInfo.mockResolvedValue({ data: new Uint8Array(2048) });
      setupParseMocks();

      statsCollector.start();
      await vi.advanceTimersByTimeAsync(10_500);

      // A null accountInfo for SLAB1 must not abort registration/processing of
      // SLAB2 — both markets are still registered via insertMarket.
      expect(insertMarketRowMock).toHaveBeenCalledWith(
        expect.objectContaining({ slab_address: SLAB1 })
      );
      expect(insertMarketRowMock).toHaveBeenCalledWith(
        expect.objectContaining({ slab_address: SLAB2 })
      );
    });

    it('should handle parse errors gracefully', async () => {
      const markets = new Map([[SLAB1, makeMockMarket(SLAB1)]]);
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(markets);
      mockGetAccountInfo.mockResolvedValue({ data: new Uint8Array(100) });
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(100) }]);
      vi.mocked(core.parseEngine).mockImplementation(() => { throw new Error('Parse error'); });

      statsCollector.start();

      // Should not crash the collect cycle.
      await vi.advanceTimersByTimeAsync(10_500);

      // Market registration (syncMarkets) is independent of slab-data parsing, so
      // it still succeeds even though engine parsing failed for this slab.
      expect(insertMarketRowMock).toHaveBeenCalledWith(
        expect.objectContaining({ slab_address: SLAB1 })
      );
      expect(shared.upsertMarketStats).not.toHaveBeenCalled();
    });

    it('should skip collect when no markets', async () => {
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(new Map());

      statsCollector.start();
      await vi.advanceTimersByTimeAsync(10_500);

      expect(insertMarketRowMock).not.toHaveBeenCalled();
      expect(shared.upsertMarketStats).not.toHaveBeenCalled();
    });
  });

  describe('MarketProvider interface', () => {
    it('should accept any MarketProvider implementation', async () => {
      const customProvider: MarketProvider = { getMarkets: vi.fn(() => new Map()) };
      const collector = new StatsCollector(customProvider);
      collector.start();
      await vi.advanceTimersByTimeAsync(10_500);
      expect(customProvider.getMarkets).toHaveBeenCalled();
      collector.stop();
    });
  });

  // GH#1748: SKR/SEEKER slab Bk7XfKWs3Sr was silently skipped by syncMarkets when
  // initialMarginBps=0, causing FK violation on market_stats insert (stats never written).
  // Fix: use default max_leverage=10 instead of skipping; ensure market is registered.
  // 2026-10-01 real case 9EPm8nB8: the auto row recorded the collateral mint as deployer
  // and a hard-coded fee of 10. See tests/db/autoMarketFields.test.ts.
  describe('auto-registration of a v17/v18 market (9EPm8nB8)', () => {
    const WRAPPER = new PublicKey('ETDLAdiAyWnEUngspYczTXUceT6X8f92eZQvr8nmSkWB');
    const SLAB = new PublicKey('9EPm8nB8Fs7WcEZgE1WGFPTGc6rAzD6GhFJyMm4dEFHn');
    const CREATOR = new PublicKey('9sM73A4MvS2ye2Fuvpr1tmkj68iA61eebuRKz1rnGUWa');
    const MINT = new PublicKey('DJ54k4wH92NTtNP8RuHAwG8si1bevXEknzctDdqYN8eC');
    const MARKETAUTH = new PublicKey('GE8NTjew1sjLVb53J1jUczDKFwn6CAqT1HhbLZ8363QV');
    const v17 = () => ({
      market: {
        slabAddress: SLAB,
        programId: WRAPPER,
        header: {},
        config: {},
        params: {},
        configV17: { marketauth: MARKETAUTH, collateralMint: MINT, tradeFeeBps: 5n, oracleTargetPriceE6: 3630n },
      },
    });

    function creationTx(blockTime: number | null = null) {
      const message = new TransactionMessage({
        payerKey: CREATOR,
        recentBlockhash: '11111111111111111111111111111111',
        instructions: [new TransactionInstruction({
          programId: WRAPPER,
          keys: [
            { pubkey: CREATOR, isSigner: true, isWritable: true },
            { pubkey: SLAB, isSigner: true, isWritable: true },
            { pubkey: MINT, isSigner: false, isWritable: false },
          ],
          data: Buffer.from([0]),
        })],
      }).compileToV0Message();
      return { slot: 1, blockTime, transaction: { message, signatures: [] }, meta: null, version: 0 };
    }

    beforeEach(() => {
      vi.mocked(shared.getMarkets).mockResolvedValue([]);
      vi.stubGlobal('fetch', vi.fn(async () => { throw new Error('no network in tests'); }));
    });
    afterEach(() => vi.unstubAllGlobals());

    it('records the creation-tx signer as deployer and the on-chain fee', async () => {
      mockGetSignaturesForAddress.mockResolvedValue([
        { signature: 'create', slot: 1, err: null, memo: null, blockTime: null },
      ]);
      mockGetTransaction.mockResolvedValue(creationTx());
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(new Map([[SLAB.toBase58(), v17()]]) as never);
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
      // The slab's engine config: initial_margin_bps = 1819 (9EPm8nB8 on chain), so 5x.
      const slabData = new Uint8Array(4096);
      new DataView(slabData.buffer).setBigUint64(V17_INITIAL_MARGIN_BPS_OFF, 1819n, true);
      mockGetAccountInfo.mockResolvedValue({ data: slabData });
      setupParseMocks();

      statsCollector.start();
      await vi.advanceTimersByTimeAsync(10_500);
      // #223: the creation time is unknown here (blockTime null), so nothing is deferred.

      expect(insertMarketRowMock).toHaveBeenCalledWith(expect.objectContaining({
        slab_address: SLAB.toBase58(),
        mint_address: MINT.toBase58(),
        deployer: CREATOR.toBase58(),
        oracle_authority: null,
        initial_price_e6: 3630,
        trading_fee_bps: 5,
        max_leverage: 5,
      }));
      const row = insertMarketRowMock.mock.calls[0][0];
      expect(row.deployer).not.toBe(MINT.toBase58());
    });

    it('falls back to marketauth (never the mint) when the creator lookup fails', async () => {
      mockGetSignaturesForAddress.mockRejectedValue(new Error('429'));
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(new Map([[SLAB.toBase58(), v17()]]) as never);
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
      setupParseMocks();

      statsCollector.start();
      await vi.advanceTimersByTimeAsync(10_500);

      expect(insertMarketRowMock).toHaveBeenCalledWith(expect.objectContaining({
        deployer: MARKETAUTH.toBase58(),
      }));
    });

    // Guards against OVER-deferral (a mutant that holds back every fresh market): it passes on
    // base too, by design — base never defers. The deferral itself is covered by the next block.
    it('#223: a FRESH market whose identity resolves registers on the FIRST pass (no grace)', async () => {
      const DEX_POOL = new PublicKey('8WC8vALsDJhNCUVRmqZBDSg5xgFAhDrgy7zWqF512pDx');
      const BASE = new PublicKey('So11111111111111111111111111111111111111112');
      const nowSec = Math.floor(Date.now() / 1000);
      mockGetSignaturesForAddress.mockResolvedValue([{ signature: 'create', slot: 1, err: null, memo: null, blockTime: nowSec }]);
      mockGetTransaction.mockResolvedValue(creationTx(nowSec));
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(new Map([[SLAB.toBase58(), {
        market: { ...v17().market, configV17: { ...v17().market.configV17, dexPool: DEX_POOL } },
      }]]) as never);
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
      mockGetAccountInfo.mockResolvedValue({ data: new Uint8Array(4096), owner: WRAPPER });
      setupParseMocks();
      const sdk = await import('@percolatorct/sdk');
      vi.mocked(sdk.detectDexType).mockReturnValue('pumpswap' as never);
      vi.mocked(sdk.parseDexPool).mockReturnValue({ baseMint: BASE } as never);
      // DAS only runs against a Helius endpoint; answer it with a real identity.
      const realGetConnection = vi.mocked(shared.getConnection).getMockImplementation();
      vi.mocked(shared.getConnection).mockImplementation(() => ({
        ...(realGetConnection as () => object)(),
        rpcEndpoint: 'https://devnet.helius-rpc.com/?api-key=test',
      }) as never);
      vi.stubGlobal('fetch', vi.fn(async () => ({
        ok: true,
        json: async () => ({ result: { content: { metadata: { symbol: 'WSOL', name: 'Wrapped SOL' }, links: {} }, token_info: {} } }),
      })));

      try {
        statsCollector.start();
        await vi.advanceTimersByTimeAsync(10_500);
        expect(insertMarketRowMock).toHaveBeenCalledWith(expect.objectContaining({
          slab_address: SLAB.toBase58(),
          symbol: 'WSOL',
        }));
      } finally {
        vi.mocked(shared.getConnection).mockImplementation(realGetConnection as never);
        vi.mocked(sdk.detectDexType).mockReturnValue(null as never);
      }
    });

    describe('#223 placeholder grace is measured from on-chain creation time', () => {
      const sigs = (blockTime: number | null) =>
        mockGetSignaturesForAddress.mockResolvedValue([{ signature: 'create', slot: 1, err: null, memo: null, blockTime }]);
      const arm = (blockTime: number | null) => {
        sigs(blockTime);
        mockGetTransaction.mockResolvedValue(creationTx(blockTime));
        vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(new Map([[SLAB.toBase58(), v17()]]) as never);
        mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
        setupParseMocks();
      };
      const nowSec = () => Math.floor(Date.now() / 1000);

      it('an OLD placeholder-only market is written on the first pass', async () => {
        arm(nowSec() - 3600);
        await statsCollector.registerNewMarkets();
        expect(insertMarketRowMock).toHaveBeenCalledTimes(1);
      });

      it('a FRESH placeholder-only market is deferred, then written once the window has passed', async () => {
        arm(nowSec());
        await statsCollector.registerNewMarkets();
        expect(insertMarketRowMock).not.toHaveBeenCalled();
        await vi.advanceTimersByTimeAsync(PLACEHOLDER_REGISTRATION_GRACE_MS + 2_000);
        await statsCollector.registerNewMarkets();
        expect(insertMarketRowMock).toHaveBeenCalledTimes(1);
      });

      it('a restart does not reset the window (a new collector writes an aged market at once)', async () => {
        arm(nowSec());
        await statsCollector.registerNewMarkets();
        expect(insertMarketRowMock).not.toHaveBeenCalled();
        await vi.advanceTimersByTimeAsync(PLACEHOLDER_REGISTRATION_GRACE_MS + 2_000);
        const restarted = new StatsCollector(mockMarketProvider); // empty in-memory state
        await restarted.registerNewMarkets();
        expect(insertMarketRowMock).toHaveBeenCalledTimes(1);
      });

      it('an unknown creation time is never deferred', async () => {
        arm(null);
        await statsCollector.registerNewMarkets();
        expect(insertMarketRowMock).toHaveBeenCalledTimes(1);
      });

      it('a failing creation lookup is never deferred', async () => {
        arm(nowSec());
        mockGetSignaturesForAddress.mockRejectedValue(new Error('429'));
        await statsCollector.registerNewMarkets();
        expect(insertMarketRowMock).toHaveBeenCalledTimes(1);
      });

      it('a deferred market costs no heavy RPC after the first pass (cheap check first, lookup cached)', async () => {
        arm(nowSec());
        await statsCollector.registerNewMarkets();
        const heavy = () => [mockGetSignaturesForAddress, mockGetTransaction, mockGetAccountInfo, mockGetParsedAccountInfo]
          .map((m) => m.mock.calls.length);
        const afterFirst = heavy();
        expect(afterFirst[2]).toBe(0); // slab read / pool read skipped
        expect(afterFirst[3]).toBe(0); // mint read skipped
        for (let i = 0; i < 3; i++) await statsCollector.registerNewMarkets();
        expect(heavy()).toEqual(afterFirst);
      });
    });

    it('#223: concurrent registration calls share one pass (no double insert)', async () => {
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(new Map([[SLAB.toBase58(), v17()]]) as never);
      let release!: (rows: never[]) => void;
      vi.mocked(shared.getMarkets).mockImplementationOnce(() => new Promise((r) => { release = r as never; }));

      const a = statsCollector.registerNewMarkets();
      const b = statsCollector.registerNewMarkets();
      release([]);
      await Promise.all([a, b]);

      expect(shared.getMarkets).toHaveBeenCalledTimes(1);
    });
  });

  describe('GH#1748 — market registration with invalid initialMarginBps', () => {
    it('should register a market with max_leverage=10 when initialMarginBps=0', async () => {
      // Slab is NOT in the DB (getMarkets returns empty), is discovered on-chain with initialMarginBps=0
      vi.mocked(shared.getMarkets).mockResolvedValue([]);
      const marketWithZeroMargin = {
        market: {
          slabAddress: new PublicKey(SLAB1),
          programId: new PublicKey('11111111111111111111111111111111'),
          config: {
            collateralMint: new PublicKey('So11111111111111111111111111111111111111112'),
            oracleAuthority: new PublicKey('SysvarC1ock11111111111111111111111111111111'),
            authorityPriceE6: 1_500_000n,
            lastEffectivePriceE6: 1_500_000n,
            indexFeedId: new PublicKey('SysvarC1ock11111111111111111111111111111111'),
          },
          params: { maintenanceMarginBps: 500n, initialMarginBps: 0n }, // zero margin
          header: { admin: new PublicKey('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL') },
        },
      };
      const markets = new Map([[SLAB1, marketWithZeroMargin]]);
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(markets);
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
      setupParseMocks();

      statsCollector.start();
      await vi.advanceTimersByTimeAsync(10_500);

      // insertMarket should have been called with max_leverage=10 (safe default, not skipped)
      expect(insertMarketRowMock).toHaveBeenCalledWith(
        expect.objectContaining({
          slab_address: SLAB1,
          max_leverage: 10,
        })
      );
    });

    it('should register a market with max_leverage=10 when initialMarginBps produces invalid leverage', async () => {
      vi.mocked(shared.getMarkets).mockResolvedValue([]);
      const marketWithHugeMargin = {
        market: {
          slabAddress: new PublicKey(SLAB1),
          programId: new PublicKey('11111111111111111111111111111111'),
          config: {
            collateralMint: new PublicKey('So11111111111111111111111111111111111111112'),
            oracleAuthority: new PublicKey('SysvarC1ock11111111111111111111111111111111'),
            authorityPriceE6: 1_500_000n,
            lastEffectivePriceE6: 1_500_000n,
            indexFeedId: new PublicKey('SysvarC1ock11111111111111111111111111111111'),
          },
          params: { maintenanceMarginBps: 500n, initialMarginBps: 99999n }, // produces maxLeverage < 1
          header: { admin: new PublicKey('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL') },
        },
      };
      const markets = new Map([[SLAB1, marketWithHugeMargin]]);
      vi.mocked(mockMarketProvider.getMarkets).mockReturnValue(markets);
      mockGetMultipleAccountsInfo.mockResolvedValue([{ data: new Uint8Array(2048) }]);
      setupParseMocks();

      statsCollector.start();
      await vi.advanceTimersByTimeAsync(10_500);

      // initialMarginBps=99999 → Math.floor(10000/99999)=0 → invalid → should use default 10
      expect(insertMarketRowMock).toHaveBeenCalledWith(
        expect.objectContaining({
          slab_address: SLAB1,
          max_leverage: 10,
        })
      );
    });
  });

  // ---------------------------------------------------------------------------
  // #132 (L-1) — closedSlabs LRU off-by-one
  // The `recordClosedSlab` helper must keep size <= MAX_CLOSED_SLABS_CACHE (1000)
  // at ALL times — including immediately after the add (no transient 1001 state).
  // ---------------------------------------------------------------------------
  describe('closedSlabs LRU bound at exactly 1000 (#132)', () => {
    it('recordClosedSlab keeps Set size at exactly MAX_CLOSED_SLABS_CACHE after filling', async () => {
      // Access the private method via a cast to any so we can unit-test it directly.
      const collector = new StatsCollector({ getMarkets: vi.fn(() => new Map()) });
      const c = collector as any;

      // Fill the cache to exactly MAX_CLOSED_SLABS_CACHE
      const MAX = 1000;
      for (let i = 0; i < MAX; i++) {
        c.recordClosedSlab(`Slab${String(i).padStart(8, '0')}111111111111111111111111111111`);
      }
      expect(c.closedSlabs.size).toBe(MAX);

      // Add one more — should evict oldest FIRST, so size stays at MAX
      c.recordClosedSlab(`SlabNew0000111111111111111111111111111111`);
      expect(c.closedSlabs.size).toBe(MAX);
    });

    it('recordClosedSlab size never exceeds MAX_CLOSED_SLABS_CACHE under repeated additions', async () => {
      const collector = new StatsCollector({ getMarkets: vi.fn(() => new Map()) });
      const c = collector as any;
      const MAX = 1000;

      // Add 1100 unique slabs
      for (let i = 0; i < MAX + 100; i++) {
        c.recordClosedSlab(`Slab${String(i).padStart(8, '0')}111111111111111111111111111111`);
        // Invariant: size must never exceed MAX at any point
        expect(c.closedSlabs.size).toBeLessThanOrEqual(MAX);
      }
      // Final size is exactly MAX
      expect(c.closedSlabs.size).toBe(MAX);
    });

    it('recordClosedSlab evicts oldest (insertion-order) entry first', async () => {
      const collector = new StatsCollector({ getMarkets: vi.fn(() => new Map()) });
      const c = collector as any;
      const MAX = 1000;

      // The first slab added is the "oldest"
      const FIRST_SLAB = 'SlabOldest1111111111111111111111111111111';
      c.recordClosedSlab(FIRST_SLAB);

      // Fill the rest to capacity
      for (let i = 1; i < MAX; i++) {
        c.recordClosedSlab(`Slab${String(i).padStart(8, '0')}111111111111111111111111111111`);
      }
      expect(c.closedSlabs.has(FIRST_SLAB)).toBe(true); // still present at capacity

      // Add one more — FIRST_SLAB should be evicted
      c.recordClosedSlab(`SlabOverflow1111111111111111111111111111`);
      expect(c.closedSlabs.has(FIRST_SLAB)).toBe(false);
      expect(c.closedSlabs.size).toBe(MAX);
    });

    it('recordClosedSlab is idempotent: re-adding existing slab does not change size', async () => {
      const collector = new StatsCollector({ getMarkets: vi.fn(() => new Map()) });
      const c = collector as any;
      const SLAB = 'SlabIdempotent11111111111111111111111111';
      c.recordClosedSlab(SLAB);
      c.recordClosedSlab(SLAB);
      c.recordClosedSlab(SLAB);
      expect(c.closedSlabs.size).toBe(1);
    });
  });
});
