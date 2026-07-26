/**
 * StatsCollector — history-only: registers markets and rolls up trade volume.
 *
 * Runs after each crank cycle to read on-chain slab data and:
 * - Auto-register newly discovered markets (insertMarket)
 * - Auto-close stale/abandoned markets (dust vault + no accounts)
 * - Roll up volume_24h / trade_count_24h into the slim market_stats table
 *   (slab_address, volume_24h, volume_24h_usd, trade_count_24h, last_price,
 *   network, updated_at)
 *
 * REDUCTION (2026-07-26): the fat per-market market_stats write (OI, insurance,
 * vault, funding rate, liquidation counters, etc.) and the oracle_prices /
 * oi_history / insurance_history / funding_history writes were all removed —
 * the frontend reads price/OI/insurance/funding-rate live from chain.
 */
import { PublicKey } from "@solana/web3.js";
import {
  parseEngine,
  detectDexType,
  parseDexPool,
  isV17Account,
  parseWrapperConfigV17,
  parseAssetOracleProfileV17,
  V17_HEADER_LEN,
  V17_MARKET_GROUP_OFF,
  V17_ASSET_ORACLE_PROFILE_LEN,
  type EngineState,
  type MarketConfig,
  type RiskParams,
  type DiscoveredMarket,
} from "@percolatorct/sdk";

/**
 * v17 market group header layout (all offsets relative to V17_MARKET_GROUP_OFF=448).
 * VERIFIED via percolator-prog `cargo run --example dump_layout` (MarketGroupV16HeaderAccount):
 *   +0    market_group_id [u8;32]
 *   +32   config V16ConfigAccount        (249 bytes — INLINE, precedes vault)
 *   +281  asset_slot_capacity u32        (4 bytes)
 *   +285  vault u128
 *   +301  insurance u128
 *   +317  c_tot u128
 * (Earlier +32/48/64 was wrong — it read inside the 249-byte config block.)
 */
const V17_MG_VAULT_OFF = 285;      // abs: V17_MARKET_GROUP_OFF + 285 = 733
const V17_MG_INSURANCE_OFF = 301;  // abs: V17_MARKET_GROUP_OFF + 301 = 749
const V17_MG_C_TOT_OFF = 317;      // abs: V17_MARKET_GROUP_OFF + 317 = 765
const V17_MG_MIN_BYTES = 333;      // must cover the c_tot read at +317 (317 + 16)

/**
 * M1: upper bound for a sane price in micro-USD (1e6). An unset/sentinel u64
 * (e.g. u64::MAX) otherwise divides to ~$1.8e13 and poisons the oracle_prices
 * chart + funding_history. Mirrors markPrice.ts and TradeIndexer.readMarkPriceFromSlab
 * (both use `< 1_000_000_000_000n`). Out-of-range prices are treated as absent (0n).
 */
export const MAX_SANE_PRICE_E6 = 1_000_000_000_000n;

/**
 * M1: return the price in micro-USD if it is in (0, MAX_SANE_PRICE_E6), else 0n.
 * Extracted as a pure helper so the sentinel guard is unit-testable outside the
 * ~1500-line collect() method.
 */
export function sanePriceE6(priceE6: bigint): bigint {
  return priceE6 > 0n && priceE6 < MAX_SANE_PRICE_E6 ? priceE6 : 0n;
}

/**
 * Market group header length between V17_MARKET_GROUP_OFF and the first
 * AssetOracleProfileV17. From the desync doc: asset-0 oracle profile at
 * abs offset 1206 = 448 + 758, so MARKET_GROUP_HDR_LEN = 758.
 */
const V17_MARKET_GROUP_HDR_LEN = 758;
const V17_ASSET0_PROFILE_OFF = V17_MARKET_GROUP_OFF + V17_MARKET_GROUP_HDR_LEN; // 1206

function readU128LESB(data: Uint8Array, offset: number): bigint {
  const dv = new DataView(data.buffer, data.byteOffset + offset, 16);
  const lo = dv.getBigUint64(0, true);
  const hi = dv.getBigUint64(8, true);
  return lo | (hi << 64n);
}

/**
 * Parse a v17 account and return v12-compatible engine/config/params shapes.
 *
 * Desync fixes 2, 3, 4:
 *   - Use parseWrapperConfigV17 for config fields (correct v17 offsets)
 *   - Use parseAssetOracleProfileV17 for oracleAuthority / price
 *   - Read vault and insurance from the v17 market group header
 */
function parseV17AccountStats(data: Uint8Array): {
  engine: EngineState;
  marketConfig: MarketConfig;
  params: RiskParams;
} {
  const cfg = parseWrapperConfigV17(data, V17_HEADER_LEN);

  // Asset-0 oracle profile for oracleAuthority and authorityPriceE6
  const zeroKey = new PublicKey(new Uint8Array(32));
  let oracleAuthority = zeroKey;
  let authorityPriceE6 = 0n;
  if (data.length >= V17_ASSET0_PROFILE_OFF + V17_ASSET_ORACLE_PROFILE_LEN) {
    try {
      const op = parseAssetOracleProfileV17(data, V17_ASSET0_PROFILE_OFF);
      oracleAuthority = op.oracleAuthority;
      authorityPriceE6 = op.oracleTargetPriceE6;
    } catch {
      // Asset oracle profile unavailable — use zero authority (Pyth-pinned fallback)
    }
  }

  // markEwmaE6 from WrapperConfigV17 (offset 232 within config block; absolute 16+232=248)
  const lastEffectivePriceE6 = cfg.markEwmaE6;

  const marketConfig: MarketConfig = {
    collateralMint: cfg.collateralMint,
    vaultPubkey: zeroKey,
    indexFeedId: zeroKey,           // zeroed: v17 has no global Pyth indexFeedId
    maxStalenessSlots: cfg.maxStalenessSecs,
    confFilterBps: cfg.confFilterBps,
    vaultAuthorityBump: 0,
    invert: cfg.invert,
    unitScale: cfg.unitScale,
    fundingHorizonSlots: 0n,
    fundingKBps: 0n,
    fundingInvScaleNotionalE6: 0n,
    fundingMaxPremiumBps: 0n,
    fundingMaxBpsPerSlot: 0n,
    threshFloor: 0n,
    threshRiskBps: 0n,
    threshUpdateIntervalSlots: 0n,
    threshStepBps: 0n,
    threshAlphaBps: 0n,
    threshMin: 0n,
    threshMax: 0n,
    threshMinStep: 0n,
    oracleAuthority,
    authorityPriceE6,
    authorityTimestamp: 0n,
    oraclePriceCapE2bps: 0n,
    lastEffectivePriceE6,
    oiCapMultiplierBps: 0n,
    maxPnlCap: 0n,
    adaptiveFundingEnabled: false,
    adaptiveScaleBps: 0,
    adaptiveMaxFundingBps: 0n,
    marketCreatedSlot: 0n,
    oiRampSlots: 0n,
    resolvedSlot: 0n,
    insuranceIsolationBps: 0,
    oraclePhase: 0,
    cumulativeVolumeE6: 0n,
    phase2DeltaSlots: 0,
    dexPool: null,
  };

  // Read vault and insurance from v17 market group header (desync fix 4)
  const mgOff = V17_MARKET_GROUP_OFF;
  const hasGroupHeader = data.length >= mgOff + V17_MG_MIN_BYTES;
  const vault = hasGroupHeader ? readU128LESB(data, mgOff + V17_MG_VAULT_OFF) : 0n;
  const insurance = hasGroupHeader ? readU128LESB(data, mgOff + V17_MG_INSURANCE_OFF) : 0n;
  const cTot = hasGroupHeader ? readU128LESB(data, mgOff + V17_MG_C_TOT_OFF) : 0n;

  const engine: EngineState = {
    vault,
    insuranceFund: { balance: insurance, feeRevenue: 0n, isolatedBalance: 0n, isolationBps: 0 },
    currentSlot: 0n,
    fundingIndexQpbE6: 0n,
    lastFundingSlot: 0n,
    fundingRateBpsPerSlotLast: 0n,
    fundingRateE9: 0n,
    marketMode: null,
    lastCrankSlot: 0n,
    maxCrankStalenessSlots: 0n,
    totalOpenInterest: 0n,
    longOi: 0n,
    shortOi: 0n,
    cTot,
    pnlPosTot: 0n,
    pnlMaturedPosTot: 0n,
    liqCursor: 0,
    gcCursor: 0,
    lastSweepStartSlot: 0n,
    lastSweepCompleteSlot: 0n,
    crankCursor: 0,
    sweepStartIdx: 0,
    lifetimeLiquidations: 0n,
    lifetimeForceCloses: 0n,
    netLpPos: 0n,
    lpSumAbs: 0n,
    lpMaxAbs: 0n,
    lpMaxAbsSweep: 0n,
    emergencyOiMode: false,
    emergencyStartSlot: 0n,
    lastBreakerSlot: 0n,
    numUsedAccounts: 0,
    nextAccountId: 0n,
    markPriceE6: lastEffectivePriceE6,
    oraclePriceE6: 0n,
    fLongNum: 0n,
    fShortNum: 0n,
    negPnlAccountCount: 0n,
    fundPxLast: 0n,
    resolvedKLongTerminalDelta: 0n,
    resolvedKShortTerminalDelta: 0n,
    resolvedLivePrice: 0n,
  };

  const params: RiskParams = {
    warmupPeriodSlots: 0n,
    maintenanceMarginBps: 0n,
    initialMarginBps: 500n,   // default 20x
    tradingFeeBps: BigInt(cfg.tradeFeeBps),
    maxAccounts: 0n,
    newAccountFee: 0n,
    riskReductionThreshold: 0n,
    maintenanceFeePerSlot: cfg.maintenanceFeePerSlot,
    maxCrankStalenessSlots: 0n,
    liquidationFeeBps: 0n,
    liquidationFeeCap: 0n,
    liquidationBufferBps: 0n,
    minLiquidationAbs: 0n,
    minInitialDeposit: 0n,
    minNonzeroMmReq: 0n,
    minNonzeroImReq: 0n,
    insuranceFloor: 0n,
    hMin: 0n,
    hMax: 0n,
  };

  return { engine, marketConfig, params };
}
import {
  getConnection,
  upsertMarketStats,
  getMarkets,
  insertMarket,
  getSupabase,
  getNetwork,
  withRetry,
  createLogger,
  captureException,
  addBreadcrumb,
} from "@percolatorct/shared";

/**
 * How often to sync volume/trade_count for ALL DB markets, including those not
 * in the on-chain market provider (stale/uncranked markets). This ensures
 * volume_24h and trade_count_24h stay accurate even for markets that are no
 * longer being actively cranked on-chain.
 *
 * Runs every 5 minutes — less frequent than the full collect cycle (2 min)
 * because it issues a single bulk trade fetch + N upserts (cheap), but still
 * infrequent enough to avoid hammering the DB under high trade volume.
 */
const VOLUME_SYNC_INTERVAL_MS = 10 * 60_000;

const logger = createLogger("indexer:stats-collector");

/** Market provider interface — allows different market discovery strategies */
export interface MarketProvider {
  getMarkets(): Map<string, { market: DiscoveredMarket }>;
}

/**
 * How often to collect stats. Configurable via `STATS_COLLECT_INTERVAL_MS` (ms).
 *
 * Default: 60_000 (1 min). The old hardcoded 5 min left `oracle_prices` with long
 * gaps — frontend price charts need denser samples for smooth backfill. Keep the
 * env override so we can crank it down further if the RPC budget allows, or up
 * if we need to back off.
 */
export const COLLECT_INTERVAL_MS: number = (() => {
  const raw = process.env.STATS_COLLECT_INTERVAL_MS;
  if (!raw) return 60_000;
  const n = Number(raw);
  if (Number.isFinite(n) && n > 0) {
    return n;
  } else {
    logger.warn("Invalid STATS_COLLECT_INTERVAL_MS env var, falling back to 60000ms", { raw });
    return 60_000;
  }
})();

export class StatsCollector {
  private timer: ReturnType<typeof setInterval> | null = null;
  private volumeTimer: ReturnType<typeof setInterval> | null = null;
  private volumeInitTimeout: ReturnType<typeof setTimeout> | null = null;
  private _running = false;
  private _collecting = false;
  private _syncingVolume = false;
  /**
   * Tracks slabs already marked as closed this session to avoid repeated DB writes.
   *
   * Purpose: once we have written `status=closed` to the DB for a slab, we record
   * it here so the next collect cycle doesn't issue the same UPDATE again (the DB
   * `.neq("status","closed")` guard is the true fence; this is a cheap in-memory
   * pre-check to skip the RPC round-trip entirely).
   *
   * Relationship to `excludedSlabs` (local var rebuilt from DB each cycle): a slab
   * in `closedSlabs` will also appear in `excludedSlabs` on the next cycle once the
   * DB write has committed, so the two sets are complementary — `closedSlabs` prevents
   * re-issuing the write; `excludedSlabs` prevents re-processing the slab at all.
   * We do NOT need to delete from `closedSlabs` when adding to `excludedSlabs` because
   * the "already closed" guard on the DB update (`neq("status","closed")`) makes the
   * write idempotent anyway. Entries are pruned at the bottom of collect() for slabs
   * that leave the discovery map.
   *
   * LRU bound (#132): capped at MAX_CLOSED_SLABS_CACHE entries. We evict the oldest
   * entry BEFORE adding the new one when the set is at capacity so the size never
   * exceeds the limit even transiently.
   */
  private closedSlabs = new Set<string>();
  private static readonly MAX_CLOSED_SLABS_CACHE = 1000;

  /**
   * Add `slabAddress` to the closedSlabs LRU cache.
   * Evicts the insertion-order-oldest entry first if the cache is already full,
   * so the set size never exceeds MAX_CLOSED_SLABS_CACHE (#132 off-by-one fix).
   */
  private recordClosedSlab(slabAddress: string): void {
    if (this.closedSlabs.has(slabAddress)) return; // already present — no-op
    if (this.closedSlabs.size >= StatsCollector.MAX_CLOSED_SLABS_CACHE) {
      // Evict before adding: Set iteration order is insertion order, so
      // values().next().value is the oldest entry. Evict it first so the
      // set size stays at MAX_CLOSED_SLABS_CACHE and never hits MAX+1.
      const oldest = this.closedSlabs.values().next().value;
      if (oldest !== undefined) this.closedSlabs.delete(oldest);
    }
    this.closedSlabs.add(slabAddress);
  }

  constructor(
    private readonly marketProvider: MarketProvider,
  ) {}

  start(): void {
    if (this._running) return;
    this._running = true;

    // Initial collection after a short delay
    setTimeout(() => this.collect(), 10_000);

    // Periodic collection
    this.timer = setInterval(() => this.collect(), COLLECT_INTERVAL_MS);

    // Volume sync for ALL DB markets (including uncranked ones) — runs independently.
    // First sync after 30s to let the indexer warm up, then every 5 minutes.
    this.volumeInitTimeout = setTimeout(() => this.syncVolumeForAllDBMarkets(), 30_000);
    this.volumeTimer = setInterval(() => this.syncVolumeForAllDBMarkets(), VOLUME_SYNC_INTERVAL_MS);

    logger.info("StatsCollector started", {
      intervalMs: COLLECT_INTERVAL_MS,
      volumeSyncIntervalMs: VOLUME_SYNC_INTERVAL_MS,
    });
  }

  stop(): void {
    this._running = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.volumeTimer) {
      clearInterval(this.volumeTimer);
      this.volumeTimer = null;
    }
    if (this.volumeInitTimeout) {
      clearTimeout(this.volumeInitTimeout);
      this.volumeInitTimeout = null;
    }
    logger.info("StatsCollector stopped");
  }

  /**
   * Sync volume_24h and trade_count_24h for ALL markets in the DB.
   *
   * StatsCollector.collect() only processes markets discovered on-chain. Markets
   * that are deployed but no longer actively cranked (e.g. test markets, stale slabs)
   * fall out of the on-chain provider map and never get their volume updated.
   *
   * This method fetches all trades in the last 24h, aggregates by slab_address, and
   * bulk-upserts volume_24h + trade_count_24h for every market that has trades.
   * It intentionally does NOT reset volume to 0 for markets with no trades — those
   * are left unchanged (they'll naturally reach 0 as their last trades age out and
   * the on-chain collect cycle picks them up).
   *
   * Bug fixed: GH#1171 — volume_24h = 0 for all markets despite trades existing.
   */
  private async syncVolumeForAllDBMarkets(): Promise<void> {
    if (this._syncingVolume || !this._running) return;
    this._syncingVolume = true;

    try {
      const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();

      // Paginated fetch: read all trades in last 24h in pages of PAGE_SIZE.
      // Previously capped at 10k rows — if >10k trades occurred in 24h,
      // volume was silently under-reported. Now fetches all pages with a
      // safety cap (MAX_PAGES) to prevent runaway memory usage.
      const PAGE_SIZE = 5_000;
      const MAX_PAGES = 20; // 100k trades max — far beyond expected 24h volume
      const allTrades: Array<{ slab_address: string; size: string }> = [];
      let page = 0;
      let hasMore = true;

      while (hasMore && page < MAX_PAGES) {
        const from = page * PAGE_SIZE;
        const to = from + PAGE_SIZE - 1;
        const { data: batch, error } = await getSupabase()
          .from("trades")
          .select("slab_address, size")
          .eq("network", getNetwork())
          .gte("created_at", since)
          .range(from, to);

        if (error) {
          logger.warn("syncVolumeForAllDBMarkets: trade fetch failed", { error: error.message, page });
          return;
        }

        if (!batch || batch.length === 0) break;
        allTrades.push(...batch);
        hasMore = batch.length === PAGE_SIZE;
        page++;
      }

      if (page >= MAX_PAGES) {
        logger.warn("syncVolumeForAllDBMarkets: hit max page limit — volume may be under-reported", {
          totalFetched: allTrades.length,
          maxPages: MAX_PAGES,
          pageSize: PAGE_SIZE,
        });
      }

      if (allTrades.length === 0) return;

      // Aggregate volume + trade count by slab_address in memory
      const volumeMap = new Map<string, { volume: bigint; count: number }>();
      for (const trade of allTrades) {
        const current = volumeMap.get(trade.slab_address) ?? { volume: 0n, count: 0 };
        try {
          const raw = BigInt(trade.size);
          const abs = raw < 0n ? -raw : raw;
          volumeMap.set(trade.slab_address, { volume: current.volume + abs, count: current.count + 1 });
        } catch {
          // Fallback: size string isn't a valid BigInt literal. Parse via BigInt()
          // instead of Math.abs(Number()) to avoid precision loss on large values.
          try {
            const numVal = Number(trade.size);
            if (!Number.isFinite(numVal)) continue; // skip Infinity/NaN
            const abs = BigInt(Math.trunc(Math.abs(numVal)));
            volumeMap.set(trade.slab_address, { volume: current.volume + abs, count: current.count + 1 });
          } catch {
            // Completely unparseable size — skip this trade
            logger.warn("syncVolumeForAllDBMarkets: unparseable trade size, skipping", {
              slabAddress: trade.slab_address?.slice(0, 8),
              size: String(trade.size).slice(0, 30),
            });
          }
        }
      }

      // Upsert volume stats for each market that has trades.
      // volume_24h is NUMERIC in PostgreSQL so it can hold arbitrary precision,
      // but MarketStatsRow types it as number|null. Use Number() with a warning
      // when precision would be lost (> MAX_SAFE_INTEGER = ~9e15).
      let updated = 0;
      for (const [slabAddress, { volume, count }] of volumeMap.entries()) {
        try {
          const exceedsSafeInt = volume > BigInt(Number.MAX_SAFE_INTEGER);
          const volumeNum = Number(volume);
          if (exceedsSafeInt) {
            logger.warn("syncVolumeForAllDBMarkets: volume exceeds MAX_SAFE_INTEGER, precision loss", {
              slabAddress: slabAddress.slice(0, 8),
              volumeBigInt: volume.toString(),
              volumeNumber: volumeNum,
            });
          }
          await upsertMarketStats({
            slab_address: slabAddress,
            volume_24h: exceedsSafeInt ? (volume.toString() as any) : volumeNum,
            trade_count_24h: count,
          });
          updated++;
        } catch (err) {
          logger.warn("syncVolumeForAllDBMarkets: upsert failed", {
            slabAddress: slabAddress.slice(0, 8),
            error: err instanceof Error ? err.message : err,
          });
        }
      }

      if (updated > 0) {
        logger.info("Volume sync complete", { marketsUpdated: updated, totalTrades: allTrades.length, pages: page });
      }
    } catch (err) {
      logger.warn("syncVolumeForAllDBMarkets failed", { error: err instanceof Error ? err.message : err });
    } finally {
      this._syncingVolume = false;
    }
  }

  /**
   * Auto-register missing markets: compare on-chain markets vs DB and insert any missing.
   */
  private async syncMarkets(): Promise<void> {
    try {
      // Get on-chain markets from market provider
      const onChainMarkets = this.marketProvider.getMarkets();
      if (onChainMarkets.size === 0) return;

      // Get existing markets from DB
      const dbMarkets = await getMarkets();
      const dbSlabAddresses = new Set(dbMarkets.map(m => m.slab_address));

      // Find missing markets
      const missingMarkets: Array<[string, any]> = [];
      for (const [slabAddress, state] of onChainMarkets.entries()) {
        if (!dbSlabAddresses.has(slabAddress)) {
          missingMarkets.push([slabAddress, state]);
        }
      }

      if (missingMarkets.length === 0) return;

      logger.info("New markets found", { count: missingMarkets.length });

      // Insert missing markets
      const connection = getConnection();
      for (const [slabAddress, state] of missingMarkets) {
        try {
          const market = state.market;
          // v17 market group accounts expose config under `configV17` (WrapperConfigV17);
          // `config`/`header`/`params` are v12-only and undefined for v17. Prefer configV17.
          const cfg: any = (market as any).configV17 ?? market.config;
          const mintAddress = cfg?.collateralMint?.toBase58() ?? "";
          if (!mintAddress) {
            logger.warn("Skipping market registration — no collateralMint", { slabAddress });
            continue;
          }
          // configV17 has no oracleAuthority / authorityPriceE6 / admin / margin — those are
          // per-asset or v12-only; fall back safely (registry only needs mint + a deployer).
          const oracleAuthority = cfg?.oracleAuthority?.toBase58() ?? "";
          const admin = (market as any).header?.admin?.toBase58() ?? (oracleAuthority || mintAddress);
          const priceE6 = Number(cfg?.authorityPriceE6 ?? cfg?.markEwmaE6 ?? 0n);
          const initialMarginBps = Number(market.params?.initialMarginBps ?? cfg?.initialMarginBps ?? 0n);

          // Compute maxLeverage from initialMarginBps.
          // Guard against division-by-zero or garbage values (e.g. uninitialized slab
          // where initialMarginBps=0). Previously we skipped these slabs entirely, which
          // caused FK violations on market_stats inserts when the slab was cranked before
          // being registered in the markets table (GH#1748: SKR slab Bk7XfKWs3Sr).
          //
          // Fix: use a safe default of max_leverage=10 (1000bps = 10% initial margin)
          // instead of skipping, so the market row exists in DB and stats can be written.
          // This is conservative and prevents the FK miss that stalls stats collection.
          let maxLeverage: number;
          if (!initialMarginBps || initialMarginBps <= 0 || !Number.isFinite(initialMarginBps)) {
            logger.warn("Invalid initialMarginBps — registering market with default max_leverage=10 (GH#1748)", {
              slabAddress,
              initialMarginBps,
            });
            maxLeverage = 10;
          } else {
            maxLeverage = Math.floor(10000 / initialMarginBps);
          }

          // Guard: ensure computed maxLeverage is a valid positive integer.
          // Math.floor(Infinity) = Infinity, NaN can propagate via type coercion, and
          // JSON serialisation converts Infinity/NaN to null — violating the DB NOT NULL
          // constraint (error code 23502). Slab 7dVewVxW triggers this path.
          if (!Number.isFinite(maxLeverage) || maxLeverage <= 0 || !Number.isInteger(maxLeverage)) {
            logger.warn("Computed maxLeverage is invalid — registering with default max_leverage=10 (GH#1748)", {
              slabAddress,
              initialMarginBps,
              maxLeverage,
            });
            maxLeverage = 10;
          }
          
          // Try to resolve token metadata from on-chain (Helius DAS / Metaplex)
          let symbol = mintAddress.substring(0, 8); // fallback
          let name = `Market ${slabAddress.substring(0, 8)}`; // fallback
          let decimals = 9;
          try {
            const mintPubkey = new PublicKey(mintAddress);
            const mintInfo = await connection.getParsedAccountInfo(mintPubkey);
            if (mintInfo.value?.data && "parsed" in mintInfo.value.data) {
              decimals = mintInfo.value.data.parsed.info.decimals ?? 9;
            }
            // Try Helius DAS API if the RPC endpoint supports it
            const endpoint = connection.rpcEndpoint;
            if (endpoint.includes("helius-rpc.com")) {
              const dasRes = await fetch(endpoint, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                  jsonrpc: "2.0",
                  id: `das-${mintAddress}`,
                  method: "getAsset",
                  params: { id: mintAddress, options: { showFungible: true } },
                }),
                signal: AbortSignal.timeout(5000),
              });
              if (dasRes.ok) {
                const dasJson = await dasRes.json();
                const metadata = dasJson?.result?.content?.metadata;
                const tokenInfo = dasJson?.result?.token_info;
                const dasSym = metadata?.symbol || tokenInfo?.symbol;
                const dasName = metadata?.name;
                const dasDecimals = tokenInfo?.decimals;
                // Sanitize external metadata: truncate length and strip control
                // characters / HTML to prevent DB bloat and stored XSS vectors
                // (defense-in-depth — frontend must also escape).
                if (typeof dasSym === "string" && dasSym.length > 0) {
                  symbol = dasSym.replace(/[\x00-\x1f<>]/g, "").slice(0, 32);
                }
                if (typeof dasName === "string" && dasName.length > 0) {
                  name = dasName.replace(/[\x00-\x1f<>]/g, "").slice(0, 128);
                }
                if (dasDecimals != null) decimals = dasDecimals;
              }
            }
          } catch (metaErr) {
            logger.debug("Token metadata resolution failed, using fallback", { mintAddress, error: metaErr instanceof Error ? metaErr.message : metaErr });
          }

          // Hyperp markets: override symbol/name with the index asset, not the collateral.
          //
          // A hyperp market uses an on-chain DEX pool as its price oracle instead of Pyth.
          // The collateral is typically USDC, but the market tracks a different base asset
          // (e.g. SOL for a SOL/USDC Perp). Without this override, auto-discovery stores
          // symbol="USDC" / name="USD Coin" which is misleading in the UI.
          //
          // Detection: indexFeedId == [0;32] identifies a hyperp market.
          // Resolution path:
          //   1. Read the dexPool address from MarketConfig (set via SetDexPool instruction).
          //   2. Fetch and parse the pool account to extract baseMint.
          //   3. Look up baseMint metadata via DAS API → use as symbol/name.
          //   4. Construct the market name as "{baseSymbol}/USDC Perpetual".
          //   5. Fall back to "SOL" / "SOL/USDC Perpetual" on any failure (only one hyperp
          //      market type exists today; this should be generalised when more are added).
          const zeroKeyBytesHyperp = new Uint8Array(32);
          const isHyperpMarket = (market as any).configV17 != null
            || (cfg?.indexFeedId?.equals(new PublicKey(zeroKeyBytesHyperp)) ?? false);
          if (isHyperpMarket) {
            let baseSymbol = "SOL";  // safe default: SOL/USDC is the only hyperp type today
            let baseName = "Solana"; // safe default
            let resolvedFromChain = false;
            try {
              const dexPool = cfg?.dexPool ?? null;
              if (dexPool != null) {
                const poolAccountInfo = await connection.getAccountInfo(dexPool);
                if (poolAccountInfo) {
                  const dexType = detectDexType(poolAccountInfo.owner);
                  if (dexType != null) {
                    const poolInfo = parseDexPool(dexType, dexPool, new Uint8Array(poolAccountInfo.data));
                    const baseMintAddress = poolInfo.baseMint.toBase58();
                    // Resolve base mint metadata via DAS (same pattern as collateral above)
                    const endpoint = connection.rpcEndpoint;
                    if (endpoint.includes("helius-rpc.com")) {
                      const baseDasRes = await fetch(endpoint, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({
                          jsonrpc: "2.0",
                          id: `das-base-${baseMintAddress}`,
                          method: "getAsset",
                          params: { id: baseMintAddress, options: { showFungible: true } },
                        }),
                        signal: AbortSignal.timeout(5000),
                      });
                      if (baseDasRes.ok) {
                        const baseDasJson = await baseDasRes.json();
                        const baseMeta = baseDasJson?.result?.content?.metadata;
                        const baseTokenInfo = baseDasJson?.result?.token_info;
                        const rawSym = baseMeta?.symbol || baseTokenInfo?.symbol;
                        const rawName = baseMeta?.name;
                        if (typeof rawSym === "string" && rawSym.length > 0) {
                          baseSymbol = rawSym.replace(/[\x00-\x1f<>]/g, "").slice(0, 32);
                          resolvedFromChain = true;
                        }
                        if (typeof rawName === "string" && rawName.length > 0) {
                          baseName = rawName.replace(/[\x00-\x1f<>]/g, "").slice(0, 128);
                        }
                      }
                    } else {
                      // Non-Helius endpoint: resolve via parsed account info for decimals only;
                      // symbol stays at default "SOL" until a DAS-capable endpoint is available.
                      resolvedFromChain = false;
                    }
                  }
                }
              }
            } catch (hyperpErr) {
              logger.debug("Hyperp base asset resolution failed, using SOL fallback", {
                slabAddress,
                error: hyperpErr instanceof Error ? hyperpErr.message : hyperpErr,
              });
            }
            // Build market symbol/name from base asset.
            // Collateral symbol is already in `symbol` from the DAS lookup above (e.g. "USDC").
            // If collateral lookup also failed, fall back gracefully.
            const collateralLabel = symbol === mintAddress.substring(0, 8) ? "USDC" : symbol;
            symbol = baseSymbol;
            name = `${baseSymbol}/${collateralLabel} Perpetual`;
            logger.info("Hyperp market metadata resolved", {
              slabAddress,
              baseSymbol,
              baseName,
              collateralLabel,
              resolvedFromChain,
              dexPool: cfg?.dexPool?.toBase58() ?? null,
            });
          }

          // Validate decimals: SPL tokens use 0-18. Values outside this range
          // indicate corrupted metadata (wrong byte offset, garbage DAS response).
          if (decimals < 0 || decimals > 18 || !Number.isInteger(decimals)) {
            logger.warn("Invalid token decimals detected, clamping to default", {
              mintAddress, rawDecimals: decimals, fallback: 6,
            });
            decimals = 6;
          }
          
          // Clamp decimals to sane range — some on-chain mints have garbage values
          const clampedDecimals = Math.min(Math.max(decimals, 0), 18);
          await insertMarket({
            slab_address: slabAddress,
            mint_address: mintAddress,
            symbol,
            name,
            decimals: clampedDecimals,
            deployer: admin,
            oracle_authority: oracleAuthority,
            initial_price_e6: priceE6,
            max_leverage: maxLeverage,
            trading_fee_bps: 10,
            lp_collateral: null,
            matcher_context: null,
            status: "active",
          });

          logger.info("Market registered", { slabAddress, symbol, name });
        } catch (err) {
          logger.warn("Failed to register market", { slabAddress, error: err instanceof Error ? err.message : err });
        }
      }
    } catch (err) {
      logger.error("Market sync failed", { error: err instanceof Error ? err.message : String(err), stack: err instanceof Error ? err.stack : undefined });
    }
  }

  /**
   * Collect stats for all known markets by reading on-chain slab accounts.
   */
  private async collect(): Promise<void> {
    if (this._collecting || !this._running) return;
    this._collecting = true;

    try {
      // Auto-register missing markets at the start of each cycle
      await this.syncMarkets();

      const markets = this.marketProvider.getMarkets();
      if (markets.size === 0) {
        logger.warn("StatsCollector.collect: marketProvider has 0 markets, skipping");
        return;
      }

      logger.info("StatsCollector.collect started", { marketCount: markets.size });

      // GH#1218: load indexer_excluded flags from DB to skip corrupt slabs.
      // These are slabs where on-chain state is permanently corrupt and re-syncing
      // would overwrite the zeroed DB values with garbage data.
      //
      // Recovery path: if an excluded market reappears in the on-chain discovery
      // map (meaning it has live accounts or vault > dust), re-enable it so stats
      // resume. This prevents the auto-close mechanism from permanently hiding
      // markets that recover after being abandoned.
      let excludedSlabs: Set<string> = new Set();
      try {
        const dbMarkets = await getMarkets();
        const excludedDbMarkets = dbMarkets.filter((m) => m.indexer_excluded === true);

        for (const m of excludedDbMarkets) {
          // If the market is back in the live discovery map, it may have recovered.
          // Re-enable it so collect() processes it this cycle.
          if (markets.has(m.slab_address)) {
            const state = markets.get(m.slab_address);
            const engine = state?.market.engine;
            const vault = engine ? BigInt(engine.vault) : 0n;
            const numUsedAccounts = engine ? Number(engine.numUsedAccounts) : 0;
            const isLive = vault > 1_000_000n || numUsedAccounts > 0;

            if (isLive) {
              try {
                await getSupabase()
                  .from("markets")
                  .update({ indexer_excluded: false, status: "active" })
                  .eq("slab_address", m.slab_address)
                  .eq("network", getNetwork());
                this.closedSlabs.delete(m.slab_address);
                logger.info("Re-enabled previously excluded market (back in discovery and active)", {
                  slabAddress: m.slab_address.slice(0, 8),
                  vault: vault.toString(),
                  numUsedAccounts,
                });
              } catch (e) {
                // Non-fatal — will retry next cycle
                logger.warn("Failed to re-enable excluded market", {
                  slabAddress: m.slab_address.slice(0, 8),
                  error: e instanceof Error ? e.message : e,
                });
                excludedSlabs.add(m.slab_address);
              }
            } else {
              excludedSlabs.add(m.slab_address);
            }
          } else {
            excludedSlabs.add(m.slab_address);
          }
        }

        if (excludedSlabs.size > 0) {
          logger.info("StatsCollector: skipping indexer_excluded slabs", { count: excludedSlabs.size, slabs: Array.from(excludedSlabs) });
        }
      } catch {
        // Non-fatal — proceed without exclusion list rather than halting all stats collection
      }

      const connection = getConnection();
      let updated = 0;
      let errors = 0;

      // Process markets in batches of 5 to avoid RPC rate limits
      // Use getMultipleAccountsInfo for batch fetching to reduce RPC round trips
      const entries = Array.from(markets.entries()).filter(([slabAddress]) => !excludedSlabs.has(slabAddress));
      for (let i = 0; i < entries.length; i += 5) {
        const batch = entries.slice(i, i + 5);
        const slabPubkeys = batch.map(([slabAddress]) => new PublicKey(slabAddress));

        try {
          // Batch fetch all account infos in one RPC call
          const accountInfos = await withRetry(
            () => connection.getMultipleAccountsInfo(slabPubkeys),
            { 
              maxRetries: 3, 
              baseDelayMs: 1000, 
              label: `getMultipleAccountsInfo(batch ${i / 5 + 1})` 
            }
          );

          // Process each account
          await Promise.all(batch.map(async ([slabAddress, state], batchIndex) => {
            try {
              const accountInfo = accountInfos[batchIndex];
              if (!accountInfo?.data) {
                // Account doesn't exist on-chain — mark as closed + excluded
                const db = getSupabase();
                await db.from("markets").update({ status: "closed", indexer_excluded: true }).eq("slab_address", slabAddress).eq("network", getNetwork());
                logger.info("Auto-closed non-existent market", { slab: slabAddress.slice(0, 8) });
                return;
              }

            const data = new Uint8Array(accountInfo.data);

            // Parse engine state — v17 dispatch (desync fixes 2, 3, 4).
            // marketConfig/params are intentionally NOT read here anymore: the only
            // remaining market_stats writer is the slim syncVolumeForAllDBMarkets()
            // upsert (volume_24h/trade_count_24h), and the auto-close check below
            // only needs engine.vault / engine.numUsedAccounts.
            let engine: EngineState;
            try {
              if (isV17Account(data)) {
                // v17: use v17-correct parser (parseEngine throws "Unrecognized slab
                // data length" for v17 account sizes — wrong magic + no registered tier)
                engine = parseV17AccountStats(data).engine;
              } else {
                engine = parseEngine(data);
              }
            } catch (parseErr) {
              // Slab too small or invalid — skip
              return;
            }

            // PERC-816/817: Dust vault guard — a market with no meaningful vault
            // liquidity and no open accounts is stale/abandoned and eligible for
            // auto-close below.
            // Threshold: 1,000,000 micro-units (≤ 1 USDC at 6 decimals, dust at 9 decimals).
            // Uses inclusive (<=) to catch vault == 1_000_000 — the exact creation-deposit
            // seed amount the program writes at market creation. A market with vault at
            // the creation boundary has received no real LP deposits.
            const MIN_VAULT_FOR_OI = 1_000_000n;
            const hasDustVault = engine.vault <= MIN_VAULT_FOR_OI;
            const hasNoAccounts = engine.numUsedAccounts === 0;

            // Auto-close detection: if vault is dust and no accounts, mark market as closed.
            // This hides stale/abandoned slabs from the frontend without manual DB edits.
            if (hasDustVault && hasNoAccounts && !this.closedSlabs.has(slabAddress)) {
              try {
                const { error: closeErr } = await getSupabase()
                  .from("markets")
                  .update({ status: "closed", indexer_excluded: true })
                  .eq("slab_address", slabAddress)
                  .eq("network", getNetwork())
                  .neq("status", "closed"); // only update if not already closed
                if (closeErr) {
                  logger.warn("Auto-close market failed", { slabAddress, error: closeErr.message });
                } else {
                  this.recordClosedSlab(slabAddress);
                  logger.info("Auto-closed stale market", { slabAddress, vault: Number(engine.vault), accounts: engine.numUsedAccounts });
                }
              } catch (e) {
                logger.warn("Auto-close market error", { slabAddress, error: e instanceof Error ? e.message : e });
              }
            }

            // REDUCTION (2026-07-26): market_stats is no longer written from collect().
            // The fat per-market upsertMarketStats() write (last/mark/index price, OI,
            // insurance, vault, funding rate, liquidation counters, cTot, pnl, etc.) was
            // removed, along with oracle_prices, oi_history, insurance_history, and
            // funding_history writes — the frontend reads price/OI/insurance/funding-rate
            // live from chain, and chart history is not needed. StatsCollector now only
            // maintains the markets registry (insertMarket via syncMarkets()) and the
            // slim market_stats volume rollup (syncVolumeForAllDBMarkets — slab_address,
            // volume_24h, trade_count_24h only).

              updated++;
            } catch (err) {
              errors++;
              logger.warn("StatsCollector: market update failed", { slabAddress: slabAddress.slice(0, 12), error: err instanceof Error ? err.message : err });
            }
          }));
        } catch (batchErr) {
          // If batch fetch fails, log all markets in batch as errors
          errors += batch.length;
          logger.error("StatsCollector: batch RPC fetch failed", { error: batchErr instanceof Error ? batchErr.message : batchErr });
        }

        // Small delay between batches
        if (i + 5 < entries.length) {
          await new Promise((r) => setTimeout(r, 1_000));
        }
      }

      logger.info("StatsCollector.collect complete", { updated, errors, totalMarkets: markets.size });
      if (errors > 0) {
        addBreadcrumb("StatsCollector completed with errors", {
          updated,
          errors,
          totalMarkets: markets.size,
        });
      }

      // Prune rate-limit maps: remove entries for slabs no longer in discovery.
      // Prevents unbounded growth if markets are delisted over time.
      for (const key of this.closedSlabs) {
        if (!markets.has(key)) this.closedSlabs.delete(key);
      }
    } catch (err) {
      logger.error("StatsCollector.collect failed", { error: err instanceof Error ? err.message : String(err), stack: err instanceof Error ? err.stack : undefined });
      captureException(err, {
        tags: { context: "stats-collector-error" },
        extra: {
          marketsCount: this.marketProvider.getMarkets().size,
        },
      });
    } finally {
      this._collecting = false;
    }
  }
}
