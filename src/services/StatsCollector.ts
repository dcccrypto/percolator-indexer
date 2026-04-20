/**
 * StatsCollector — Populates market_stats and oracle_prices tables.
 *
 * Runs after each crank cycle to read on-chain slab data and persist:
 * - Market stats (OI, vault, accounts, insurance, prices)
 * - Oracle prices (for price chart history)
 *
 * This closes two architecture gaps:
 * 1. market_stats table was never populated
 * 2. oracle_prices table was never populated
 */
import { PublicKey } from "@solana/web3.js";
import {
  parseEngine,
  parseConfig,
  parseParams,
  parseAllAccounts,
  detectDexType,
  parseDexPool,
  type EngineState,
  type MarketConfig,
  type RiskParams,
  type DiscoveredMarket,
} from "@percolatorct/sdk";
import { 
  getConnection,
  upsertMarketStats, 
  insertOraclePrice, 
  get24hVolume,
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

/**
 * How often to sync full stats (OI, vault, price, insurance) for DB-registered
 * markets that are NOT in the live MarketDiscovery map.
 *
 * "Orphan" markets — typically admin-oracle slabs where the keeper is not the
 * oracle authority — can fall out of the in-memory discovery map after an
 * indexer redeploy or RPC hiccup. When they do, StatsCollector.collect() never
 * processes them and stats_updated_at goes stale indefinitely (GH#1774).
 *
 * This sync reads their on-chain slab data directly (same as collect()) and
 * upserts stats into the DB, keeping them current regardless of whether
 * MarketDiscovery re-discovers them.
 *
 * Runs every 10 minutes — slower than the main cycle because orphan markets
 * are typically inactive (no positions, no LP), so sub-minute freshness is
 * not required. Batched at 5 slabs per RPC call to avoid rate limits.
 */
const ORPHAN_STATS_SYNC_INTERVAL_MS = 10 * 60_000;

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
  return Number.isFinite(n) && n > 0 ? n : 60_000;
})();

/** How often to log oracle prices to DB (every 60s per market to avoid bloat) */
const ORACLE_LOG_INTERVAL_MS = 60_000;

export class StatsCollector {
  private timer: ReturnType<typeof setInterval> | null = null;
  private volumeTimer: ReturnType<typeof setInterval> | null = null;
  private volumeInitTimeout: ReturnType<typeof setTimeout> | null = null;
  private orphanStatsTimer: ReturnType<typeof setInterval> | null = null;
  private orphanStatsInitTimeout: ReturnType<typeof setTimeout> | null = null;
  private _running = false;
  private _collecting = false;
  private _syncingVolume = false;
  private _syncingOrphanStats = false;
  private lastOracleLogTime = new Map<string, number>();
  private lastOiHistoryTime = new Map<string, number>();
  private lastInsHistoryTime = new Map<string, number>();
  private lastFundingHistoryTime = new Map<string, number>();
  /** Tracks slabs already marked as closed this session to avoid repeated DB writes */
  private closedSlabs = new Set<string>();

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

    // Orphan stats sync — updates OI/vault/price for DB-registered markets not in the
    // live discovery map (e.g. admin-oracle foreign slabs, GH#1774).
    // First run after 60s (let discovery complete), then every 10 minutes.
    this.orphanStatsInitTimeout = setTimeout(() => this.syncStatsForOrphanDBMarkets(), 60_000);
    this.orphanStatsTimer = setInterval(() => this.syncStatsForOrphanDBMarkets(), ORPHAN_STATS_SYNC_INTERVAL_MS);

    logger.info("StatsCollector started", {
      intervalMs: COLLECT_INTERVAL_MS,
      volumeSyncIntervalMs: VOLUME_SYNC_INTERVAL_MS,
      orphanStatsSyncIntervalMs: ORPHAN_STATS_SYNC_INTERVAL_MS,
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
    if (this.orphanStatsTimer) {
      clearInterval(this.orphanStatsTimer);
      this.orphanStatsTimer = null;
    }
    if (this.orphanStatsInitTimeout) {
      clearTimeout(this.orphanStatsInitTimeout);
      this.orphanStatsInitTimeout = null;
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
          const volumeNum = Number(volume);
          if (volume > BigInt(Number.MAX_SAFE_INTEGER)) {
            logger.warn("syncVolumeForAllDBMarkets: volume exceeds MAX_SAFE_INTEGER, precision loss", {
              slabAddress: slabAddress.slice(0, 8),
              volumeBigInt: volume.toString(),
              volumeNumber: volumeNum,
            });
          }
          await upsertMarketStats({
            slab_address: slabAddress,
            volume_24h: volumeNum,
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
   * GH#1774: Sync full stats for DB-registered markets NOT in the live MarketDiscovery map.
   *
   * These "orphan" markets are typically admin-oracle slabs where the keeper is not the
   * oracle authority (foreignOracleSkipped). They can fall out of the in-memory discovery
   * map after an indexer redeploy or an RPC 429 hiccup that caused a partial discovery
   * result. When that happens StatsCollector.collect() never processes them and
   * stats_updated_at becomes permanently stale.
   *
   * This method:
   *   1. Fetches all markets from DB
   *   2. Computes the set NOT in the live discovery map
   *   3. For each orphan: fetches on-chain slab data + upserts stats (same logic as collect())
   *
   * Runs every 10 minutes. Orphan markets are typically inactive so sub-minute freshness
   * is not required.
   */
  private async syncStatsForOrphanDBMarkets(): Promise<void> {
    if (this._syncingOrphanStats || !this._running) return;
    this._syncingOrphanStats = true;

    try {
      const dbMarkets = await getMarkets();
      if (!dbMarkets || dbMarkets.length === 0) return;

      const liveSlabs = new Set(this.marketProvider.getMarkets().keys());
      const orphans = dbMarkets.filter(
        (m) => !liveSlabs.has(m.slab_address) && !m.indexer_excluded,
      );

      if (orphans.length === 0) return;

      logger.info("Syncing stats for orphan DB markets", { count: orphans.length });

      const connection = getConnection();
      let updated = 0;
      let errors = 0;

      // Process in batches of 5 (same as collect()) to avoid RPC rate limits
      for (let i = 0; i < orphans.length; i += 5) {
        const batch = orphans.slice(i, i + 5);
        const slabPubkeys = batch.map((m) => new PublicKey(m.slab_address));

        try {
          const accountInfos = await withRetry(
            () => connection.getMultipleAccountsInfo(slabPubkeys),
            { maxRetries: 3, baseDelayMs: 1000, label: `orphan-stats-batch-${i / 5 + 1}` },
          );

          await Promise.all(batch.map(async (dbMarket, batchIdx) => {
            try {
              const accountInfo = accountInfos[batchIdx];
              if (!accountInfo?.data) {
                // Account doesn't exist on-chain — mark as closed + excluded
                const db = getSupabase();
                await db.from("markets").update({ status: "closed", indexer_excluded: true }).eq("slab_address", dbMarket.slab_address).eq("network", getNetwork());
                logger.info("Auto-closed non-existent orphan market", { slab: dbMarket.slab_address.slice(0, 8) });
                return;
              }

              const data = new Uint8Array(accountInfo.data);

              let engine: ReturnType<typeof parseEngine>;
              let marketConfig: ReturnType<typeof parseConfig>;
              let params: ReturnType<typeof parseParams>;
              try {
                engine = parseEngine(data);
                marketConfig = parseConfig(data);
                params = parseParams(data);
              } catch {
                // Slab too small or unrecognized layout — skip
                return;
              }


              // Oracle-mode-aware price resolution (mirrors collect())
              const zeroKeyBytes = new Uint8Array(32);
              const isHyperpMode = marketConfig.indexFeedId.equals(new PublicKey(zeroKeyBytes));
              const isPythPinned = !isHyperpMode && marketConfig.oracleAuthority.equals(new PublicKey(zeroKeyBytes));
              let priceE6: bigint;
              if (isPythPinned || isHyperpMode) {
                priceE6 = marketConfig.lastEffectivePriceE6;
              } else {
                priceE6 = marketConfig.authorityPriceE6 > 0n
                  ? marketConfig.authorityPriceE6
                  : marketConfig.lastEffectivePriceE6;
              }
              const priceUsd = priceE6 > 0n ? Number(priceE6) / 1_000_000 : null;

              // Dust vault guard (mirrors collect())
              const MIN_VAULT_FOR_OI = 1_000_000n;
              const hasDustVault = engine.vault <= MIN_VAULT_FOR_OI;
              const hasNoAccounts = engine.numUsedAccounts === 0;

              let oiLong = 0n;
              let oiShort = 0n;
              if (!hasDustVault && !hasNoAccounts) {
                try {
                  const accounts = parseAllAccounts(data);
                  for (const { account } of accounts) {
                    if (account.positionSize > 0n) oiLong += account.positionSize;
                    else if (account.positionSize < 0n) oiShort += -account.positionSize;
                  }
                } catch {
                  oiLong = engine.totalOpenInterest > 0n ? engine.totalOpenInterest / 2n : 0n;
                  oiShort = oiLong;
                }
              }

              const U64_MAX = 18446744073709551615n;
              const PG_BIGINT_MAX = 9223372036854775807n;
              const safeBigNum = (v: bigint): any => {
                if (v >= U64_MAX || v < 0n) return 0;
                if (v > BigInt(Number.MAX_SAFE_INTEGER)) return v.toString();
                return Number(v);
              };
              const safePgBigint = (v: bigint): any => {
                if (v >= U64_MAX || v < 0n) return 0;
                if (v > PG_BIGINT_MAX) return Number(PG_BIGINT_MAX);
                if (v > BigInt(Number.MAX_SAFE_INTEGER)) return v.toString();
                return Number(v);
              };

              // Sanity check (mirrors collect())
              // MAX_SANE_VALUE guards instantaneous balances (OI, insurance, vault) against
              // garbage values produced by wrong slab-tier layout detection (~9.8e34).
              // MAX_SANE_CUMULATIVE guards cTot separately: it's a running lifetime total
              // so it grows without bound on active markets (seen at 1.99e13 after 52h) and
              // must not be capped at the same threshold as point-in-time balances. (GH#1789)
              // GH#1799: raised from 1e18 → 2e18 — slab 3ZKKwsK has legitimate cTot=1.1e18.
              // u64::MAX garbage is ~1.84e19, so 2e18 is still 9x below the noise floor.
              const MAX_SANE_VALUE = 1e13;
              const MAX_SANE_CUMULATIVE = 2_000_000_000_000_000_000n; // bigint literal — avoids Number precision loss above MAX_SAFE_INTEGER (GH#31)
              const MAX_SANE_COUNTER = 1e12;
              // GH#33: dust-vault / empty markets (cTot=0, vault=1M) have their OI zeroed
              // by the guard above — the raw engine.totalOpenInterest is not written to DB
              // and may contain garbage from wrong-tier parsing. Skip the OI sanity component
              // for these markets so stats still get written (insurance, vault, price etc).
              const isSaneEngine = (
                (hasDustVault || hasNoAccounts || safeBigNum(engine.totalOpenInterest) < MAX_SANE_VALUE) &&
                safeBigNum(engine.insuranceFund.balance) < MAX_SANE_VALUE &&
                engine.cTot < MAX_SANE_CUMULATIVE &&
                safeBigNum(engine.vault) < MAX_SANE_VALUE &&
                safeBigNum(engine.lifetimeLiquidations) < MAX_SANE_COUNTER &&
                safeBigNum(engine.lifetimeForceCloses) < MAX_SANE_COUNTER
              );
              if (!isSaneEngine) {
                logger.warn("Orphan market: insane engine values, skipping", {
                  slabAddress: dbMarket.slab_address.slice(0, 8),
                });
                return;
              }

              await upsertMarketStats({
                slab_address: dbMarket.slab_address,
                last_price: priceUsd,
                mark_price: priceUsd,
                index_price: priceUsd,
                open_interest_long: safeBigNum(oiLong),
                open_interest_short: safeBigNum(oiShort),
                insurance_fund: safeBigNum(engine.insuranceFund.balance),
                total_accounts: engine.numUsedAccounts,
                funding_rate: Number(engine.fundingRateBpsPerSlotLast),
                total_open_interest: safeBigNum(oiLong + oiShort),
                net_lp_pos: engine.netLpPos.toString(),
                lp_sum_abs: safeBigNum(engine.lpSumAbs),
                lp_max_abs: safeBigNum(engine.lpMaxAbs),
                insurance_balance: safeBigNum(engine.insuranceFund.balance),
                insurance_fee_revenue: safeBigNum(engine.insuranceFund.feeRevenue),
                warmup_period_slots: safePgBigint(params.warmupPeriodSlots),
                vault_balance: safeBigNum(engine.vault),
                lifetime_liquidations: safeBigNum(engine.lifetimeLiquidations),
                lifetime_force_closes: safeBigNum(engine.lifetimeForceCloses),
                c_tot: safeBigNum(engine.cTot),
                pnl_pos_tot: safeBigNum(engine.pnlPosTot),
                last_crank_slot: safePgBigint(engine.lastCrankSlot),
                max_crank_staleness_slots: safePgBigint(engine.maxCrankStalenessSlots),
                maintenance_fee_per_slot: params.maintenanceFeePerSlot.toString(),
                liquidation_fee_bps: safePgBigint(params.liquidationFeeBps),
                liquidation_fee_cap: params.liquidationFeeCap.toString(),
                liquidation_buffer_bps: safePgBigint(params.liquidationBufferBps),
                updated_at: new Date().toISOString(),
              });

              // Auto-close orphan markets with dust vault + no accounts
              if (hasDustVault && hasNoAccounts && !this.closedSlabs.has(dbMarket.slab_address)) {
                try {
                  await getSupabase()
                    .from("markets")
                    .update({ status: "closed", indexer_excluded: true })
                    .eq("slab_address", dbMarket.slab_address)
                    .eq("network", getNetwork())
                    .neq("status", "closed");
                  this.closedSlabs.add(dbMarket.slab_address);
                  logger.info("Auto-closed orphan market", { slabAddress: dbMarket.slab_address.slice(0, 8) });
                } catch (e) {
                  // Non-fatal
                }
              }

              updated++;
            } catch (err) {
              errors++;
              logger.warn("Orphan stats upsert failed", {
                slabAddress: dbMarket.slab_address.slice(0, 8),
                error: err instanceof Error ? err.message : err,
              });
            }
          }));
        } catch (batchErr) {
          errors += batch.length;
          logger.warn("Orphan stats batch fetch failed", {
            error: batchErr instanceof Error ? batchErr.message : batchErr,
          });
        }

        // Small delay between batches to avoid RPC rate limits
        if (i + 5 < orphans.length) {
          await new Promise((r) => setTimeout(r, 1_000));
        }
      }

      if (updated > 0 || errors > 0) {
        logger.info("Orphan stats sync complete", { updated, errors, totalOrphans: orphans.length });
      }
    } catch (err) {
      logger.warn("syncStatsForOrphanDBMarkets failed", { error: err instanceof Error ? err.message : err });
    } finally {
      this._syncingOrphanStats = false;
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
          const mintAddress = market.config.collateralMint.toBase58();
          const admin = market.header.admin.toBase58();
          const oracleAuthority = market.config.oracleAuthority.toBase58();
          const priceE6 = Number(market.config.authorityPriceE6);
          const initialMarginBps = Number(market.params.initialMarginBps);

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
          const isHyperpMarket = market.config.indexFeedId.equals(new PublicKey(zeroKeyBytesHyperp));
          if (isHyperpMarket) {
            let baseSymbol = "SOL";  // safe default: SOL/USDC is the only hyperp type today
            let baseName = "Solana"; // safe default
            let resolvedFromChain = false;
            try {
              const dexPool = market.config.dexPool;
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
              dexPool: market.config.dexPool?.toBase58() ?? null,
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
            try {
              await getSupabase()
                .from("markets")
                .update({ indexer_excluded: false, status: "active" })
                .eq("slab_address", m.slab_address)
                .eq("network", getNetwork());
              this.closedSlabs.delete(m.slab_address);
              logger.info("Re-enabled previously excluded market (back in discovery)", {
                slabAddress: m.slab_address.slice(0, 8),
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

            // Parse engine state and risk params
            let engine: EngineState;
            let marketConfig: MarketConfig;
            let params: RiskParams;
            try {
              engine = parseEngine(data);
              marketConfig = parseConfig(data);
              params = parseParams(data);
            } catch (parseErr) {
              // Slab too small or invalid — skip
              return;
            }

            // Calculate open interest (separate long/short)
            let oiLong = 0n;
            let oiShort = 0n;
            try {
              const accounts = parseAllAccounts(data);
              for (const { account } of accounts) {
                if (account.positionSize > 0n) {
                  oiLong += account.positionSize;
                } else if (account.positionSize < 0n) {
                  oiShort += -account.positionSize;
                }
              }
            } catch {
              // If account parsing fails, use engine aggregate
              oiLong = engine.totalOpenInterest > 0n ? engine.totalOpenInterest / 2n : 0n;
              oiShort = oiLong;
            }

            // PERC-816/817: Dust vault guard — enforce the invariant that OI must be 0
            // when the vault has no meaningful liquidity. This prevents phantom OI from
            // being re-written by the indexer for markets that were created but never
            // received real LP deposits (e.g. creation-deposit splits leave dust in vault).
            // Threshold: 1,000,000 micro-units (≤ 1 USDC at 6 decimals, dust at 9 decimals).
            // Uses inclusive (<=) to catch vault == 1_000_000 — the exact creation-deposit
            // seed amount the program writes at market creation. A market with vault at
            // the creation boundary has received no real LP deposits.
            // Also enforces: total_accounts = 0 → OI must be 0 (no open positions).
            const MIN_VAULT_FOR_OI = 1_000_000n;
            const hasDustVault = engine.vault <= MIN_VAULT_FOR_OI;
            const hasNoAccounts = engine.numUsedAccounts === 0;
            if (hasDustVault || hasNoAccounts) {
              oiLong = 0n;
              oiShort = 0n;
            }

            // Use on-chain price with oracle-mode-aware resolution
            // Oracle modes:
            //   - pyth-pinned: oracleAuthority == [0;32] → use lastEffectivePriceE6
            //   - hyperp: indexFeedId == [0;32] → use lastEffectivePriceE6 (index price)
            //   - admin: both non-zero → use authorityPriceE6 (authority-pushed price)
            const zeroKeyBytes = new Uint8Array(32);
            const isHyperpMode = marketConfig.indexFeedId.equals(new PublicKey(zeroKeyBytes));
            const isPythPinned = !isHyperpMode && marketConfig.oracleAuthority.equals(new PublicKey(zeroKeyBytes));
            let priceE6: bigint;
            if (isPythPinned || isHyperpMode) {
              // Pyth-pinned and hyperp: use lastEffectivePriceE6 (on-chain resolved price)
              // For hyperp, authorityPriceE6 is the mark price which can be inflated
              priceE6 = marketConfig.lastEffectivePriceE6;
            } else {
              // Admin oracle: prefer authorityPriceE6, fall back to lastEffectivePriceE6
              priceE6 = marketConfig.authorityPriceE6 > 0n
                ? marketConfig.authorityPriceE6
                : marketConfig.lastEffectivePriceE6;
            }
            const priceUsd = priceE6 > 0n ? Number(priceE6) / 1_000_000 : null;

            // Calculate 24h volume and trade count from trades table
            let volume24h: number | null = null;
            let tradeCount24h: number | null = null;
            try {
              const { volume, tradeCount } = await get24hVolume(slabAddress);
              volume24h = Number(volume);
              tradeCount24h = tradeCount;
            } catch (volErr) {
              // Non-fatal — volume calculation failure shouldn't break stats collection
              logger.warn("24h volume calculation failed", { slabAddress, error: volErr instanceof Error ? volErr.message : volErr });
            }

            // Safe bigint→number conversions for PostgreSQL storage.
            // PG BIGINT is signed 64-bit: max 9223372036854775807 (~9.2e18)
            // PG NUMERIC is arbitrary precision — use string for those columns.
            const U64_MAX = 18446744073709551615n;
            const PG_BIGINT_MAX = 9223372036854775807n;

            /** For NUMERIC columns: convert to number, or string if precision would be lost */
            const safeBigNum = (v: bigint): any => {
              if (v >= U64_MAX || v < 0n) return 0;
              if (v > BigInt(Number.MAX_SAFE_INTEGER)) return v.toString();
              return Number(v);
            };

            /** For BIGINT columns: cap at PG signed bigint max to prevent overflow */
            const safePgBigint = (v: bigint): any => {
              if (v >= U64_MAX || v < 0n) return 0;
              if (v > PG_BIGINT_MAX) return PG_BIGINT_MAX.toString();
              if (v > BigInt(Number.MAX_SAFE_INTEGER)) return v.toString();
              return Number(v);
            };

            // Sanity-check parsed engine values: if the slab layout detection
            // failed (wrong tier), the parser reads garbage from wrong offsets.
            // Telltale sign: values like 9.8e34 OI or 1.8e25 insurance.
            // Max sane value: 1e13 (~$10M USD in micro-USDC). Previously 1e18 which
            // let through corrupt values from wrong slab tier detection (see #491).
            const MAX_SANE_VALUE = 1e13;
            // cTot is a cumulative lifetime-collateral total — it grows without bound on
            // active markets (observed at 1.99e13 after 52h of trading). Use a separate
            // higher threshold so legitimate admin-oracle markets aren't skipped. (GH#1789)
            // GH#1799: raised from 1e18 → 2e18 — slab 3ZKKwsK has legitimate cTot=1.1e18.
            const MAX_SANE_CUMULATIVE = 2_000_000_000_000_000_000n; // bigint literal — avoids Number precision loss above MAX_SAFE_INTEGER (GH#31)
            // Max sane counter value: liquidation/force-close counts shouldn't exceed 1e12
            const MAX_SANE_COUNTER = 1e12;
            // GH#33: dust-vault / empty markets (cTot=0, vault=1M) have their OI zeroed
            // by the guard above — the raw engine.totalOpenInterest is not written to DB
            // and may contain garbage from wrong-tier parsing. Skip the OI sanity component
            // for these markets so stats still get written (insurance, vault, price etc).
            const isSaneEngine = (
              (hasDustVault || hasNoAccounts || safeBigNum(engine.totalOpenInterest) < MAX_SANE_VALUE) &&
              safeBigNum(engine.insuranceFund.balance) < MAX_SANE_VALUE &&
              engine.cTot < MAX_SANE_CUMULATIVE &&
              safeBigNum(engine.vault) < MAX_SANE_VALUE &&
              safeBigNum(engine.lifetimeLiquidations) < MAX_SANE_COUNTER &&
              safeBigNum(engine.lifetimeForceCloses) < MAX_SANE_COUNTER
            );

            if (!isSaneEngine) {
              logger.warn("Insane engine state values detected (likely wrong slab layout), skipping stats update", {
                slabAddress,
                totalOI: safeBigNum(engine.totalOpenInterest),
                insurance: safeBigNum(engine.insuranceFund.balance),
                cTot: safeBigNum(engine.cTot),
                vault: safeBigNum(engine.vault),
                lifetimeLiquidations: safeBigNum(engine.lifetimeLiquidations),
                lifetimeForceCloses: safeBigNum(engine.lifetimeForceCloses),
              });
              return;
            }

            // Upsert market stats with ALL RiskEngine fields (migration 010)
            // Note: NUMERIC columns use safeBigNum(), BIGINT columns use safePgBigint()
            await upsertMarketStats({
              slab_address: slabAddress,
              last_price: priceUsd,
              mark_price: priceUsd, // Same as last_price for now (no funding adjustment)
              index_price: priceUsd,
              open_interest_long: safeBigNum(oiLong),          // NUMERIC
              open_interest_short: safeBigNum(oiShort),        // NUMERIC
              insurance_fund: safeBigNum(engine.insuranceFund.balance), // NUMERIC
              total_accounts: engine.numUsedAccounts,          // INTEGER
              funding_rate: Number(engine.fundingRateBpsPerSlotLast), // NUMERIC
              volume_24h: volume24h,                           // NUMERIC
              trade_count_24h: tradeCount24h,                  // INT4
              // Hidden features (migration 007)
              // GH#1250: Use computed oiLong + oiShort (from parsed accounts) rather than
              // engine.totalOpenInterest. The engine field can be non-zero even when all
              // accounts are closed (OI counter not decremented on force-close / reclaim),
              // producing misleading OI with vault=0 and accounts=0.
              // The computed sum is authoritative: it directly reflects live position sizes.
              total_open_interest: safeBigNum(oiLong + oiShort), // NUMERIC
              net_lp_pos: engine.netLpPos.toString(),          // NUMERIC
              lp_sum_abs: safeBigNum(engine.lpSumAbs),         // NUMERIC
              lp_max_abs: safeBigNum(engine.lpMaxAbs),         // NUMERIC
              insurance_balance: safeBigNum(engine.insuranceFund.balance), // NUMERIC
              insurance_fee_revenue: safeBigNum(engine.insuranceFund.feeRevenue), // NUMERIC
              warmup_period_slots: safePgBigint(params.warmupPeriodSlots), // BIGINT
              // Complete RiskEngine state fields (migration 010)
              vault_balance: safeBigNum(engine.vault),         // NUMERIC
              lifetime_liquidations: safeBigNum(engine.lifetimeLiquidations), // NUMERIC (migration 024)
              lifetime_force_closes: safeBigNum(engine.lifetimeForceCloses),  // NUMERIC (migration 024)
              c_tot: safeBigNum(engine.cTot),                  // NUMERIC
              pnl_pos_tot: safeBigNum(engine.pnlPosTot),       // NUMERIC
              last_crank_slot: safePgBigint(engine.lastCrankSlot), // BIGINT
              max_crank_staleness_slots: safePgBigint(engine.maxCrankStalenessSlots), // BIGINT
              // RiskParams fields (migration 010)
              maintenance_fee_per_slot: params.maintenanceFeePerSlot.toString(), // TEXT
              liquidation_fee_bps: safePgBigint(params.liquidationFeeBps), // BIGINT
              liquidation_fee_cap: params.liquidationFeeCap.toString(),    // TEXT
              liquidation_buffer_bps: safePgBigint(params.liquidationBufferBps), // BIGINT
              updated_at: new Date().toISOString(),
            });

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
                  this.closedSlabs.add(slabAddress);
                  logger.info("Auto-closed stale market", { slabAddress, vault: Number(engine.vault), accounts: engine.numUsedAccounts });
                }
              } catch (e) {
                logger.warn("Auto-close market error", { slabAddress, error: e instanceof Error ? e.message : e });
              }
            }

            // Log oracle price to DB (rate-limited per market)
            if (priceE6 > 0n) {
              const lastLog = this.lastOracleLogTime.get(slabAddress) ?? 0;
              if (Date.now() - lastLog >= ORACLE_LOG_INTERVAL_MS) {
                try {
                  await insertOraclePrice({
                    slab_address: slabAddress,
                    price_e6: priceE6.toString(),
                    timestamp: Math.floor(Date.now() / 1000),
                  });
                  this.lastOracleLogTime.set(slabAddress, Date.now());
                } catch (oracleErr) {
                  // Non-fatal — oracle logging shouldn't break stats collection
                  logger.warn("Oracle price log failed", { slabAddress, error: oracleErr instanceof Error ? oracleErr.message : oracleErr });
                }
              }
            }

            // Log OI history (rate-limited per market)
            // History tables have FK to market_stats(slab_address). If the market
            // hasn't been inserted yet, we get FK violation (23503) — skip gracefully.
            const OI_HISTORY_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
            const lastOiLog = this.lastOiHistoryTime.get(slabAddress) ?? 0;
            if (Date.now() - lastOiLog >= OI_HISTORY_INTERVAL_MS) {
              try {
                const { error: oiErr } = await getSupabase().from('oi_history').insert({
                  market_slab: slabAddress,
                  slot: safePgBigint(engine.lastCrankSlot),        // BIGINT
                  total_oi: safeBigNum(engine.totalOpenInterest),  // NUMERIC
                  net_lp_pos: safeBigNum(engine.netLpPos),         // NUMERIC
                  lp_sum_abs: safeBigNum(engine.lpSumAbs),         // NUMERIC
                  lp_max_abs: safeBigNum(engine.lpMaxAbs),         // NUMERIC
                  network: getNetwork(),                            // PERC-8192: stamp network
                });
                // Ignore FK violations (23503) and unique constraint violations (23505)
                if (oiErr && oiErr.code !== '23503' && oiErr.code !== '23505') {
                  logger.warn("OI history log failed", { slabAddress, error: oiErr.message, code: oiErr.code });
                } else {
                  this.lastOiHistoryTime.set(slabAddress, Date.now());
                }
              } catch (e) {
                // Non-fatal
                logger.warn("OI history log failed", { slabAddress, error: e instanceof Error ? e.message : e });
              }
            }

            // Log insurance history (rate-limited per market)
            const INS_HISTORY_INTERVAL_MS = 5 * 60 * 1000;
            const lastInsLog = this.lastInsHistoryTime.get(slabAddress) ?? 0;
            if (Date.now() - lastInsLog >= INS_HISTORY_INTERVAL_MS) {
              try {
                const { error: insErr } = await getSupabase().from('insurance_history').insert({
                  market_slab: slabAddress,
                  slot: safePgBigint(engine.lastCrankSlot),                // BIGINT
                  balance: safeBigNum(engine.insuranceFund.balance),       // NUMERIC
                  fee_revenue: safeBigNum(engine.insuranceFund.feeRevenue), // NUMERIC
                  network: getNetwork(),                                    // PERC-8192: stamp network
                });
                if (insErr && insErr.code !== '23503' && insErr.code !== '23505') {
                  logger.warn("Insurance history log failed", { slabAddress, error: insErr.message, code: insErr.code });
                } else {
                  this.lastInsHistoryTime.set(slabAddress, Date.now());
                }
              } catch (e) {
                logger.warn("Insurance history log failed", { slabAddress, error: e instanceof Error ? e.message : e });
              }
            }

            // Log funding history (rate-limited per market)
            const FUNDING_HISTORY_INTERVAL_MS = 5 * 60 * 1000;
            const lastFundLog = this.lastFundingHistoryTime.get(slabAddress) ?? 0;
            if (Date.now() - lastFundLog >= FUNDING_HISTORY_INTERVAL_MS) {
              try {
                const { error: fundErr } = await getSupabase().from('funding_history').insert({
                  market_slab: slabAddress,
                  slot: safePgBigint(engine.lastCrankSlot),                    // BIGINT
                  rate_bps_per_slot: Number(engine.fundingRateBpsPerSlotLast), // NUMERIC
                  net_lp_pos: safeBigNum(engine.netLpPos),                     // NUMERIC
                  price_e6: safeBigNum(priceE6),                               // NUMERIC
                  funding_index_qpb_e6: engine.fundingIndexQpbE6.toString(),   // TEXT
                  network: getNetwork(),                                        // PERC-8192: stamp network
                });
                if (fundErr && fundErr.code !== '23503' && fundErr.code !== '23505') {
                  logger.warn("Funding history log failed", { slabAddress, error: fundErr.message, code: fundErr.code });
                } else {
                  this.lastFundingHistoryTime.set(slabAddress, Date.now());
                }
              } catch (e) {
                logger.warn("Funding history log failed", { slabAddress, error: e instanceof Error ? e.message : e });
              }
            }

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
      for (const key of this.lastOracleLogTime.keys()) {
        if (!markets.has(key)) this.lastOracleLogTime.delete(key);
      }
      for (const key of this.lastOiHistoryTime.keys()) {
        if (!markets.has(key)) this.lastOiHistoryTime.delete(key);
      }
      for (const key of this.lastInsHistoryTime.keys()) {
        if (!markets.has(key)) this.lastInsHistoryTime.delete(key);
      }
      for (const key of this.lastFundingHistoryTime.keys()) {
        if (!markets.has(key)) this.lastFundingHistoryTime.delete(key);
      }
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
