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
  type EngineState,
  type MarketConfig,
  type RiskParams,
  type DiscoveredMarket,
} from "@percolator/sdk";
import { 
  getConnection,
  upsertMarketStats, 
  insertOraclePrice, 
  get24hVolume,
  getMarkets,
  insertMarket,
  updateMarketDecimals,
  getSupabase,
  withRetry,
  createLogger,
  captureException,
  addBreadcrumb,
} from "@percolator/shared";
// Local bigint sanitization — prevents PG overflow from u64::MAX sentinels (PERC-206).
// TODO: Switch to @percolator/shared once the shared package is republished.
import { sanitizeBigIntForDb, sanitizeBigIntToString } from "../utils/bigint-sanitize.js";

const logger = createLogger("indexer:stats-collector");

/** Market provider interface — allows different market discovery strategies */
export interface MarketProvider {
  getMarkets(): Map<string, { market: DiscoveredMarket }>;
}

/** How often to collect stats (every 30s — runs after crank cycles) */
const COLLECT_INTERVAL_MS = 120_000;

/** How often to log oracle prices to DB (every 60s per market to avoid bloat) */
const ORACLE_LOG_INTERVAL_MS = 60_000;

/** How many consecutive parse failures before a market is permanently skipped */
const PARSE_FAIL_SKIP_THRESHOLD = 10;

export class StatsCollector {
  private timer: ReturnType<typeof setInterval> | null = null;
  private _running = false;
  private _collecting = false;
  private lastOracleLogTime = new Map<string, number>();
  private lastFundingLogSlot = new Map<string, number>();
  private lastOiHistoryTime = new Map<string, number>();
  private lastInsHistoryTime = new Map<string, number>();
  private lastFundingHistoryTime = new Map<string, number>();

  /** Consecutive parse-failure count per slab — reset on first successful parse */
  private parseFailureCount = new Map<string, number>();
  /** Slabs that have failed PARSE_FAIL_SKIP_THRESHOLD times in a row — skip until rediscovery */
  readonly permanentlySkippedSlabs = new Set<string>();

  constructor(
    private readonly marketProvider: MarketProvider,
  ) {}

  /**
   * Re-enable a slab that was permanently skipped.
   * Called by market rediscovery when a slab appears with a new layout version.
   */
  clearSkippedSlab(slabAddress: string): void {
    this.permanentlySkippedSlabs.delete(slabAddress);
    this.parseFailureCount.delete(slabAddress);
  }

  start(): void {
    if (this._running) return;
    this._running = true;

    // Initial collection after a short delay
    setTimeout(() => this.collect(), 10_000);

    // Periodic collection
    this.timer = setInterval(() => this.collect(), COLLECT_INTERVAL_MS);

    logger.info("StatsCollector started", { intervalMs: COLLECT_INTERVAL_MS });
  }

  stop(): void {
    this._running = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    logger.info("StatsCollector stopped");
  }

  /**
   * Fix decimals on existing markets by reading the on-chain mint account.
   * This self-heals markets that were registered with incorrect fallback decimals.
   */
  private async fixExistingMarketDecimals(
    onChainMarkets: Map<string, { market: DiscoveredMarket }>,
    dbMarkets: Array<{ slab_address: string; mint_address: string; decimals: number }>,
  ): Promise<void> {
    const conn = getConnection();

    // Build a map of mint → on-chain decimals (cache to avoid duplicate RPC calls)
    const mintDecimalsCache = new Map<string, number>();

    for (const dbMarket of dbMarkets) {
      const onChain = onChainMarkets.get(dbMarket.slab_address);
      if (!onChain) continue;

      const mintAddress = dbMarket.mint_address;

      // Look up or fetch on-chain decimals
      let onChainDecimals = mintDecimalsCache.get(mintAddress);
      if (onChainDecimals === undefined) {
        try {
          const mintInfo = await conn.getAccountInfo(onChain.market.config.collateralMint);
          if (mintInfo && mintInfo.data.length >= 45) {
            onChainDecimals = mintInfo.data[44]; // SPL Token Mint: decimals at offset 44
          } else {
            continue; // Can't determine — skip
          }
          mintDecimalsCache.set(mintAddress, onChainDecimals);
        } catch (err) {
          logger.warn("Failed to fetch mint decimals for existing market", {
            slabAddress: dbMarket.slab_address,
            mintAddress,
            error: err instanceof Error ? err.message : err,
          });
          continue;
        }
      }

      // If DB decimals don't match on-chain, fix them
      if (dbMarket.decimals !== onChainDecimals) {
        try {
          await updateMarketDecimals(dbMarket.slab_address, onChainDecimals);
          logger.info("Fixed market decimals", {
            slabAddress: dbMarket.slab_address,
            oldDecimals: dbMarket.decimals,
            newDecimals: onChainDecimals,
            mintAddress,
          });
        } catch (err) {
          logger.warn("Failed to fix market decimals", {
            slabAddress: dbMarket.slab_address,
            error: err instanceof Error ? err.message : err,
          });
        }
      }
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

      // Fix decimals on existing markets whose on-chain mint decimals differ from DB
      await this.fixExistingMarketDecimals(onChainMarkets, dbMarkets);

      if (missingMarkets.length === 0) return;

      logger.info("New markets found", { count: missingMarkets.length });

      // Insert missing markets
      for (const [slabAddress, state] of missingMarkets) {
        try {
          const market = state.market;
          const mintAddress = market.config.collateralMint.toBase58();
          const admin = market.header.admin.toBase58();
          const oracleAuthority = market.config.oracleAuthority.toBase58();
          const priceE6 = sanitizeBigIntForDb(market.config.authorityPriceE6);
          const initialMarginBps = sanitizeBigIntForDb(market.params.initialMarginBps, 100);
          
          // Fetch actual mint decimals from on-chain
          let mintDecimals = 9; // fallback
          try {
            const conn = getConnection();
            const mintInfo = await conn.getAccountInfo(market.config.collateralMint);
            if (mintInfo && mintInfo.data.length >= 45) {
              // SPL Token Mint layout: decimals is a u8 at offset 44
              mintDecimals = mintInfo.data[44];
            }
          } catch (err) {
            logger.warn("Failed to fetch mint decimals, using default 9", { mintAddress, error: err instanceof Error ? err.message : err });
          }

          // Derive fields as specified
          const symbol = mintAddress.substring(0, 8);
          const name = `Market ${slabAddress.substring(0, 8)}`;
          // Guard: initialMarginBps of 0 produces Infinity (10000/0) which JSON-serialises
          // to null and violates the NOT NULL constraint on max_leverage. Fall back to 100
          // (= 100x leverage, equivalent to 1% initial margin) for uninitialised slabs.
          const safeMarginBps = initialMarginBps > 0 ? initialMarginBps : 100;
          const maxLeverage = Math.floor(10000 / safeMarginBps);
          
          await insertMarket({
            slab_address: slabAddress,
            mint_address: mintAddress,
            symbol,
            name,
            decimals: mintDecimals,
            deployer: admin,
            oracle_authority: oracleAuthority,
            initial_price_e6: priceE6,
            max_leverage: maxLeverage,
            trading_fee_bps: 10,
            lp_collateral: null,
            matcher_context: null,
            status: "active",
          });

          logger.info("Market registered", { slabAddress, symbol });
        } catch (err) {
          logger.warn("Failed to register market", { slabAddress, error: err instanceof Error ? err.message : err });
        }
      }
    } catch (err) {
      logger.error("Market sync failed", { error: err });
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
      if (markets.size === 0) return;

      const connection = getConnection();
      let updated = 0;
      let errors = 0;

      // Process markets in batches of 5 to avoid RPC rate limits
      // Use getMultipleAccountsInfo for batch fetching to reduce RPC round trips
      let parseErrors = 0;

      const entries = Array.from(markets.entries());
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
              // Skip slabs that have consistently failed parsing — re-enabled on rediscovery
              if (this.permanentlySkippedSlabs.has(slabAddress)) return;

              const accountInfo = accountInfos[batchIndex];
              if (!accountInfo?.data) return;

            const data = new Uint8Array(accountInfo.data);

            // Parse engine state and risk params
            let engine: EngineState;
            let marketConfig: MarketConfig;
            let params: RiskParams;
            try {
              engine = parseEngine(data);
              marketConfig = parseConfig(data);
              params = parseParams(data);
              // Successful parse — reset consecutive failure counter
              this.parseFailureCount.delete(slabAddress);
            } catch (parseErr) {
              // Slab too small or invalid layout — track separately from DB errors
              parseErrors++;
              const failCount = (this.parseFailureCount.get(slabAddress) ?? 0) + 1;
              this.parseFailureCount.set(slabAddress, failCount);

              if (failCount >= PARSE_FAIL_SKIP_THRESHOLD) {
                this.permanentlySkippedSlabs.add(slabAddress);
                logger.warn("Market permanently skipped after repeated parse failures", {
                  slabAddress,
                  failCount,
                  error: parseErr instanceof Error ? parseErr.message : parseErr,
                });
              } else {
                logger.debug("Market parse failed — will retry next cycle", {
                  slabAddress,
                  failCount,
                  threshold: PARSE_FAIL_SKIP_THRESHOLD,
                  error: parseErr instanceof Error ? parseErr.message : parseErr,
                });
              }
              return;
            }

            // Calculate open interest (separate long/short)
            // Note: positionSize is i128 on-chain; only aggregate into oiLong/oiShort
            // which are then sanitized before DB insertion below.
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
              // If account parsing fails, use engine aggregate (also sanitized)
              const rawOi = engine.totalOpenInterest;
              oiLong = rawOi > 0n ? rawOi / 2n : 0n;
              oiShort = oiLong;
            }

            // Use on-chain price: prefer authorityPriceE6, fall back to lastEffectivePriceE6 (for Pyth-pinned markets)
            // Sanitize: sentinel values (u64::MAX) should not produce a price
            const rawPriceE6 = marketConfig.authorityPriceE6 > 0n
              ? marketConfig.authorityPriceE6
              : marketConfig.lastEffectivePriceE6;
            const safePriceE6 = sanitizeBigIntForDb(rawPriceE6);
            const priceE6 = rawPriceE6; // keep bigint for oracle log comparison
            const priceUsd = safePriceE6 > 0 ? safePriceE6 / 1_000_000 : null;

            // Calculate 24h volume and trade count from trades table
            let volume24h: number | null = null;
            let tradeCount24h: number = 0;
            try {
              const { volume, tradeCount } = await get24hVolume(slabAddress);
              volume24h = Number(volume);
              tradeCount24h = tradeCount;
            } catch (volErr) {
              // Non-fatal — volume calculation failure shouldn't break stats collection
              logger.warn("24h volume calculation failed", { slabAddress, error: volErr instanceof Error ? volErr.message : volErr });
            }

            // Upsert market stats with ALL RiskEngine fields (migration 010)
            // All bigint → Number conversions go through sanitizeBigIntForDb()
            // to replace u64::MAX sentinels and prevent PG bigint overflow.
            await upsertMarketStats({
              slab_address: slabAddress,
              last_price: priceUsd,
              mark_price: priceUsd, // Same as last_price for now (no funding adjustment)
              index_price: priceUsd,
              open_interest_long: sanitizeBigIntForDb(oiLong),
              open_interest_short: sanitizeBigIntForDb(oiShort),
              insurance_fund: sanitizeBigIntForDb(engine.insuranceFund.balance),
              total_accounts: engine.numUsedAccounts,
              funding_rate: sanitizeBigIntForDb(engine.fundingRateBpsPerSlotLast),
              volume_24h: volume24h,
              trade_count_24h: tradeCount24h,
              // Hidden features (migration 007)
              total_open_interest: sanitizeBigIntForDb(engine.totalOpenInterest),
              net_lp_pos: sanitizeBigIntToString(engine.netLpPos),
              lp_sum_abs: sanitizeBigIntForDb(engine.lpSumAbs),
              lp_max_abs: sanitizeBigIntForDb(engine.lpMaxAbs),
              insurance_balance: sanitizeBigIntForDb(engine.insuranceFund.balance),
              insurance_fee_revenue: sanitizeBigIntForDb(engine.insuranceFund.feeRevenue),
              warmup_period_slots: sanitizeBigIntForDb(params.warmupPeriodSlots),
              // Complete RiskEngine state fields (migration 010)
              vault_balance: sanitizeBigIntForDb(engine.vault),
              lifetime_liquidations: sanitizeBigIntForDb(engine.lifetimeLiquidations),
              lifetime_force_closes: sanitizeBigIntForDb(engine.lifetimeForceCloses),
              c_tot: sanitizeBigIntForDb(engine.cTot),
              pnl_pos_tot: sanitizeBigIntForDb(engine.pnlPosTot),
              last_crank_slot: sanitizeBigIntForDb(engine.lastCrankSlot),
              max_crank_staleness_slots: sanitizeBigIntForDb(engine.maxCrankStalenessSlots),
              // RiskParams fields (migration 010)
              maintenance_fee_per_slot: sanitizeBigIntToString(params.maintenanceFeePerSlot),
              liquidation_fee_bps: sanitizeBigIntForDb(params.liquidationFeeBps),
              liquidation_fee_cap: sanitizeBigIntToString(params.liquidationFeeCap),
              liquidation_buffer_bps: sanitizeBigIntForDb(params.liquidationBufferBps),
              updated_at: new Date().toISOString(),
            });

            // Log oracle price to DB (rate-limited per market)
            if (safePriceE6 > 0) {
              const lastLog = this.lastOracleLogTime.get(slabAddress) ?? 0;
              if (Date.now() - lastLog >= ORACLE_LOG_INTERVAL_MS) {
                try {
                  await insertOraclePrice({
                    slab_address: slabAddress,
                    price_e6: sanitizeBigIntToString(rawPriceE6),
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
            const OI_HISTORY_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
            const lastOiLog = this.lastOiHistoryTime.get(slabAddress) ?? 0;
            if (Date.now() - lastOiLog >= OI_HISTORY_INTERVAL_MS) {
              try {
                await getSupabase().from('oi_history').insert({
                  market_slab: slabAddress,
                  slot: sanitizeBigIntForDb(engine.lastCrankSlot),
                  total_oi: sanitizeBigIntForDb(engine.totalOpenInterest),
                  net_lp_pos: sanitizeBigIntForDb(engine.netLpPos),
                  lp_sum_abs: sanitizeBigIntForDb(engine.lpSumAbs),
                  lp_max_abs: sanitizeBigIntForDb(engine.lpMaxAbs),
                });
                this.lastOiHistoryTime.set(slabAddress, Date.now());
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
                await getSupabase().from('insurance_history').insert({
                  market_slab: slabAddress,
                  slot: sanitizeBigIntForDb(engine.lastCrankSlot),
                  balance: sanitizeBigIntForDb(engine.insuranceFund.balance),
                  fee_revenue: sanitizeBigIntForDb(engine.insuranceFund.feeRevenue),
                });
                this.lastInsHistoryTime.set(slabAddress, Date.now());
              } catch (e) {
                logger.warn("Insurance history log failed", { slabAddress, error: e instanceof Error ? e.message : e });
              }
            }

            // Log funding history (rate-limited per market)
            const FUNDING_HISTORY_INTERVAL_MS = 5 * 60 * 1000;
            const lastFundLog = this.lastFundingHistoryTime.get(slabAddress) ?? 0;
            if (Date.now() - lastFundLog >= FUNDING_HISTORY_INTERVAL_MS) {
              try {
                await getSupabase().from('funding_history').insert({
                  market_slab: slabAddress,
                  slot: sanitizeBigIntForDb(engine.lastCrankSlot),
                  rate_bps_per_slot: sanitizeBigIntForDb(engine.fundingRateBpsPerSlotLast),
                  net_lp_pos: sanitizeBigIntForDb(engine.netLpPos),
                  price_e6: sanitizeBigIntForDb(priceE6),
                  funding_index_qpb_e6: sanitizeBigIntToString(engine.fundingIndexQpbE6),
                });
                this.lastFundingHistoryTime.set(slabAddress, Date.now());
              } catch (e) {
                console.warn(`[StatsCollector] Funding history log failed for ${slabAddress}:`, e instanceof Error ? e.message : e);
              }
            }

              updated++;
            } catch (err) {
              errors++;
              console.warn(`[StatsCollector] Failed for ${slabAddress}:`, err instanceof Error ? err.message : err);
            }
          }));
        } catch (batchErr) {
          // If batch fetch fails, log all markets in batch as errors
          errors += batch.length;
          console.error(`[StatsCollector] Batch fetch failed:`, batchErr instanceof Error ? batchErr.message : batchErr);
        }

        // Small delay between batches
        if (i + 5 < entries.length) {
          await new Promise((r) => setTimeout(r, 1_000));
        }
      }

      const skipped = this.permanentlySkippedSlabs.size;
      const logParts = [
        `Updated ${updated}/${markets.size} markets`,
        `${errors} DB errors`,
        `${parseErrors} parse failures`,
        ...(skipped > 0 ? [`${skipped} permanently skipped`] : []),
      ];
      console.log(`[StatsCollector] ${logParts.join(", ")}`);
      if (errors > 0 || parseErrors > 0) {
        addBreadcrumb("StatsCollector completed with errors", {
          updated,
          errors,
          parseErrors,
          permanentlySkipped: skipped,
          totalMarkets: markets.size,
        });
      }
    } catch (err) {
      console.error("[StatsCollector] Collection failed:", err);
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
