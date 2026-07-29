/**
 * StatsCollector — history-only: registers markets and rolls up trade volume.
 *
 * Runs after each crank cycle to read on-chain slab data and:
 * - Auto-register newly discovered markets (insertMarketRow)
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
import { fetchDasTokenMetadata, placeholderIdentity } from "./tokenMetadata.js";
import { insertMarketRow, updateAutoMarketMetadata } from "../db/insertMarketRow.js";
import { isBlockedSlab } from "../blocklist.js";
import { resolveIdentitiesByCa, chunkForDexScreener, type DexScreenerIdentity } from "./dexscreener.js";
import {
  getConnection,
  getMarkets,
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
/** A row of the `markets` table, as returned by the shared getMarkets() helper. */
type DbMarketRow = Awaited<ReturnType<typeof getMarkets>>[number];

const VOLUME_SYNC_INTERVAL_MS = 10 * 60_000;

/**
 * Markets whose identity is re-resolved per refresh cycle. Bounded so a large
 * unresolved backlog cannot turn one cycle into a long DexScreener hammering;
 * the remainder is picked up next cycle.
 */
const METADATA_REFRESH_LIMIT = 60;

const logger = createLogger("indexer:stats-collector");

/** Market provider interface — allows different market discovery strategies */
export interface MarketProvider {
  getMarkets(): Map<string, { market: DiscoveredMarket }>;
}

/**
 * How often the collect cycle runs. Configurable via `STATS_COLLECT_INTERVAL_MS` (ms).
 *
 * Default: 60_000 (1 min). This cadence is now driven by market REGISTRATION, not
 * stats: trades carry an FK to markets(slab_address), so a market that isn't
 * registered yet can't have its fills indexed. Keeping this at 1 min bounds that
 * window. The expensive part of the cycle (the per-slab RPC sweep) is separately
 * gated by AUTOCLOSE_INTERVAL_MS below.
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

/**
 * How often the per-slab on-chain sweep inside collect() runs (ms).
 *
 * Since the REDUCTION (2026-07-26) that sweep no longer writes any stats — the
 * frontend reads price/OI/insurance live from chain. Its only remaining job is
 * janitorial: auto-close abandoned slabs (dust vault + no accounts) and re-enable
 * markets that came back. That does not need to run every minute, and running it
 * every minute meant a getMultipleAccountsInfo round-trip per batch of markets
 * plus an inter-batch sleep, every 60s, forever.
 *
 * Default: 10 min. Registration (syncMarkets) still runs every COLLECT_INTERVAL_MS.
 */
export const AUTOCLOSE_INTERVAL_MS: number = (() => {
  const raw = process.env.STATS_AUTOCLOSE_INTERVAL_MS;
  if (!raw) return 10 * 60_000;
  const n = Number(raw);
  if (Number.isFinite(n) && n > 0) return n;
  logger.warn("Invalid STATS_AUTOCLOSE_INTERVAL_MS env var, falling back to 600000ms", { raw });
  return 10 * 60_000;
})();

/**
 * Accounts per getMultipleAccountsInfo call in the sweep. The RPC accepts up to
 * 100; 25 keeps each response modest while cutting round-trips 5x versus the
 * previous batch of 5.
 */
const SWEEP_BATCH_SIZE = 25;

/** Pause between sweep batches, to stay clear of RPC rate limits. */
const SWEEP_BATCH_DELAY_MS = 250;

export class StatsCollector {
  private timer: ReturnType<typeof setInterval> | null = null;
  private volumeTimer: ReturnType<typeof setInterval> | null = null;
  private volumeInitTimeout: ReturnType<typeof setTimeout> | null = null;
  private _running = false;
  private _collecting = false;
  private _syncingVolume = false;
  private _refreshingMetadata = false;
  /**
   * When the on-chain janitorial sweep last ran. 0 means "never", so the first
   * collect cycle after start() always sweeps.
   */
  private _lastSweepAt = 0;
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
    this.volumeTimer = setInterval(() => {
      void this.syncVolumeForAllDBMarkets();
      void this.refreshMarketMetadata();
    }, VOLUME_SYNC_INTERVAL_MS);

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
   * Fill in identity for markets the on-chain path could not name.
   *
   * A playground market tracks a MAINNET token (markets.mainnet_ca) while this
   * service talks to a devnet RPC, so DAS cannot see the asset — which is why
   * these rows end up as UNKNOWN placeholders with no logo. DexScreener is
   * chain-wide and keyed by contract address, so it can resolve them.
   *
   * Only touches rows that are still 'auto' AND still missing a logo, so it
   * converges: once a market resolves it stops being a candidate, and a row a
   * human has edited is never a candidate at all (the UPDATE is additionally
   * fenced on metadata_source='auto', so a row flipped to 'manual' mid-pass
   * matches nothing).
   */
  private async refreshMarketMetadata(): Promise<void> {
    if (this._refreshingMetadata || !this._running) return;
    this._refreshingMetadata = true;

    try {
      const { data, error } = await getSupabase()
        .from("markets")
        .select("slab_address, mainnet_ca")
        .eq("network", getNetwork())
        .eq("metadata_source", "auto")
        .is("logo_url", null)
        .not("mainnet_ca", "is", null)
        .limit(METADATA_REFRESH_LIMIT);

      if (error) {
        logger.warn("refreshMarketMetadata: candidate query failed", { error: error.message });
        return;
      }

      const candidates = (data ?? []) as Array<{ slab_address: string; mainnet_ca: string }>;
      if (candidates.length === 0) return;

      // One CA can back several slabs (the same token relaunched as a new
      // market), so resolve the DISTINCT set and fan the result back out.
      const bySlab = new Map<string, string>();
      for (const c of candidates) bySlab.set(c.slab_address, c.mainnet_ca);
      const uniqueCas = Array.from(new Set(bySlab.values()));

      const identities = new Map<string, DexScreenerIdentity>();
      for (const chunk of chunkForDexScreener(uniqueCas)) {
        const resolved = await resolveIdentitiesByCa(chunk);
        for (const [ca, identity] of resolved) identities.set(ca, identity);
      }
      if (identities.size === 0) {
        logger.info("refreshMarketMetadata: nothing resolved", { candidates: candidates.length });
        return;
      }

      let updated = 0;
      for (const [slabAddress, ca] of bySlab) {
        const identity = identities.get(ca);
        if (!identity) continue;

        // Merge only what resolved — a token with a symbol but no image should
        // not blank the name, and a row must not be marked done without a logo
        // (logo_url IS NULL is the candidate predicate, so writing null here
        // would leave it looping forever).
        const patch: { symbol?: string; name?: string; logo_url?: string } = {};
        if (identity.symbol) patch.symbol = identity.symbol;
        if (identity.name) patch.name = identity.name;
        if (identity.logoUrl) patch.logo_url = identity.logoUrl;
        if (Object.keys(patch).length === 0) continue;

        if (await updateAutoMarketMetadata(slabAddress, patch)) {
          updated++;
          logger.info("Market metadata resolved from CA", {
            slabAddress: slabAddress.slice(0, 8),
            ca: ca.slice(0, 8),
            symbol: patch.symbol,
            hasLogo: patch.logo_url != null,
          });
        }
      }

      logger.info("refreshMarketMetadata complete", {
        candidates: candidates.length,
        resolved: identities.size,
        updated,
      });
    } catch (err) {
      logger.warn("refreshMarketMetadata failed", { error: err instanceof Error ? err.message : err });
    } finally {
      this._refreshingMetadata = false;
    }
  }

  /**
   * Sync volume_24h and trade_count_24h for ALL markets in the DB.
   *
   * StatsCollector.collect() only processes markets discovered on-chain. Markets
   * that are deployed but no longer actively cranked (e.g. test markets, stale slabs)
   * fall out of the on-chain provider map and never get their volume updated.
   *
   * The aggregation runs server-side via the volume_24h_by_slab(network) SQL
   * function (migration 20260726180000) and the result is written back in ONE
   * bulk upsert. It intentionally does NOT reset volume to 0 for markets with no
   * trades — those are left unchanged (they'll naturally reach 0 as their last
   * trades age out).
   *
   * PERF: this previously paginated the whole 24h trade tape out of PostgREST
   * (up to 20 round-trips x 5k rows = 100k rows), summed it in JS, then issued
   * one sequential upsert per market. Both the transfer and the N round-trips
   * are gone — it is now 1 RPC + 1 upsert regardless of trade volume, and the
   * 100k-row MAX_PAGES cap that could silently under-report volume is gone too.
   *
   * Bug fixed: GH#1171 — volume_24h = 0 for all markets despite trades existing.
   * Bug fixed: is_liquidation markers (NULL size) inflated trade_count_24h — the
   * SQL function filters them (see the migration header for why the old JS path
   * counted them as zero-volume trades instead of skipping them).
   */
  private async syncVolumeForAllDBMarkets(): Promise<void> {
    if (this._syncingVolume || !this._running) return;
    this._syncingVolume = true;

    try {
      // Server-side rollup: one row per market with trades in the window.
      // Filters is_liquidation markers and NULL sizes (see migration header).
      const { data: rows, error } = await getSupabase().rpc("volume_24h_by_slab", {
        p_network: getNetwork(),
      });

      if (error) {
        logger.warn("syncVolumeForAllDBMarkets: rollup RPC failed", { error: error.message });
        return;
      }

      const agg = (rows ?? []) as Array<{
        slab_address: string;
        volume_24h: string | number;
        trade_count_24h: string | number;
      }>;
      if (agg.length === 0) return;

      // volume_24h is NUMERIC in Postgres (arbitrary precision) but MarketStatsRow
      // types it as number. postgres-js/PostgREST hands NUMERIC back as a string to
      // avoid lossy parsing, so keep the string when it exceeds MAX_SAFE_INTEGER and
      // let Postgres store it exactly; only downcast when it's safely representable.
      const payload = agg.map((r) => {
        const raw = String(r.volume_24h ?? "0");
        const asNum = Number(raw);
        const safe = Number.isFinite(asNum) && Math.abs(asNum) <= Number.MAX_SAFE_INTEGER;
        if (!safe) {
          logger.warn("syncVolumeForAllDBMarkets: volume exceeds MAX_SAFE_INTEGER, storing as string", {
            slabAddress: r.slab_address.slice(0, 8),
            volume: raw,
          });
        }
        return {
          slab_address: r.slab_address,
          volume_24h: (safe ? asNum : raw) as unknown as number,
          trade_count_24h: Number(r.trade_count_24h ?? 0),
          network: getNetwork(),
          updated_at: new Date().toISOString(),
        };
      });

      // Single bulk upsert. Columns absent from the payload (last_price,
      // volume_24h_usd) are left untouched on conflict.
      const { error: upsertErr } = await getSupabase()
        .from("market_stats")
        .upsert(payload as never, { onConflict: "slab_address" });

      if (upsertErr) {
        // A row whose slab_address isn't in `markets` yet violates the FK — that
        // market simply hasn't been registered by syncMarkets() yet, so it will
        // land on the next cycle. Log rather than retry-loop.
        logger.warn("syncVolumeForAllDBMarkets: bulk upsert failed", {
          error: upsertErr.message,
          markets: payload.length,
        });
        return;
      }

      logger.info("Volume sync complete", { marketsUpdated: payload.length });
    } catch (err) {
      logger.warn("syncVolumeForAllDBMarkets failed", { error: err instanceof Error ? err.message : err });
    } finally {
      this._syncingVolume = false;
    }
  }

  /**
   * Auto-register missing markets: compare on-chain markets vs DB and insert any missing.
   *
   * Returns the markets rows it read, so collect() can reuse them for the
   * indexer_excluded pass instead of issuing a second full-table read in the
   * same cycle. Returns null when no read happened (or it failed), in which
   * case the caller falls back to fetching them itself.
   */
  private async syncMarkets(): Promise<DbMarketRow[] | null> {
    try {
      // Get on-chain markets from market provider
      const onChainMarkets = this.marketProvider.getMarkets();
      if (onChainMarkets.size === 0) return null;

      // Get existing markets from DB
      const dbMarkets = await getMarkets();
      const dbSlabAddresses = new Set(dbMarkets.map(m => m.slab_address));

      // Find missing markets. Blocked slabs are skipped here rather than
      // deleted afterwards: discovery sees them on chain every cycle, so
      // without this a retired market is re-inserted within a minute of being
      // removed from the database. See src/blocklist.ts.
      const missingMarkets: Array<[string, any]> = [];
      let blockedSkipped = 0;
      for (const [slabAddress, state] of onChainMarkets.entries()) {
        if (isBlockedSlab(slabAddress)) {
          blockedSkipped++;
          continue;
        }
        if (!dbSlabAddresses.has(slabAddress)) {
          missingMarkets.push([slabAddress, state]);
        }
      }
      if (blockedSkipped > 0) {
        logger.debug("Skipped blocked slabs during registration", { count: blockedSkipped });
      }

      if (missingMarkets.length === 0) return dbMarkets;

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
          let logoUrl: string | null = null;
          let decimals = 9;
          try {
            const mintPubkey = new PublicKey(mintAddress);
            const mintInfo = await connection.getParsedAccountInfo(mintPubkey);
            if (mintInfo.value?.data && "parsed" in mintInfo.value.data) {
              decimals = mintInfo.value.data.parsed.info.decimals ?? 9;
            }
            // Collateral identity via DAS (sanitization lives in the helper).
            const collateralMeta = await fetchDasTokenMetadata(connection.rpcEndpoint, mintAddress);
            if (collateralMeta) {
              if (collateralMeta.symbol) symbol = collateralMeta.symbol;
              if (collateralMeta.name) name = collateralMeta.name;
              if (collateralMeta.decimals != null) decimals = collateralMeta.decimals;
              // Provisional: for a hyperp market the block below replaces this with
              // the BASE asset's logo, or clears it if the market can't be identified.
              if (collateralMeta.logoUrl) logoUrl = collateralMeta.logoUrl;
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
          //   3. Look up baseMint metadata via DAS API → use as symbol/name/logo.
          //   4. Construct the market name as "{baseSymbol}/{collateral} Perpetual".
          //   5. If NONE of that resolves, write a neutral placeholder.
          //
          // Step 5 used to default to "SOL"/"SOL/USDC Perpetual". v17 admin-oracle
          // markets have no dexPool, so that branch was the ONLY one that ever ran
          // for them and every such market claimed to be SOL — six unrelated devnet
          // markets all displayed as "SOL/USDC Perpetual". An unnamed market is
          // honest and is queryable for follow-up; a market mislabelled SOL is not.
          const zeroKeyBytesHyperp = new Uint8Array(32);
          const isHyperpMarket = (market as any).configV17 != null
            || (cfg?.indexFeedId?.equals(new PublicKey(zeroKeyBytesHyperp)) ?? false);
          if (isHyperpMarket) {
            let baseSymbol: string | null = null;
            let baseName: string | null = null;
            try {
              const dexPool = cfg?.dexPool ?? null;
              if (dexPool != null) {
                const poolAccountInfo = await connection.getAccountInfo(dexPool);
                if (poolAccountInfo) {
                  const dexType = detectDexType(poolAccountInfo.owner);
                  if (dexType != null) {
                    const poolInfo = parseDexPool(dexType, dexPool, new Uint8Array(poolAccountInfo.data));
                    const baseMintAddress = poolInfo.baseMint.toBase58();
                    const baseMeta = await fetchDasTokenMetadata(connection.rpcEndpoint, baseMintAddress);
                    if (baseMeta) {
                      baseSymbol = baseMeta.symbol;
                      baseName = baseMeta.name;
                      // The market's logo is the BASE asset's logo (what is being
                      // traded), not the collateral's — a SOL perp shows the SOL
                      // mark, not USDC's.
                      if (baseMeta.logoUrl) logoUrl = baseMeta.logoUrl;
                    }
                  }
                }
              }
            } catch (hyperpErr) {
              logger.debug("Hyperp base asset resolution failed", {
                slabAddress,
                error: hyperpErr instanceof Error ? hyperpErr.message : hyperpErr,
              });
            }

            if (baseSymbol) {
              // Collateral symbol came from the DAS lookup above (e.g. "USDC").
              // If that also failed, `symbol` is still the mint-prefix fallback.
              const collateralLabel = symbol === mintAddress.substring(0, 8) ? "USDC" : symbol;
              symbol = baseSymbol;
              name = `${baseSymbol}/${collateralLabel} Perpetual`;
            } else {
              // Nothing on-chain identifies this market. Do NOT guess, and do NOT
              // keep the collateral's identity (that would label every market
              // "USDC"). A human sets the real values via PATCH /api/markets/[slab],
              // which flips metadata_source to 'manual' so this never overwrites them.
              const placeholder = placeholderIdentity(slabAddress);
              symbol = placeholder.symbol;
              name = placeholder.name;
              // The collateral logo is wrong for an unidentified market — drop it
              // rather than show USDC's mark on something that isn't a USDC market.
              logoUrl = null;
            }

            logger.info("Hyperp market metadata resolved", {
              slabAddress,
              symbol,
              name,
              resolvedFromChain: baseSymbol != null,
              hasLogo: logoUrl != null,
              baseName,
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
          await insertMarketRow({
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
            logo_url: logoUrl,
          });

          logger.info("Market registered", { slabAddress, symbol, name, hasLogo: logoUrl != null });
        } catch (err) {
          logger.warn("Failed to register market", { slabAddress, error: err instanceof Error ? err.message : err });
        }
      }

      // Newly-inserted rows are absent from dbMarkets, but the caller only reads
      // indexer_excluded off it — which defaults to false for fresh inserts, the
      // same answer a re-read would give.
      return dbMarkets;
    } catch (err) {
      logger.error("Market sync failed", { error: err instanceof Error ? err.message : String(err), stack: err instanceof Error ? err.stack : undefined });
      return null;
    }
  }

  /**
   * Registry sync every cycle; the on-chain janitorial sweep every
   * AUTOCLOSE_INTERVAL_MS.
   */
  private async collect(): Promise<void> {
    if (this._collecting || !this._running) return;
    this._collecting = true;

    try {
      // Auto-register missing markets at the start of each cycle. Reuse the rows
      // it read for the indexer_excluded pass below (saves a full markets read).
      const syncedDbMarkets = await this.syncMarkets();

      const markets = this.marketProvider.getMarkets();
      if (markets.size === 0) {
        logger.warn("StatsCollector.collect: marketProvider has 0 markets, skipping");
        return;
      }

      // The sweep below is pure janitorial work (auto-close / re-enable) and costs
      // one RPC round-trip per SWEEP_BATCH_SIZE markets. Registration above already
      // ran; skip the sweep until it's due.
      const now = Date.now();
      if (now - this._lastSweepAt < AUTOCLOSE_INTERVAL_MS) return;
      this._lastSweepAt = now;

      logger.info("StatsCollector sweep started", { marketCount: markets.size });

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
        const dbMarkets = syncedDbMarkets ?? (await getMarkets());
        const excludedDbMarkets = dbMarkets.filter((m) => m.indexer_excluded === true);

        for (const m of excludedDbMarkets) {
          // If the market is back in the live discovery map, it may have recovered.
          // Re-enable it so collect() processes it this cycle.
          if (markets.has(m.slab_address)) {
            // DiscoveredMarket.engine is a v12 field — the SDK documents it as
            // "Absent (undefined) for v17 market group accounts". Reading it
            // here made `vault` 0 for EVERY v17 market, so isLive was always
            // false and a market auto-closed once could never be re-enabled: a
            // launch that stalled between InitMarket and its funding step got
            // closed by the sweep, and stayed hidden forever even after the
            // creator completed the deposit and the vault held real collateral.
            //
            // Read the vault from chain instead, v17-aware, the same way the
            // sweep below does (parseV17AccountStats). One account read per
            // excluded market, and the excluded set is normally tiny.
            let vault = 0n;
            let numUsedAccounts = 0;
            try {
              const info = await getConnection().getAccountInfo(new PublicKey(m.slab_address));
              if (info?.data) {
                const data = new Uint8Array(info.data);
                const eng = isV17Account(data) ? parseV17AccountStats(data).engine : parseEngine(data);
                vault = eng.vault;
                numUsedAccounts = eng.numUsedAccounts;
              }
            } catch {
              // Unreadable — leave it excluded and try again next cycle rather
              // than un-hiding a market we cannot verify.
            }
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

      // Batch the account reads: getMultipleAccountsInfo takes up to 100 keys, so
      // SWEEP_BATCH_SIZE keys per call instead of one call per market.
      const entries = Array.from(markets.entries()).filter(([slabAddress]) => !excludedSlabs.has(slabAddress));
      for (let i = 0; i < entries.length; i += SWEEP_BATCH_SIZE) {
        const batch = entries.slice(i, i + SWEEP_BATCH_SIZE);
        const slabPubkeys = batch.map(([slabAddress]) => new PublicKey(slabAddress));

        try {
          // Batch fetch all account infos in one RPC call
          const accountInfos = await withRetry(
            () => connection.getMultipleAccountsInfo(slabPubkeys),
            { 
              maxRetries: 3, 
              baseDelayMs: 1000, 
              label: `getMultipleAccountsInfo(batch ${i / SWEEP_BATCH_SIZE + 1})` 
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
            // maintains the markets registry (insertMarketRow via syncMarkets()) and the
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
        if (i + SWEEP_BATCH_SIZE < entries.length) {
          await new Promise((r) => setTimeout(r, SWEEP_BATCH_DELAY_MS));
        }
      }

      logger.info("StatsCollector sweep complete", { updated, errors, totalMarkets: markets.size });
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
