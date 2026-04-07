import { getSupabase, getNetwork, config, createLogger } from "@percolator/shared";
import type { DiscoveredMarket } from "@percolatorct/sdk";

const logger = createLogger("indexer:insurance-lp");

// BL2: Named constants for magic numbers
const POLL_INTERVAL_MS = 120_000;
const MS_PER_DAY = 86_400_000;
const REDEMPTION_RATE_E6_DEFAULT = 1_000_000; // 1:1 ratio when no LPs

interface InsuranceSnapshot {
  slab: string;
  insurance_balance: number;
  lp_supply: number;
  redemption_rate_e6: number;
  snapshot_slot: number;
  created_at: string;
}

interface InsuranceStats {
  balance: number;
  lpSupply: number;
  redemptionRate: number;
  apy7d: number | null;
  apy30d: number | null;
}

interface MarketProvider {
  getMarkets(): Map<string, { market: DiscoveredMarket }>;
}

export class InsuranceLPService {
  private timer: ReturnType<typeof setInterval> | null = null;
  private readonly marketProvider: MarketProvider;
  private cache = new Map<string, InsuranceStats>();

  constructor(marketProvider: MarketProvider) {
    this.marketProvider = marketProvider;
  }

  start(): void {
    if (this.timer) return;
    if (!config.supabaseUrl || !config.supabaseKey) {
      logger.warn("SUPABASE_URL/KEY not set, service disabled");
      return;
    }
    this.poll().catch((e) => logger.error("Failed to run initial poll", { error: e }));
    this.timer = setInterval(() => {
      this.poll().catch((e) => logger.error("Failed to poll insurance data", { error: e }));
    }, POLL_INTERVAL_MS);
    logger.info("InsuranceLPService started", { intervalMs: POLL_INTERVAL_MS });
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  getStats(slab: string): InsuranceStats | null {
    return this.cache.get(slab) ?? null;
  }

  private async poll(): Promise<void> {
    const markets = this.marketProvider.getMarkets();

    for (const [slab, state] of markets.entries()) {
      try {
        const engine = state.market.engine;
        
        // Get real insurance balance from on-chain engine state
        const insuranceBalance = Number(engine.insuranceFund.balance);

        // Insurance LP program removed in @percolatorct/sdk@1.0.0-beta.4.
        // deriveInsuranceLpMint no longer exists; LP supply is always 0.
        const lpSupply = 0;

        const redemptionRateE6 =
          lpSupply > 0 ? Math.floor((insuranceBalance * 1_000_000) / lpSupply) : REDEMPTION_RATE_E6_DEFAULT;

        // Record snapshot — stamp network to prevent devnet/mainnet mixing (PERC-8192)
        const db = getSupabase();
        await db.from("insurance_snapshots").insert({
          slab,
          insurance_balance: insuranceBalance,
          lp_supply: lpSupply,
          redemption_rate_e6: redemptionRateE6,
          snapshot_slot: Number(engine.lastCrankSlot),
          network: getNetwork(),
        });

        // Compute APY from history
        const apy7d = await this.computeTrailingAPY(slab, 7);
        const apy30d = await this.computeTrailingAPY(slab, 30);

        this.cache.set(slab, {
          balance: insuranceBalance,
          lpSupply,
          redemptionRate: redemptionRateE6,
          apy7d,
          apy30d,
        });
      } catch (err) {
        logger.error("Error polling market", { slab, error: err });
      }
    }

    // Evict cache entries for slabs no longer in the active market set
    for (const key of this.cache.keys()) {
      if (!markets.has(key)) this.cache.delete(key);
    }
  }

  private async computeTrailingAPY(slab: string, days: number): Promise<number | null> {
    // BM5: Add error handling for APY calculation
    try {
      const db = getSupabase();
      const since = new Date(Date.now() - days * MS_PER_DAY).toISOString();

      // PERC-8192: Filter by network to prevent devnet snapshots from
      // contaminating mainnet APY calculations (insert at line 122 stamps
      // network, but this query was missing the filter).
      const { data, error } = await db
        .from("insurance_snapshots")
        .select("redemption_rate_e6, created_at")
        .eq("slab", slab)
        .eq("network", getNetwork())
        .gte("created_at", since)
        .order("created_at", { ascending: true })
        .limit(1);

      if (error || !data || data.length === 0) return null;

      const oldest = data[0] as InsuranceSnapshot;
      const oldRate = oldest.redemption_rate_e6;

      const current = this.cache.get(slab);
      if (!current || oldRate === 0) return null;

      const growth = (current.redemptionRate - oldRate) / oldRate;
      const elapsed = Date.now() - new Date(oldest.created_at).getTime();
      if (elapsed < MS_PER_DAY) return null; // need at least 1 day of data

      // Guard against infinite/NaN results
      if (!isFinite(growth) || !isFinite(elapsed) || elapsed === 0) return null;

      const annualized = growth * (365 * MS_PER_DAY) / elapsed;
      
      if (!isFinite(annualized)) return null;
      
      return Math.round(annualized * 10_000) / 10_000; // 4 decimal places
    } catch (err) {
      logger.error("APY calculation error", { slab, error: err });
      return null;
    }
  }

  async getEvents(slab: string, limit = 50) {
    const db = getSupabase();
    const { data, error } = await db
      .from("insurance_lp_events")
      .select("*")
      .eq("slab", slab)
      .eq("network", getNetwork())
      .order("created_at", { ascending: false })
      .limit(limit);

    if (error) throw error;
    return data ?? [];
  }

  async getDepositorCount(slab: string): Promise<number> {
    const db = getSupabase();
    const { data, error } = await db
      .from("insurance_lp_events")
      .select("user_wallet")
      .eq("slab", slab)
      .eq("event_type", "deposit")
      .eq("network", getNetwork());

    if (error) throw error;
    if (!data) return 0;
    const uniqueWallets = new Set(data.map((row: { user_wallet: string }) => row.user_wallet));
    return uniqueWallets.size;
  }
}
