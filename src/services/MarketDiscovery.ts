import { PublicKey } from "@solana/web3.js";
import { discoverMarkets, getMarketsByAddress, isV17Account, type DiscoveredMarket } from "@percolatorct/sdk";
import { config, getPrimaryConnection, getFallbackConnection, createLogger, captureException, sendCriticalAlert } from "@percolatorct/shared";
import { discoverV17Markets } from "../v17/discovery.js";

const logger = createLogger("indexer:market-discovery");

const INITIAL_RETRY_DELAYS = [5_000, 15_000, 30_000, 60_000]; // escalating backoff

/**
 * Exponential backoff delays for Helius 429 rate-limit responses during discovery.
 * Helius free/starter plans cap getProgramAccounts at ~40 req/s. When discovery
 * iterates multiple programs in quick succession each call internally batches
 * several RPC calls and can exhaust the limit. These delays give the rate limiter
 * time to recover before the next program is attempted.
 */
const HELIUS_429_BACKOFF_MS = [2_000, 5_000, 15_000, 30_000]; // per-program retry

/** Jitter: add up to 25% random delay to avoid thundering-herd on retry. */
function withJitter(delayMs: number): number {
  return delayMs + Math.floor(Math.random() * delayMs * 0.25);
}

/** Return true if the error looks like an HTTP 429 / rate-limit response. */
function isRateLimitError(err: unknown): boolean {
  if (!err) return false;
  const msg = err instanceof Error ? err.message : String(err);
  return msg.includes("429") || msg.toLowerCase().includes("rate limit") || msg.toLowerCase().includes("too many requests");
}

/**
 * #223: cadence of the full multi-tier discovery pass (v17 scan + ~43 slab-size-tier scans +
 * the legacy fallback scans, ~45 getProgramAccounts calls per program). Unchanged from the
 * shared config's default; DISCOVERY_INTERVAL_MS still overrides it.
 */
export const DEFAULT_FULL_DISCOVERY_INTERVAL_MS = 300_000;

/**
 * #223: cadence of the light pass: ONE getProgramAccounts (v17 KIND_MARKET filter, header-only
 * dataSlice) that registers a market created since the last pass. This is what bounds how long
 * a new market stays unlisted. DISCOVERY_LIGHT_INTERVAL_MS overrides it; 0 disables the pass.
 */
export const DEFAULT_LIGHT_DISCOVERY_INTERVAL_MS = 60_000;

type DiscoveredListener = (markets: DiscoveredMarket[]) => void | Promise<void>;

export class MarketDiscovery {
  private markets = new Map<string, { market: DiscoveredMarket }>();
  private timer: ReturnType<typeof setInterval> | null = null;
  private lightTimer: ReturnType<typeof setInterval> | null = null;
  private consecutiveFailures = 0;
  private _discovering = false;
  private discoveredListeners: DiscoveredListener[] = [];

  /**
   * #223: run `fn` after a pass that found a market not previously known (so it can be
   * registered right away instead of on the next stats sweep). Listener
   * errors are logged and never affect discovery. Returns an unsubscribe function.
   */
  onDiscovered(fn: DiscoveredListener): () => void {
    this.discoveredListeners.push(fn);
    return () => {
      this.discoveredListeners = this.discoveredListeners.filter((l) => l !== fn);
    };
  }

  async discover(): Promise<DiscoveredMarket[]> {
    if (this._discovering) {
      logger.warn("discover() already in progress — skipping overlapping invocation");
      return [];
    }
    this._discovering = true;
    let found: DiscoveredMarket[] = [];
    const known = new Set(this.markets.keys());
    try {
      found = await this._doDiscover();
    } finally {
      this._discovering = false;
    }
    // #223: only a market that was not already known is news; an unchanged set must not
    // trigger the registration pass (it reads the whole `markets` table).
    const fresh = found.filter((m) => !known.has(m.slabAddress.toBase58()));
    if (fresh.length > 0) this.notifyDiscovered(fresh);
    return found;
  }

  /**
   * #223: the light pass. One v17 KIND_MARKET-filtered scan per program with a header-only
   * dataSlice (or one getMultipleAccounts under MARKETS_FILTER), instead of the ~45-call full
   * pass. It only ever ADDS markets to the map: a failed, rate-limited or empty scan leaves the
   * map untouched, and removal stays the full pass's job. Listeners fire only for addresses
   * that were not already known. Skipped while a full pass is running. Returns the new markets.
   */
  async discoverLight(): Promise<DiscoveredMarket[]> {
    if (this._discovering) return [];
    this._discovering = true;
    const fresh: DiscoveredMarket[] = [];
    try {
      const conn = getPrimaryConnection();
      const filter = (process.env.MARKETS_FILTER ?? "").trim();
      const addresses = filter ? filter.split(",").map((a) => a.trim()).filter(Boolean).map((a) => new PublicKey(a)) : undefined;
      for (const id of config.allProgramIds) {
        try {
          const found = await discoverV17Markets(conn, new PublicKey(id), addresses, { light: true });
          for (const m of found) {
            const key = m.slabAddress.toBase58();
            if (!this.markets.has(key) && !fresh.some((f) => f.slabAddress.toBase58() === key)) fresh.push(m);
          }
        } catch (e) {
          logger.warn("Light discovery failed on program", { programId: id, error: e instanceof Error ? e.message : String(e) });
        }
      }
      if (fresh.length > 0) {
        // Atomic swap on a copy: readers never see a partial map, and nothing is removed.
        const next = new Map(this.markets);
        for (const m of fresh) next.set(m.slabAddress.toBase58(), { market: m });
        this.markets = next;
      }
    } finally {
      this._discovering = false;
    }
    if (fresh.length > 0) {
      logger.info("Light discovery found new markets", { count: fresh.length });
      this.notifyDiscovered(fresh);
    }
    return fresh;
  }

  private notifyDiscovered(markets: DiscoveredMarket[]): void {
    for (const fn of this.discoveredListeners) {
      Promise.resolve()
        .then(() => fn(markets))
        .catch((err) => logger.warn("onDiscovered listener failed", { error: err instanceof Error ? err.message : String(err) }));
    }
  }

  private async _doDiscover(): Promise<DiscoveredMarket[]> {
    const programIds = config.allProgramIds;
    // Use Helius primary RPC (primaryConn) for all discovery attempts.
    // fallbackConn is tried exactly once — only after all HELIUS_429_BACKOFF_MS retries are
    // exhausted due to 429 rate-limit responses. Non-429 errors (transport, auth) cause an
    // immediate per-program failure without falling back.
    const primaryConn = getPrimaryConnection();
    const fallbackConn = getFallbackConnection();
    const all: DiscoveredMarket[] = [];
    let failedPrograms = 0;

    const marketsFilter = (process.env.MARKETS_FILTER ?? "").trim();
    if (marketsFilter) {
      const slabAddresses = marketsFilter
        .split(",")
        .map(s => s.trim())
        .filter(Boolean)
        .map(s => new PublicKey(s));

      logger.info("Using MARKETS_FILTER — skipping getProgramAccounts discovery", {
        count: slabAddresses.length,
      });

      for (const id of programIds) {
        try {
          // #145: Always run BOTH v17 and v12 scanners and merge results.
          // A program may host a mix of v17 and v12 markets — skipping the v12
          // scan whenever v17 finds anything silently drops all v12 markets under
          // that program from the live map.
          const v17Found = await discoverV17Markets(primaryConn, new PublicKey(id), slabAddresses);
          if (v17Found.length > 0) {
            all.push(...v17Found);
          }
          // Always also run v12 SDK path for legacy markets, regardless of v17 results.
          const found = await getMarketsByAddress(
            primaryConn,
            new PublicKey(id),
            slabAddresses,
            { batchSize: 25, interBatchDelayMs: 250 },
          );
          all.push(...found);
        } catch (e) {
          failedPrograms++;
          logger.warn("MARKETS_FILTER discovery failed on program", {
            programId: id,
            error: e instanceof Error ? e.message : String(e),
          });
        }
      }

      if (all.length > 0) {
        const newMarkets = new Map<string, { market: DiscoveredMarket }>();
        for (const market of all) {
          newMarkets.set(market.slabAddress.toBase58(), { market });
        }
        this.markets = newMarkets;
        this.consecutiveFailures = 0;
      } else {
        logger.warn("MARKETS_FILTER discovery found 0 markets", {
          count: slabAddresses.length,
          failedPrograms,
        });
      }

      logger.info("Market discovery complete", {
        totalMarkets: all.length,
        failedPrograms,
        consecutiveFailures: this.consecutiveFailures,
      });
      return all;
    }
    
    for (const id of programIds) {
      let discovered = false;
      for (let attempt = 0; attempt <= HELIUS_429_BACKOFF_MS.length; attempt++) {
        // After exhausting all retries on primary, try public fallback once before giving up
        const conn = attempt === HELIUS_429_BACKOFF_MS.length ? fallbackConn : primaryConn;
        const connLabel = conn === fallbackConn ? "fallback" : "primary";
        try {
          // #145: Always run BOTH v17 and v12 scanners and merge results.
          // A program may host a mix of v17 and v12 markets — breaking out of the
          // retry loop on the first v17 hit silently drops all v12 markets under
          // that program from the live map.
          const v17Found = await discoverV17Markets(conn, new PublicKey(id));
          if (v17Found.length > 0) {
            all.push(...v17Found);
          }
          // Always also run v12 discovery for legacy markets, regardless of v17 results.
          const found = await discoverMarkets(conn, new PublicKey(id));
          all.push(...found);
          discovered = true;
          if (conn === fallbackConn) {
            logger.warn("discoverMarkets succeeded on fallback RPC after primary 429s", { programId: id });
          }
          break;
        } catch (e) {
          if (isRateLimitError(e) && attempt < HELIUS_429_BACKOFF_MS.length) {
            const delay = withJitter(HELIUS_429_BACKOFF_MS[attempt]);
            logger.warn("Helius 429 on discoverMarkets — backing off", {
              programId: id,
              conn: connLabel,
              attempt: attempt + 1,
              delayMs: delay,
            });
            await new Promise(r => setTimeout(r, delay));
            continue;
          }
          // Non-429 error or exhausted retries (including fallback)
          failedPrograms++;
          logger.warn("Failed to discover on program", { programId: id, conn: connLabel, error: e, attempt: attempt + 1 });
          break;
        }
      }
      // Inter-program spacing: 2s base, helps avoid consecutive 429s on multi-program configs
      await new Promise(r => setTimeout(r, 2000));
    }
    
    // All programs failed — RPC is likely down
    if (failedPrograms === programIds.length && programIds.length > 0) {
      this.consecutiveFailures++;
      const err = new Error(`Market discovery failed for all ${programIds.length} programs (consecutive: ${this.consecutiveFailures})`);
      logger.error("All program discoveries failed — RPC may be down", {
        consecutiveFailures: this.consecutiveFailures,
        staleMarkets: this.markets.size,
      });
      captureException(err, { tags: { context: "market-discovery-total-failure" } });
      // Alert operators after 3 consecutive total failures (avoid noise on transient blips)
      if (this.consecutiveFailures >= 3) {
        sendCriticalAlert("Market discovery failed for all programs — RPC may be down", [
          { name: "Consecutive failures", value: String(this.consecutiveFailures), inline: true },
          { name: "Stale markets", value: String(this.markets.size), inline: true },
        ]).catch((alertErr) => logger.error("Failed to send discovery alert", { error: alertErr }));
      }
      // Preserve stale markets — do NOT clear the map
      return [];
    }
    
    // Discovery returned 0 markets despite some programs succeeding
    if (all.length === 0) {
      logger.warn("Discovery succeeded but found 0 markets", {
        programCount: programIds.length,
        failedPrograms,
      });
    }
    
    // Only update the map when we actually found markets
    if (all.length > 0) {
      // Atomic swap: build new map first, then replace reference in one step.
      // This ensures concurrent readers via getMarkets() never see a partially
      // populated or empty map during the rebuild.
      const newMarkets = new Map<string, { market: DiscoveredMarket }>();
      for (const market of all) {
        newMarkets.set(market.slabAddress.toBase58(), { market });
      }
      this.markets = newMarkets;
      this.consecutiveFailures = 0;
    }
    
    logger.info("Market discovery complete", {
      totalMarkets: all.length,
      failedPrograms,
      consecutiveFailures: this.consecutiveFailures,
    });
    return all;
  }
  
  getMarkets() {
    return this.markets;
  }
  
  async start(intervalMs = DEFAULT_FULL_DISCOVERY_INTERVAL_MS, lightIntervalMs = 0) {
    // Initial discovery with retry + backoff
    let initialSuccess = false;
    for (let attempt = 0; attempt <= INITIAL_RETRY_DELAYS.length; attempt++) {
      try {
        const markets = await this.discover();
        if (markets.length > 0) {
          initialSuccess = true;
          break;
        }
        // Got 0 markets — worth retrying
        if (attempt < INITIAL_RETRY_DELAYS.length) {
          const delay = INITIAL_RETRY_DELAYS[attempt];
          logger.warn(`Initial discovery returned 0 markets, retrying in ${delay / 1000}s`, { attempt: attempt + 1 });
          await new Promise(r => setTimeout(r, delay));
        }
      } catch (err) {
        logger.error("Initial discovery failed", { error: err, attempt: attempt + 1 });
        captureException(err, { tags: { context: "market-discovery-initial", attempt: String(attempt + 1) } });
        if (attempt < INITIAL_RETRY_DELAYS.length) {
          const delay = INITIAL_RETRY_DELAYS[attempt];
          logger.warn(`Retrying initial discovery in ${delay / 1000}s`);
          await new Promise(r => setTimeout(r, delay));
        }
      }
    }
    
    if (!initialSuccess) {
      logger.error("Initial market discovery exhausted all retries — will continue with periodic polling");
    }
    
    this.timer = setInterval(() => this.discover().catch((err) => {
      logger.error("Discovery failed", { error: err });
      captureException(err, { tags: { context: "market-discovery-periodic" } });
    }), intervalMs);

    // #223: the fast path. Only worth running when it is meaningfully faster than the full pass.
    if (lightIntervalMs > 0 && lightIntervalMs < intervalMs) {
      this.lightTimer = setInterval(() => this.discoverLight().catch((err) => {
        logger.warn("Light discovery failed", { error: err instanceof Error ? err.message : String(err) });
      }), lightIntervalMs);
    }
  }
  
  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    if (this.lightTimer) {
      clearInterval(this.lightTimer);
      this.lightTimer = null;
    }
  }
}
