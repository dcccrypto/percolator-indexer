import { PublicKey } from "@solana/web3.js";
import { discoverMarkets, type DiscoveredMarket } from "@percolatorct/sdk";
import { config, getPrimaryConnection, getFallbackConnection, createLogger, captureException } from "@percolatorct/shared";

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

export class MarketDiscovery {
  private markets = new Map<string, { market: DiscoveredMarket }>();
  private timer: ReturnType<typeof setInterval> | null = null;
  private consecutiveFailures = 0;
  private _discovering = false;

  async discover(): Promise<DiscoveredMarket[]> {
    if (this._discovering) {
      logger.warn("discover() already in progress — skipping overlapping invocation");
      return [];
    }
    this._discovering = true;
    try {
      return await this._doDiscover();
    } finally {
      this._discovering = false;
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
    
    for (const id of programIds) {
      let discovered = false;
      for (let attempt = 0; attempt <= HELIUS_429_BACKOFF_MS.length; attempt++) {
        // After exhausting all retries on primary, try public fallback once before giving up
        const conn = attempt === HELIUS_429_BACKOFF_MS.length ? fallbackConn : primaryConn;
        const connLabel = conn === fallbackConn ? "fallback" : "primary";
        try {
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
  
  async start(intervalMs = 300_000) {
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
  }
  
  stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}
