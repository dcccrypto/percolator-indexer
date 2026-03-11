import { PublicKey } from "@solana/web3.js";
import { discoverMarkets, type DiscoveredMarket } from "@percolator/sdk";
import { config, getConnection, getFallbackConnection, createLogger, captureException } from "@percolator/shared";

const logger = createLogger("indexer:market-discovery");

export class MarketDiscovery {
  private markets = new Map<string, { market: DiscoveredMarket }>();
  private timer: ReturnType<typeof setInterval> | null = null;
  
  /**
   * discoverMarkets with exponential backoff on transient RPC errors (issue #840).
   * Retries on: 429/rate-limit, 502/503/504 (Cloudflare/Helius transient failures).
   * Falls back to primary connection on retry so we aren't hammering one endpoint.
   */
  private async discoverWithRetry(
    programId: PublicKey,
    maxRetries = 4,
  ): Promise<DiscoveredMarket[]> {
    let lastErr: unknown;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      // Alternate between fallback (lower-priority) and primary connection on retries
      const conn = attempt === 0 ? getFallbackConnection() : getConnection();
      try {
        return await discoverMarkets(conn, programId);
      } catch (e) {
        lastErr = e;
        const msg = e instanceof Error ? e.message.toLowerCase() : String(e).toLowerCase();
        const isRateLimit = msg.includes("429") || msg.includes("too many requests") || msg.includes("rate limit");
        const isTransient = msg.includes("502") || msg.includes("503") || msg.includes("504")
          || msg.includes("bad gateway") || msg.includes("service unavailable")
          || msg.includes("gateway timeout") || msg.includes("econnreset")
          || msg.includes("econnrefused") || msg.includes("etimedout");
        const isRetryable = isRateLimit || isTransient;
        if (!isRetryable || attempt >= maxRetries - 1) throw e;
        const delayMs = Math.min(500 * Math.pow(2, attempt), 30_000); // 500ms → 1s → 2s → 4s
        logger.warn("discoverMarkets transient RPC error, backing off", {
          programId: programId.toBase58(),
          attempt: attempt + 1,
          delayMs,
          error: msg.substring(0, 120),
        });
        await new Promise((r) => setTimeout(r, delayMs));
      }
    }
    throw lastErr;
  }

  async discover(): Promise<DiscoveredMarket[]> {
    const programIds = config.allProgramIds;
    const all: DiscoveredMarket[] = [];
    
    for (const id of programIds) {
      try {
        const found = await this.discoverWithRetry(new PublicKey(id));
        all.push(...found);
      } catch (e) {
        logger.warn("Failed to discover on program", { programId: id, error: e });
      }
      // Pause between program IDs to reduce RPC burst pressure
      await new Promise(r => setTimeout(r, 2_000));
    }
    
    for (const market of all) {
      this.markets.set(market.slabAddress.toBase58(), { market });
    }
    
    logger.info("Market discovery complete", { totalMarkets: all.length });
    return all;
  }
  
  getMarkets() {
    return this.markets;
  }
  
  start(intervalMs = 300_000) {
    this.discover().catch((err) => {
      logger.error("Initial discovery failed", { error: err });
      captureException(err, { tags: { context: "market-discovery-initial" } });
    });
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
