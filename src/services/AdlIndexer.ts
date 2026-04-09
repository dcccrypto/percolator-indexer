// SQL Schema:
// -- adl_events: id, signature UNIQUE, slab, target_idx, slot, timestamp, network
//
// CREATE TABLE adl_events (
//   id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//   signature  TEXT NOT NULL UNIQUE,
//   slab       TEXT NOT NULL,
//   target_idx INT  NOT NULL,
//   slot       BIGINT NOT NULL,
//   timestamp  BIGINT NOT NULL,
//   network    TEXT NOT NULL,
//   created_at TIMESTAMPTZ DEFAULT NOW()
// );
// CREATE INDEX idx_adl_events_slab      ON adl_events(slab);
// CREATE INDEX idx_adl_events_slot      ON adl_events(slot);
// CREATE INDEX idx_adl_events_target    ON adl_events(target_idx);

import { PublicKey, type ParsedTransactionWithMeta } from "@solana/web3.js";
import { IX_TAG } from "@percolatorct/sdk";
import {
  config,
  getConnection,
  getSupabase,
  getMarkets,
  getNetwork,
  withRetry,
  decodeBase58,
  createLogger,
  captureException,
} from "@percolator/shared";

const logger = createLogger("indexer:adl-indexer");

/**
 * Percolator-prog tag for ADL execution.
 * Tag 50 = ExecuteAdl (PERC-305).
 */
const ADL_TAGS = new Set<number>([IX_TAG.ExecuteAdl]);

/** How many recent signatures to fetch per slab per cycle */
const MAX_SIGNATURES = 50;

/** Poll interval (5 minutes — backup/backfill only, primary is webhook) */
const POLL_INTERVAL_MS = 5 * 60_000;

/** Initial backfill: fetch more signatures on first run */
const BACKFILL_SIGNATURES = 100;

/**
 * AdlIndexerPolling — backup/backfill indexer for Auto-Deleveraging (ADL) events.
 *
 * Tracks IX_TAG.ExecuteAdl (50) on percolator-prog. Polls all active market slabs
 * and upserts events to the `adl_events` table.
 *
 * Account layout for ExecuteAdl (percolator-prog):
 *   [0] caller/signer
 *   [1] slab (the perp market slab account)
 *   [2+] additional accounts
 *
 * Data layout:
 *   ExecuteAdl (tag 50): tag(1) + target_idx(u16 LE = 2) — min 3 bytes
 */
export class AdlIndexerPolling {
  /** Track last indexed signature per slab to avoid re-processing */
  private lastSignature = new Map<string, string>();
  private _running = false;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private hasBackfilled = false;
  private backfillAttempts = 0;

  start(): void {
    if (this._running) return;
    this._running = true;

    // Initial backfill after short delay to let discovery finish
    setTimeout(() => this.backfill(), 5_000);

    // Start periodic polling
    this.pollTimer = setInterval(() => this.pollAllMarkets(), POLL_INTERVAL_MS);

    logger.info("AdlIndexerPolling started (backup mode)", { intervalMs: POLL_INTERVAL_MS });
  }

  stop(): void {
    this._running = false;
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    logger.info("AdlIndexer stopped");
  }

  /**
   * Backfill: fetch recent ADL events for all known market slabs on startup.
   */
  private async backfill(): Promise<void> {
    if (this.hasBackfilled || !this._running) return;

    try {
      const markets = await getMarkets();
      if (markets.length === 0) {
        logger.info("No markets found for ADL backfill");
        this.hasBackfilled = true;
        return;
      }

      logger.info("Starting ADL backfill", { marketCount: markets.length });
      for (const market of markets) {
        if (!this._running) break;
        try {
          await this.indexAdlEventsForSlab(market.slab_address, BACKFILL_SIGNATURES);
        } catch (err) {
          logger.error("Backfill error", {
            slabAddress: market.slab_address.slice(0, 8),
            error: err instanceof Error ? err.message : err,
          });
          captureException(err, {
            tags: {
              context: "adl-indexer-backfill",
              slabAddress: market.slab_address,
            },
          });
        }
        await sleep(1_000);
      }
      this.hasBackfilled = true;
      logger.info("ADL backfill complete");
    } catch (err) {
      logger.error("Backfill failed", { error: err instanceof Error ? err.message : err });
      captureException(err, {
        tags: { context: "adl-indexer-backfill" },
      });
      // Retry with backoff, up to 3 attempts
      this.backfillAttempts++;
      if (this.backfillAttempts < 3 && this._running) {
        const delayMs = 10_000 * Math.pow(2, this.backfillAttempts); // 20s, 40s
        logger.info("Scheduling backfill retry", { attempt: this.backfillAttempts, delayMs });
        setTimeout(() => this.backfill(), delayMs);
      } else {
        this.hasBackfilled = true;
        logger.error("Backfill exhausted retries — pollAllMarkets will cover the gap");
      }
    }
  }

  /**
   * Poll all active market slabs for new ADL events.
   */
  private async pollAllMarkets(): Promise<void> {
    if (!this._running) return;

    try {
      const markets = await getMarkets();
      for (const market of markets) {
        if (!this._running) break;
        try {
          await this.indexAdlEventsForSlab(market.slab_address, MAX_SIGNATURES);
        } catch (err) {
          logger.error("Poll error", {
            slabAddress: market.slab_address.slice(0, 8),
            error: err instanceof Error ? err.message : err,
          });
          captureException(err, {
            tags: {
              context: "adl-indexer-poll",
              slabAddress: market.slab_address,
            },
          });
        }
        await sleep(500);
      }
    } catch (err) {
      logger.error("Poll failed", { error: err instanceof Error ? err.message : err });
      captureException(err, {
        tags: { context: "adl-indexer-poll" },
      });
    }
  }

  private async indexAdlEventsForSlab(slabAddress: string, maxSigs = MAX_SIGNATURES): Promise<void> {
    const connection = getConnection();
    const slabPk = new PublicKey(slabAddress);
    const programIds = new Set(config.allProgramIds);

    const opts: { limit: number; until?: string } = { limit: maxSigs };
    const lastSig = this.lastSignature.get(slabAddress);
    if (lastSig) opts.until = lastSig;

    let sigInfos;
    try {
      sigInfos = await withRetry(
        () => connection.getSignaturesForAddress(slabPk, opts),
        {
          maxRetries: 3,
          baseDelayMs: 1000,
          label: `getSignaturesForAddress(${slabAddress.slice(0, 8)})`,
        }
      );
    } catch (err) {
      logger.warn("Failed to get signatures after retries", {
        slabAddress: slabAddress.slice(0, 8),
        error: err instanceof Error ? err.message : err,
      });
      return;
    }

    if (sigInfos.length === 0) return;

    // Update last signature (most recent first)
    this.lastSignature.set(slabAddress, sigInfos[0].signature);

    // Filter out errored transactions; retain slot/blockTime for later
    const validSigInfos = sigInfos.filter(s => !s.err);
    if (validSigInfos.length === 0) return;

    let indexed = 0;

    // Fetch transactions in batches of 5
    for (let i = 0; i < validSigInfos.length; i += 5) {
      const batch = validSigInfos.slice(i, i + 5);
      const txResults = await Promise.allSettled(
        batch.map(({ signature: sig }) =>
          withRetry(
            () => connection.getParsedTransaction(sig, { maxSupportedTransactionVersion: 0 }),
            {
              maxRetries: 3,
              baseDelayMs: 1000,
              label: `getParsedTransaction(${sig.slice(0, 12)})`,
            }
          )
        )
      );

      for (let j = 0; j < txResults.length; j++) {
        const result = txResults[j];
        if (result.status !== "fulfilled" || !result.value) continue;

        const tx = result.value;
        const sigInfo = batch[j];

        try {
          const didIndex = await this.processTransaction(
            tx,
            sigInfo.signature,
            slabAddress,
            programIds,
            sigInfo.slot,
            sigInfo.blockTime ?? 0,
          );
          if (didIndex) indexed++;
        } catch (err) {
          logger.warn("Failed to process transaction", {
            signature: sigInfo.signature.slice(0, 12),
            error: err instanceof Error ? err.message : err,
          });
        }
      }
    }

    if (indexed > 0) {
      logger.info("ADL events indexed", { count: indexed, slabAddress: slabAddress.slice(0, 8) });
    }
  }

  private async processTransaction(
    tx: ParsedTransactionWithMeta,
    signature: string,
    slabAddress: string,
    programIds: Set<string>,
    slot: number,
    timestamp: number,
  ): Promise<boolean> {
    if (!tx.meta || tx.meta.err) return false;

    const message = tx.transaction.message;

    for (const ix of message.instructions) {
      // Skip parsed instructions (system, token, etc.)
      if ("parsed" in ix) continue;

      // Check if this instruction is for one of our programs
      const programId = ix.programId.toBase58();
      if (!programIds.has(programId)) continue;

      // Decode instruction tag from data
      const data = decodeBase58(ix.data);
      if (!data || data.length < 1) continue;

      const tag = data[0];
      if (!ADL_TAGS.has(tag)) continue;

      // ExecuteAdl: tag(1) + target_idx(u16 LE = 2) — min 3 bytes
      if (data.length < 3) continue;

      const targetIdx = data[1] | (data[2] << 8); // u16 little-endian

      // Account layout: [0] = caller/signer, [1] = slab
      const accountSlab = ix.accounts[1]?.toBase58();
      if (!accountSlab) continue;

      // accountSlab should match the slab we're polling — sanity-check
      if (accountSlab !== slabAddress) continue;

      await this.upsertAdlEvent({
        signature,
        slab: slabAddress,
        target_idx: targetIdx,
        slot,
        timestamp,
      });

      return true;
    }

    return false;
  }

  private async upsertAdlEvent(event: {
    signature: string;
    slab: string;
    target_idx: number;
    slot: number;
    timestamp: number;
  }): Promise<void> {
    const { error } = await getSupabase()
      .from("adl_events")
      .upsert(
        {
          signature: event.signature,
          slab: event.slab,
          target_idx: event.target_idx,
          slot: event.slot,
          timestamp: event.timestamp,
          network: getNetwork(),
        },
        { onConflict: "signature" }
      );

    if (error) {
      // Duplicate inserts are expected when polling overlaps — not an error
      if (error.message.includes("23505") || error.message.toLowerCase().includes("duplicate")) {
        logger.debug("Duplicate ADL event insert skipped", { signature: event.signature.slice(0, 12) });
        return;
      }
      throw new Error(`Failed to upsert ADL event: ${error.message}`);
    }

    logger.debug("ADL event upserted", {
      targetIdx: event.target_idx,
      slab: event.slab.slice(0, 8),
      signature: event.signature.slice(0, 12),
    });
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
