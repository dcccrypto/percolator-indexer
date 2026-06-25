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
} from "@percolatorct/shared";

const logger = createLogger("indexer:adl-indexer");

function readPositiveIntEnv(name: string, fallback: number, max?: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) return fallback;
  return max ? Math.min(parsed, max) : parsed;
}

/**
 * v17: ExecuteAdl (v12.x tag 101) is NOT in the v17 wrapper decoder — it was removed.
 *
 * In v17:
 *   - Tag 50 = WithdrawBackingBucket (a completely different instruction — NOT ADL)
 *   - Tag 101 = CancelQueuedWithdrawal (also not ADL)
 *
 * ADL_TAGS is intentionally EMPTY to prevent mis-indexing any v17 instruction under
 * a stale tag number. The indexer is effectively a no-op and will log a warning on
 * startup. The adl_events table schema is preserved for historical data compatibility.
 *
 * When ADL is re-implemented in the v17 program, update ADL_TAGS with the new tag.
 */
const ADL_TAGS = new Set<number>(); // DISABLED: ExecuteAdl removed in v17

/** How many recent signatures to fetch per slab per cycle */
const MAX_SIGNATURES = readPositiveIntEnv("ADL_MAX_SIGNATURES", 50, 100);

/** Poll interval (5 minutes — backup/backfill only, primary is webhook) */
const POLL_INTERVAL_MS = readPositiveIntEnv("ADL_POLL_INTERVAL_MS", 5 * 60_000);

/** Initial backfill: fetch more signatures on first run */
const BACKFILL_SIGNATURES = readPositiveIntEnv("ADL_BACKFILL_SIGNATURES", 100, 100);

/** Helius historical-data batch cap is 100. Keep ours lower to avoid bursty catches. */
const TX_BATCH_SIZE = readPositiveIntEnv("INDEXER_TX_BATCH_SIZE", 10, 100);
const TX_FETCH_RETRIES = readPositiveIntEnv("INDEXER_TX_FETCH_RETRIES", 2, 5);
const STARTUP_BACKFILL_ENABLED = process.env.INDEXER_STARTUP_BACKFILL_ENABLED !== "false";

/**
 * AdlIndexerPolling — NO-OP in v17. ExecuteAdl was removed from the v17 wrapper program.
 *
 * ADL_TAGS is empty so no transactions will ever be indexed. The service starts,
 * logs a warning, and then does nothing. The adl_events table and upsert logic are
 * preserved for historical data compatibility and future re-enablement.
 *
 * To re-enable: confirm the v17 ADL instruction tag and add it to ADL_TAGS above.
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

    // v17: ADL removed from the v17 wrapper program. This indexer is a no-op.
    // ADL_TAGS is empty so processTransaction will never match any instruction.
    logger.warn(
      "AdlIndexerPolling is DISABLED — ExecuteAdl was removed in v17. " +
      "ADL_TAGS is empty; no ADL events will be indexed. " +
      "Update ADL_TAGS when v17 re-introduces an ADL instruction.",
    );

    // Keep the poll infrastructure running so the service can be re-enabled
    // without a code change — just update ADL_TAGS above.
    if (STARTUP_BACKFILL_ENABLED) {
      setTimeout(() => this.backfill(), 5_000);
    } else {
      this.hasBackfilled = true;
    }

    // Start periodic polling
    this.pollTimer = setInterval(() => this.pollAllMarkets(), POLL_INTERVAL_MS);

    logger.info("AdlIndexerPolling started (backup mode)", {
      intervalMs: POLL_INTERVAL_MS,
      startupBackfillEnabled: STARTUP_BACKFILL_ENABLED,
      maxSignatures: MAX_SIGNATURES,
      txBatchSize: TX_BATCH_SIZE,
    });
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

    // Fetch transactions in Helius-supported historical batches instead of
    // parallel single-tx calls. This reduces request bursts and avoids 429 loops.
    for (let i = 0; i < validSigInfos.length; i += TX_BATCH_SIZE) {
      const batch = validSigInfos.slice(i, i + TX_BATCH_SIZE);
      const signatures = batch.map(({ signature }) => signature);
      const txs = await withRetry(
        () => connection.getParsedTransactions(signatures, { maxSupportedTransactionVersion: 0 }),
        {
          maxRetries: TX_FETCH_RETRIES,
          baseDelayMs: 1000,
          label: `getParsedTransactions(${signatures.length})`,
        },
      );

      for (let j = 0; j < txs.length; j++) {
        const tx = txs[j];
        if (!tx) continue;
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
