// SQL Schema:
// -- position_nft_events: id, signature UNIQUE, event_type, slab, user_idx, owner, nft_mint, slot, timestamp, network
//
// v17 MIGRATION NOTE: Add 'transfer' to the CHECK constraint before deploying v17:
//   ALTER TABLE position_nft_events DROP CONSTRAINT position_nft_events_event_type_check;
//   ALTER TABLE position_nft_events ADD CONSTRAINT position_nft_events_event_type_check
//     CHECK (event_type IN ('mint', 'burn', 'transfer'));
//
// CREATE TABLE position_nft_events (
//   id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//   signature  TEXT NOT NULL UNIQUE,
//   event_type TEXT NOT NULL CHECK (event_type IN ('mint', 'burn', 'transfer')),
//   slab       TEXT NOT NULL,
//   user_idx   INT  NOT NULL,
//   owner      TEXT NOT NULL,
//   nft_mint   TEXT,
//   slot       BIGINT NOT NULL,
//   timestamp  BIGINT NOT NULL,
//   network    TEXT NOT NULL,
//   created_at TIMESTAMPTZ DEFAULT NOW()
// );
// CREATE INDEX idx_position_nft_events_slab      ON position_nft_events(slab);
// CREATE INDEX idx_position_nft_events_owner     ON position_nft_events(owner);
// CREATE INDEX idx_position_nft_events_slot      ON position_nft_events(slot);

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
} from "@percolatorct/shared";

const logger = createLogger("indexer:nft-indexer");

function readPositiveIntEnv(name: string, fallback: number, max?: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) return fallback;
  return max ? Math.min(parsed, max) : parsed;
}

/**
 * v17 wrapper tag for NFT B-3 ownership transfer.
 * Tag 72 = TransferPortfolioOwnership (wrapper program).
 *
 * v17 tag collision audit:
 *   IX_TAG.MintPositionNft = 64 (DEPRECATED) — collides with ForceCloseAbandonedAsset=64.
 *     Indexing tag 64 in v17 would misclassify force-close txs as NFT mints. REMOVED.
 *   IX_TAG.BurnPositionNft = 66 (DEPRECATED) — collides with BatchTradeNoCpi=66.
 *     Indexing tag 66 in v17 would misclassify batch-trade fills as NFT burns. REMOVED.
 *   IX_TAG.TransferPortfolioOwnership = 72 — the canonical v17 NFT B-3 ownership change.
 *     This is issued by the wrapper program when portfolio ownership (= NFT holder) changes.
 *
 * event_type for TransferPortfolioOwnership is stored as "transfer" (schema allows
 * 'mint'|'burn'|'transfer').  Existing v12 rows with 'mint'/'burn' are not migrated.
 */
const NFT_TAGS = new Set<number>([IX_TAG.TransferPortfolioOwnership]); // tag 72

/** How many recent signatures to fetch per slab per cycle */
const MAX_SIGNATURES = readPositiveIntEnv("NFT_MAX_SIGNATURES", 50, 100);

/** Poll interval (5 minutes — backup/backfill only, primary is webhook) */
const POLL_INTERVAL_MS = readPositiveIntEnv("NFT_POLL_INTERVAL_MS", 5 * 60_000);

/** Initial backfill: fetch more signatures on first run */
const BACKFILL_SIGNATURES = readPositiveIntEnv("NFT_BACKFILL_SIGNATURES", 100, 100);

/** Helius historical-data batch cap is 100. Keep ours lower to avoid bursty catches. */
const TX_BATCH_SIZE = readPositiveIntEnv("INDEXER_TX_BATCH_SIZE", 10, 100);
const TX_FETCH_RETRIES = readPositiveIntEnv("INDEXER_TX_FETCH_RETRIES", 2, 5);
const STARTUP_BACKFILL_ENABLED = process.env.INDEXER_STARTUP_BACKFILL_ENABLED !== "false";

/**
 * NftIndexerPolling — backup/backfill indexer for position NFT B-3 ownership transfer events.
 *
 * Tracks IX_TAG.TransferPortfolioOwnership (72) on the WRAPPER program.
 * Polls all active market slabs and upserts events to the `position_nft_events` table
 * with event_type = 'transfer'.
 *
 * v17 tag collision context (DO NOT revert to 64/66):
 *   Tag 64 = ForceCloseAbandonedAsset in v17 (was MintPositionNft in v12 — COLLISION)
 *   Tag 66 = BatchTradeNoCpi in v17 (was BurnPositionNft in v12 — COLLISION)
 *   Tag 72 = TransferPortfolioOwnership — the canonical v17 NFT B-3 path
 *
 * Account layout for TransferPortfolioOwnership (wrapper program):
 *   [0] from_owner/signer (current NFT holder / portfolio owner)
 *   [1] to_owner         (new owner)
 *   [2] slab (writable)
 *   [3+] additional accounts
 *
 * Data layout:
 *   TransferPortfolioOwnership (tag 72): tag(1) + user_idx(u16 LE = 2) — min 3 bytes
 */
export class NftIndexerPolling {
  /** Track last indexed signature per slab to avoid re-processing */
  private lastSignature = new Map<string, string>();
  private _running = false;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private hasBackfilled = false;
  private backfillAttempts = 0;

  start(): void {
    if (this._running) return;
    this._running = true;

    // Initial backfill after short delay to let discovery finish.
    if (STARTUP_BACKFILL_ENABLED) {
      setTimeout(() => this.backfill(), 5_000);
    } else {
      this.hasBackfilled = true;
    }

    // Start periodic polling
    this.pollTimer = setInterval(() => this.pollAllMarkets(), POLL_INTERVAL_MS);

    logger.info("NftIndexerPolling started (backup mode)", {
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
    logger.info("NftIndexer stopped");
  }

  /**
   * Backfill: fetch recent NFT events for all known market slabs on startup.
   */
  private async backfill(): Promise<void> {
    if (this.hasBackfilled || !this._running) return;

    try {
      const markets = await getMarkets();
      if (markets.length === 0) {
        logger.info("No markets found for NFT backfill");
        this.hasBackfilled = true;
        return;
      }

      logger.info("Starting NFT backfill", { marketCount: markets.length });
      for (const market of markets) {
        if (!this._running) break;
        try {
          await this.indexNftEventsForSlab(market.slab_address, BACKFILL_SIGNATURES);
        } catch (err) {
          logger.error("Backfill error", {
            slabAddress: market.slab_address.slice(0, 8),
            error: err instanceof Error ? err.message : err,
          });
          captureException(err, {
            tags: {
              context: "nft-indexer-backfill",
              slabAddress: market.slab_address,
            },
          });
        }
        await sleep(1_000);
      }
      this.hasBackfilled = true;
      logger.info("NFT backfill complete");
    } catch (err) {
      logger.error("Backfill failed", { error: err instanceof Error ? err.message : err });
      captureException(err, {
        tags: { context: "nft-indexer-backfill" },
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
   * Poll all active market slabs for new NFT events.
   */
  private async pollAllMarkets(): Promise<void> {
    if (!this._running) return;

    try {
      const markets = await getMarkets();
      for (const market of markets) {
        if (!this._running) break;
        try {
          await this.indexNftEventsForSlab(market.slab_address, MAX_SIGNATURES);
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
        tags: { context: "nft-indexer-poll" },
      });
    }
  }

  private async indexNftEventsForSlab(slabAddress: string, maxSigs = MAX_SIGNATURES): Promise<void> {
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
      logger.info("NFT events indexed", { count: indexed, slabAddress: slabAddress.slice(0, 8) });
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
      if (!NFT_TAGS.has(tag)) continue;

      // TransferPortfolioOwnership carries user_idx as u16 LE at data[1..3]
      if (data.length < 3) continue;

      const userIdx = data[1] | (data[2] << 8); // u16 little-endian

      // Account layout (v17 wrapper TransferPortfolioOwnership):
      //   [0] from_owner/signer, [1] to_owner, [2] slab (writable)
      const owner = ix.accounts[0]?.toBase58();    // from_owner
      const accountSlab = ix.accounts[2]?.toBase58();

      if (!owner || !accountSlab) continue;

      // accountSlab should match the slab we're polling — sanity-check
      if (accountSlab !== slabAddress) continue;

      // v17: all NFT events are portfolio ownership transfers
      const eventType = "transfer" as const;

      await this.upsertNftEvent({
        signature,
        event_type: eventType,
        slab: slabAddress,
        user_idx: userIdx,
        owner,
        slot,
        timestamp,
      });

      return true;
    }

    return false;
  }

  private async upsertNftEvent(event: {
    signature: string;
    event_type: "mint" | "burn" | "transfer"; // v17 adds "transfer" for TransferPortfolioOwnership
    slab: string;
    user_idx: number;
    owner: string;
    slot: number;
    timestamp: number;
  }): Promise<void> {
    const { error } = await getSupabase()
      .from("position_nft_events")
      .upsert(
        {
          signature: event.signature,
          event_type: event.event_type,
          slab: event.slab,
          user_idx: event.user_idx,
          owner: event.owner,
          slot: event.slot,
          timestamp: event.timestamp,
          network: getNetwork(),
        },
        { onConflict: "signature" }
      );

    if (error) {
      // Duplicate inserts are expected when polling overlaps — not an error
      if (error.message.includes("23505") || error.message.toLowerCase().includes("duplicate")) {
        logger.debug("Duplicate NFT event insert skipped", { signature: event.signature.slice(0, 12) });
        return;
      }
      throw new Error(`Failed to upsert NFT event: ${error.message}`);
    }

    logger.debug("NFT event upserted", {
      eventType: event.event_type,
      owner: event.owner.slice(0, 8),
      slab: event.slab.slice(0, 8),
      signature: event.signature.slice(0, 12),
    });
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
