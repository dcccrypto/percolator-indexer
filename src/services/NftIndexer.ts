// SQL Schema:
// -- position_nft_events: id, signature UNIQUE, event_type, slab, user_idx, owner, nft_mint, slot, timestamp, network
//
// CREATE TABLE position_nft_events (
//   id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//   signature  TEXT NOT NULL UNIQUE,
//   event_type TEXT NOT NULL CHECK (event_type IN ('mint', 'burn')),
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
} from "@percolator/shared";

const logger = createLogger("indexer:nft-indexer");

/**
 * Percolator-prog tags for NFT operations.
 * Tag 64 = MintPositionNft, Tag 66 = BurnPositionNft.
 * These are main-program (percolator-prog) instructions, not NFT-program instructions.
 */
const NFT_TAGS = new Set<number>([IX_TAG.MintPositionNft, IX_TAG.BurnPositionNft]);

/** How many recent signatures to fetch per slab per cycle */
const MAX_SIGNATURES = 50;

/** Poll interval (5 minutes — backup/backfill only, primary is webhook) */
const POLL_INTERVAL_MS = 5 * 60_000;

/** Initial backfill: fetch more signatures on first run */
const BACKFILL_SIGNATURES = 100;

/**
 * NftIndexerPolling — backup/backfill indexer for position NFT mint/burn events.
 *
 * Tracks IX_TAG.MintPositionNft (64) and IX_TAG.BurnPositionNft (66) on
 * percolator-prog. Polls all active market slabs and upserts events to the
 * `position_nft_events` table.
 *
 * Account layout for both instructions (percolator-prog):
 *   [0] owner/signer (position owner for mint, NFT holder for burn)
 *   [1] slab (writable — the perp market slab account)
 *   [2+] additional accounts (NFT mint, ATAs, etc.)
 *
 * Data layout:
 *   MintPositionNft (tag 64): tag(1) + user_idx(u16 LE = 2) — min 3 bytes
 *   BurnPositionNft (tag 66): tag(1) + user_idx(u16 LE = 2) — min 3 bytes
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

    // Initial backfill after short delay to let discovery finish
    setTimeout(() => this.backfill(), 5_000);

    // Start periodic polling
    this.pollTimer = setInterval(() => this.pollAllMarkets(), POLL_INTERVAL_MS);

    logger.info("NftIndexerPolling started (backup mode)", { intervalMs: POLL_INTERVAL_MS });
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

      // Both MintPositionNft and BurnPositionNft carry user_idx as u16 LE at data[1..3]
      if (data.length < 3) continue;

      const userIdx = data[1] | (data[2] << 8); // u16 little-endian

      // Account layout: [0] = owner/signer, [1] = slab
      const owner = ix.accounts[0]?.toBase58();
      const accountSlab = ix.accounts[1]?.toBase58();

      if (!owner || !accountSlab) continue;

      // accountSlab should match the slab we're polling — sanity-check
      if (accountSlab !== slabAddress) continue;

      const eventType: "mint" | "burn" =
        tag === IX_TAG.MintPositionNft ? "mint" : "burn";

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
    event_type: "mint" | "burn";
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
