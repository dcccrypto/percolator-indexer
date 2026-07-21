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

import { PublicKey, type AccountInfo, type ParsedTransactionWithMeta } from "@solana/web3.js";
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

/**
 * #165: the portfolio fetch could not be resolved for a TRANSIENT reason (RPC
 * error persisting through retries), as opposed to a deterministic one (account
 * absent, data too short, slab mismatch).
 *
 * The distinction drives cursor handling. A deterministic skip is final — the
 * next poll would reach the same conclusion, so holding the cursor would spin
 * forever. A transient skip is not: advancing past it drops the NFT
 * ownership-transfer event permanently, because the signature falls below the
 * cursor and is never re-fetched.
 */
export class PortfolioFetchUnavailableError extends Error {
  constructor(readonly portfolio: string, cause: unknown) {
    super(
      `portfolio ${portfolio} could not be fetched after retries: ` +
        (cause instanceof Error ? cause.message : String(cause)),
    );
    this.name = "PortfolioFetchUnavailableError";
  }
}

function readPositiveIntEnv(name: string, fallback: number, max?: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    logger.warn(`Invalid env var ${name}: "${raw}". Falling back to default: ${fallback}`, { raw });
    return fallback;
  }
  const val = max ? Math.min(parsed, max) : parsed;
  if (max && parsed > max) {
    logger.warn(`Env var ${name}: "${raw}" exceeds max limit ${max}. Clamped to ${val}`, { raw });
  }
  return val;
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
 * v17 account layout for TransferPortfolioOwnership (wrapper program) — desync fix 7:
 *   [0] mint_auth (NFT program's mint-authority PDA, signer — NOT the user wallet)
 *   [1] portfolio (writable)
 *   [2] nft_registry (read-only PDA)
 *   [3+] additional accounts
 *
 * v17 wire data format — desync fix 7:
 *   tag(1) + new_owner([u8;32]) + asset_index(u16 LE) = 35 bytes minimum
 *   The new_owner pubkey at data[1:33] identifies who becomes the portfolio owner.
 *   The 16-bit value at data[33:35] is asset_index (NOT user_idx as the old comment stated).
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
        // #111: don't mark backfill complete with no markets — discovery may still be running on
        // cold start. Return without the flag so pollAllMarkets re-triggers backfill once markets appear.
        logger.info("No markets found for NFT backfill yet — will retry next cycle");
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

    // #111: re-trigger the startup backfill if it hasn't completed (discovery may not have
    // populated markets when it first ran). backfill()'s guard makes this a no-op once done.
    if (!this.hasBackfilled) {
      await this.backfill();
    }

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
          captureException(err, {
            tags: {
              context: "nft-indexer-poll",
              slabAddress: market.slab_address,
            },
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

    // Filter out errored transactions; retain slot/blockTime for later
    const validSigInfos = sigInfos.filter(s => !s.err);
    if (validSigInfos.length === 0) {
      // If there are no valid signatures, we can safely advance the cursor
      this.lastSignature.set(slabAddress, sigInfos[0].signature);
      return;
    }

    let indexed = 0;
    let cursorHeld = false;

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
          if (err instanceof PortfolioFetchUnavailableError) {
            // #165: transient — stop and leave the cursor where it is so this
            // transaction is re-fetched next poll. Mirrors the #147 fix in
            // TradeIndexer. Advancing here drops the event permanently.
            logger.warn("Portfolio fetch unavailable — cursor not advanced, will retry next poll", {
              signature: sigInfo.signature.slice(0, 12),
              slabAddress: slabAddress.slice(0, 8),
              error: err.message,
            });
            cursorHeld = true;
            break;
          }
          logger.warn("Failed to process transaction", {
            signature: sigInfo.signature.slice(0, 12),
            error: err instanceof Error ? err.message : err,
          });
        }
      }
      if (cursorHeld) break;
    }

    // Update last signature (most recent first) after successful processing (M-2).
    // #165: skipped when a transient portfolio fetch failure held the cursor.
    if (!cursorHeld) {
      this.lastSignature.set(slabAddress, sigInfos[0].signature);
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

      // v17 wire decode — desync fix 7:
      //   tag(1) + new_owner([u8;32]) + asset_index(u16 LE) = 35 bytes minimum
      if (data.length < 35) continue;

      // new_owner is the recipient pubkey at data[1:33]
      const newOwnerBytes = data.slice(1, 33);
      const newOwnerPk = new PublicKey(newOwnerBytes).toBase58();

      // asset_index (u16 LE) at data[33:35]
      const assetIndex = data[33] | (data[34] << 8);

      // v17 account layout (desync fix 7):
      //   [0] mint_auth (PDA signer — NOT the user wallet)
      //   [1] portfolio (writable)
      //   [2] nft_registry (PDA)
      // The portfolio's new owner comes from the wire data new_owner field, not accounts[].
      // The nft_registry at accounts[2] is NOT the market slab — we match on portfolio at [1].
      const portfolio = ix.accounts[1]?.toBase58();

      if (!newOwnerPk || !portfolio) continue;

      // To prevent multi-slab attribution errors (L-3 / #134), fetch the portfolio
      // account to derive the correct slab address from its on-chain state.
      // v17 portfolio accounts store the market group (slab) address at offset 16.
      //
      // SEC (#134): on null response or RPC error we MUST skip this record.
      // The original code left `actualSlab = slabAddress` on error, so the
      // event would be written attributed to the poll-loop slab even though we
      // could not verify it. An unverified attribution is wrong attribution.
      // #165: this was a bare getAccountInfo — the only RPC call in the indexer
      // without a retry wrapper. A single transient blip skipped the event while
      // the cursor advanced past it anyway, dropping it permanently.
      let actualSlab: string | null = null;
      let portfolioInfo: AccountInfo<Buffer> | null;
      try {
        const connection = getConnection();
        portfolioInfo = await withRetry(
          () => connection.getAccountInfo(new PublicKey(portfolio)),
          {
            maxRetries: 3,
            baseDelayMs: 1000,
            label: `getAccountInfo(portfolio ${portfolio.slice(0, 8)})`,
          },
        );
      } catch (err) {
        // Still failing after retries: TRANSIENT. Propagate so the caller holds
        // the cursor and this transaction is re-fetched next poll, rather than
        // silently dropping the event.
        throw new PortfolioFetchUnavailableError(portfolio, err);
      }

      if (portfolioInfo?.data) {
        const portfolioData = new Uint8Array(portfolioInfo.data);
        if (portfolioData.length >= 48) {
          actualSlab = new PublicKey(portfolioData.subarray(16, 48)).toBase58();
        } else {
          // Deterministic: re-fetching cannot lengthen the account. Skip and let
          // the cursor advance.
          logger.warn("Portfolio account data too short to read slab — skipping NFT event", {
            portfolio,
            dataLen: portfolioData.length,
          });
        }
      } else {
        // Deterministic: account absent. Slab cannot be verified; skip.
        logger.warn("Portfolio account not found on-chain — skipping NFT event", { portfolio });
      }

      // Skip if we could not determine the actual slab OR if it doesn't match
      // the slab we are currently polling (wrong-market guard).
      if (actualSlab === null || actualSlab !== slabAddress) continue;

      // v17: all NFT events are portfolio ownership transfers
      const eventType = "transfer" as const;

      await this.upsertNftEvent({
        signature,
        event_type: eventType,
        slab: slabAddress,
        user_idx: assetIndex,   // v17: asset_index used in user_idx column (schema compatible)
        owner: newOwnerPk,      // v17: new_owner from wire data
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
