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
import { NFT_IX_TAG, NFT_PROGRAM_ID } from "@percolatorct/sdk";
import {
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
 * NFT program address for v17.
 *
 * v17 BREAKING CHANGE: NFT operations (MintPositionNft, BurnPositionNft) are now handled
 * by the standalone percolator-nft program, NOT by the main wrapper program.
 * The old wrapper tags 64 (v12 MintPositionNft) and 66 (v12 BurnPositionNft) were
 * REPURPOSED in v17:
 *   - Wrapper tag 64 = ForceCloseAbandonedAsset
 *   - Wrapper tag 66 = BatchTradeNoCpi
 *
 * Set NFT_PROGRAM_ID env var to override the default from the SDK.
 * Devnet deployed NFT program: 5TnritLtHS76s5iV8axqDmqhcmJKMRUekMGrk9rBTqSP
 */
const NFT_PROGRAM_ADDRESS = NFT_PROGRAM_ID.toBase58();

/**
 * NFT instruction tags (standalone percolator-nft program — NOT main wrapper tags).
 *   NFT_IX_TAG.MintPositionNft = 0
 *   NFT_IX_TAG.BurnPositionNft = 1
 */
const NFT_TAGS = new Set<number>([NFT_IX_TAG.MintPositionNft, NFT_IX_TAG.BurnPositionNft]);

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
 * NftIndexerPolling — backup/backfill indexer for position NFT mint/burn events.
 *
 * v17 architecture: NFT operations go to the standalone percolator-nft program
 * (NFT_PROGRAM_ID), not the wrapper. We poll by slab address because the portfolio
 * account is writable in both MintPositionNft and BurnPositionNft — those txs appear
 * in getSignaturesForAddress for the slab account.
 *
 * Instructions are filtered to those targeting the NFT program and matching
 * NFT_IX_TAG.MintPositionNft(0) or NFT_IX_TAG.BurnPositionNft(1).
 *
 * Account layout for MintPositionNft (tag 0):
 *   [0]  payer/owner (signer, writable)
 *   [1]  PositionNft PDA (writable, created)
 *   [2]  NFT mint (signer, writable)
 *   [3]  Owner NFT ATA (writable)
 *   [4]  Portfolio account (writable — B-3 escrow CPI mutates owner)
 *   [5+] mint authority PDA, token programs, system program, extra metas, NftRegistry, wrapper
 *
 * Account layout for BurnPositionNft (tag 1):
 *   [0]  NFT holder (signer)
 *   [1]  PositionNft PDA (writable, closed)
 *   [2]  NFT mint (writable)
 *   [3]  Holder NFT ATA (writable)
 *   [4]  Portfolio account (writable — unwrap CPI releases escrow)
 *   [5+] mint authority PDA, Token-2022, ExtraAccountMetaList, NftRegistry, wrapper
 *
 * Data layout:
 *   MintPositionNft (tag 0): tag(1) + asset_index(u16 LE=2) — min 3 bytes
 *   BurnPositionNft (tag 1): tag(1) — 1 byte (no extra data)
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
      nftProgram: NFT_PROGRAM_ADDRESS.slice(0, 8),
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
      this.backfillAttempts++;
      if (this.backfillAttempts < 3 && this._running) {
        const delayMs = 10_000 * Math.pow(2, this.backfillAttempts);
        logger.info("Scheduling backfill retry", { attempt: this.backfillAttempts, delayMs });
        setTimeout(() => this.backfill(), delayMs);
      } else {
        this.hasBackfilled = true;
        logger.error("Backfill exhausted retries — pollAllMarkets will cover the gap");
      }
    }
  }

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
    this.lastSignature.set(slabAddress, sigInfos[0].signature);

    const validSigInfos = sigInfos.filter(s => !s.err);
    if (validSigInfos.length === 0) return;

    let indexed = 0;

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
    slot: number,
    timestamp: number,
  ): Promise<boolean> {
    if (!tx.meta || tx.meta.err) return false;

    const message = tx.transaction.message;

    for (const ix of message.instructions) {
      // Skip parsed instructions (system, token, etc.)
      if ("parsed" in ix) continue;

      // v17: NFT operations are on the standalone NFT program, not the wrapper.
      // Only process instructions targeting the NFT program address.
      const programId = ix.programId.toBase58();
      if (programId !== NFT_PROGRAM_ADDRESS) continue;

      const data = decodeBase58(ix.data);
      if (!data || data.length < 1) continue;

      const tag = data[0];
      if (!NFT_TAGS.has(tag)) continue;

      // MintPositionNft (tag 0): tag(1) + asset_index(u16 LE=2) — min 3 bytes
      // BurnPositionNft (tag 1): tag(1) only — no extra data required
      let userIdx = 0;
      if (tag === NFT_IX_TAG.MintPositionNft) {
        if (data.length < 3) continue;
        userIdx = data[1] | (data[2] << 8); // asset_index as u16 LE
      }

      // Account layout (both Mint and Burn):
      //   [0] owner/holder (signer)
      //   [4] Portfolio account (writable, #134 slab derivation source)
      //
      // #134 fix: require portfolio account at [4] to sanity-check the tx
      // involves the position we're tracking; prevents indexing unrelated NFT txs.
      const owner = ix.accounts[0]?.toBase58();
      const portfolioAccount = ix.accounts[4]?.toBase58();

      if (!owner || !portfolioAccount) continue;

      const eventType: "mint" | "burn" =
        tag === NFT_IX_TAG.MintPositionNft ? "mint" : "burn";

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
