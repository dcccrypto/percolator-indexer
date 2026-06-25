// SQL Schema:
// -- liquidations: id, signature UNIQUE, slab, user_idx, asset_index, slot, timestamp, network
//
// CREATE TABLE IF NOT EXISTS liquidations (
//   id         BIGSERIAL PRIMARY KEY,
//   signature  TEXT NOT NULL UNIQUE,
//   slab       TEXT NOT NULL,
//   user_idx   INTEGER,
//   asset_index INTEGER,
//   slot       BIGINT NOT NULL,
//   timestamp  TIMESTAMPTZ NOT NULL,
//   network    TEXT NOT NULL DEFAULT 'devnet',
//   created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
// );
// CREATE INDEX IF NOT EXISTS idx_liquidations_slab ON liquidations(slab);
// CREATE INDEX IF NOT EXISTS idx_liquidations_slot ON liquidations(slot);
// CREATE INDEX IF NOT EXISTS idx_liquidations_network ON liquidations(network);

import { PublicKey, type ParsedTransactionWithMeta } from "@solana/web3.js";
import { IX_TAG, CrankAction } from "@percolatorct/sdk";
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

const logger = createLogger("indexer:liquidation-indexer");

function readPositiveIntEnv(name: string, fallback: number, max?: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) return fallback;
  return max ? Math.min(parsed, max) : parsed;
}

/**
 * v17 PermissionlessCrank (tag 5) wire layout:
 *   tag(1) + action(u8) + asset_index(u16) + now_slot(u64) +
 *   funding_rate_e9(i128=16) + close_q(u128=16) + fee_bps(u64) +
 *   recovery_reason(u8) = 47 bytes.
 *
 * CrankAction.Liquidate = 1 (byte at offset 1).
 * asset_index at offset 2 (u16 LE) — identifies the portfolio/position being liquidated.
 *
 * Slab address: read from ix.accounts[1] (the slab/market-group account).
 */
const CRANK_TAG = IX_TAG.PermissionlessCrank; // tag 5
const LIQUIDATE_ACTION = CrankAction.Liquidate; // action byte = 1

// Minimum bytes to read: tag(1) + action(1) + asset_index(2) = 4 bytes
const MIN_CRANK_BYTES = 4;

/** How many recent signatures to fetch per slab per cycle */
const MAX_SIGNATURES = readPositiveIntEnv("LIQ_MAX_SIGNATURES", 50, 100);

/** Poll interval (5 minutes — backup/backfill only) */
const POLL_INTERVAL_MS = readPositiveIntEnv("LIQ_POLL_INTERVAL_MS", 5 * 60_000);

/** Initial backfill: fetch more signatures on first run */
const BACKFILL_SIGNATURES = readPositiveIntEnv("LIQ_BACKFILL_SIGNATURES", 100, 100);

/** Helius historical-data batch cap is 100. Keep lower to avoid bursty 429s. */
const TX_BATCH_SIZE = readPositiveIntEnv("INDEXER_TX_BATCH_SIZE", 10, 100);
const TX_FETCH_RETRIES = readPositiveIntEnv("INDEXER_TX_FETCH_RETRIES", 2, 5);
const STARTUP_BACKFILL_ENABLED = process.env.INDEXER_STARTUP_BACKFILL_ENABLED !== "false";

/**
 * LiquidationIndexerPolling — backup/backfill indexer for liquidation events.
 *
 * In v17, liquidations are PermissionlessCrank (tag 5) instructions with action=1.
 * This indexer polls all active market slabs for crank transactions and filters to
 * action=1 (Liquidate), persisting them to the `liquidations` table.
 *
 * Account layout for PermissionlessCrank (percolator-prog):
 *   [0] caller/signer (cranker/keeper)
 *   [1] slab (the perp market account — used to derive slab address for the event)
 *   [2+] additional accounts (portfolio being liquidated, oracle, etc.)
 *
 * The CREATE TABLE IF NOT EXISTS DDL runs in initialize() so no separate migration
 * is required — same pattern used by other indexers in this service.
 */
export class LiquidationIndexerPolling {
  private lastSignature = new Map<string, string>();
  private _running = false;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private hasBackfilled = false;
  private backfillAttempts = 0;

  async initialize(): Promise<void> {
    // Ensure the liquidations table exists. Uses IF NOT EXISTS so it's idempotent.
    // Supabase/Postgres: run via rpc or raw query. We use the Supabase client's
    // rpc("sql", ...) call which executes arbitrary SQL.
    const { error } = await getSupabase().rpc("exec_sql", {
      sql: `
        CREATE TABLE IF NOT EXISTS liquidations (
          id          BIGSERIAL PRIMARY KEY,
          signature   TEXT NOT NULL UNIQUE,
          slab        TEXT NOT NULL,
          user_idx    INTEGER,
          asset_index INTEGER,
          slot        BIGINT NOT NULL,
          timestamp   TIMESTAMPTZ NOT NULL,
          network     TEXT NOT NULL DEFAULT 'devnet',
          created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_liquidations_slab    ON liquidations(slab);
        CREATE INDEX IF NOT EXISTS idx_liquidations_slot    ON liquidations(slot);
        CREATE INDEX IF NOT EXISTS idx_liquidations_network ON liquidations(network);
      `,
    });

    if (error) {
      // exec_sql RPC may not exist — log and continue. The table should be
      // created via migration before this runs in production.
      logger.warn("LiquidationIndexer: could not auto-create liquidations table via exec_sql RPC", {
        error: error.message,
        hint: "Apply the DDL from the SQL Schema comment at the top of LiquidationIndexer.ts manually.",
      });
    } else {
      logger.info("LiquidationIndexer: liquidations table ensured");
    }
  }

  start(): void {
    if (this._running) return;
    this._running = true;

    if (STARTUP_BACKFILL_ENABLED) {
      setTimeout(() => this.backfill(), 5_000);
    } else {
      this.hasBackfilled = true;
    }

    this.pollTimer = setInterval(() => this.pollAllMarkets(), POLL_INTERVAL_MS);

    logger.info("LiquidationIndexerPolling started (backup mode)", {
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
    logger.info("LiquidationIndexer stopped");
  }

  private async backfill(): Promise<void> {
    if (this.hasBackfilled || !this._running) return;

    try {
      const markets = await getMarkets();
      if (markets.length === 0) {
        logger.info("No markets found for liquidation backfill");
        this.hasBackfilled = true;
        return;
      }

      logger.info("Starting liquidation backfill", { marketCount: markets.length });
      for (const market of markets) {
        if (!this._running) break;
        try {
          await this.indexLiquidationsForSlab(market.slab_address, BACKFILL_SIGNATURES);
        } catch (err) {
          logger.error("Backfill error", {
            slabAddress: market.slab_address.slice(0, 8),
            error: err instanceof Error ? err.message : err,
          });
          captureException(err, {
            tags: { context: "liquidation-indexer-backfill", slabAddress: market.slab_address },
          });
        }
        await sleep(1_000);
      }
      this.hasBackfilled = true;
      logger.info("Liquidation backfill complete");
    } catch (err) {
      logger.error("Backfill failed", { error: err instanceof Error ? err.message : err });
      captureException(err, { tags: { context: "liquidation-indexer-backfill" } });
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
          await this.indexLiquidationsForSlab(market.slab_address, MAX_SIGNATURES);
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
      captureException(err, { tags: { context: "liquidation-indexer-poll" } });
    }
  }

  private async indexLiquidationsForSlab(slabAddress: string, maxSigs = MAX_SIGNATURES): Promise<void> {
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
        { maxRetries: 3, baseDelayMs: 1000, label: `getSignaturesForAddress(${slabAddress.slice(0, 8)})` }
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
        { maxRetries: TX_FETCH_RETRIES, baseDelayMs: 1000, label: `getParsedTransactions(${signatures.length})` },
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
      logger.info("Liquidations indexed", { count: indexed, slabAddress: slabAddress.slice(0, 8) });
    }
  }

  private async processTransaction(
    tx: ParsedTransactionWithMeta,
    signature: string,
    slabAddress: string,
    programIds: Set<string>,
    slot: number,
    blockTime: number,
  ): Promise<boolean> {
    if (!tx.meta || tx.meta.err) return false;

    const message = tx.transaction.message;

    for (const ix of message.instructions) {
      if ("parsed" in ix) continue;

      const programId = ix.programId.toBase58();
      if (!programIds.has(programId)) continue;

      const data = decodeBase58(ix.data);
      if (!data || data.length < MIN_CRANK_BYTES) continue;

      const tag = data[0];
      if (tag !== CRANK_TAG) continue;

      const action = data[1];
      if (action !== LIQUIDATE_ACTION) continue;

      // asset_index: u16 LE at offset 2 (identifies the portfolio/domain being liquidated)
      const assetIndex = data[2] | (data[3] << 8);

      // Slab is at ix.accounts[1] — verify it matches the slab we're polling
      const ixSlab = ix.accounts[1]?.toBase58();
      if (!ixSlab || ixSlab !== slabAddress) continue;

      const timestamp = new Date(blockTime * 1000).toISOString();

      const { error } = await getSupabase()
        .from("liquidations")
        .upsert(
          {
            signature,
            slab: slabAddress,
            user_idx: null,      // not directly in the ix payload; would require reading portfolio acct
            asset_index: assetIndex,
            slot,
            timestamp,
            network: getNetwork(),
          },
          { onConflict: "signature" }
        );

      if (error) {
        if (error.message.includes("23505") || error.message.toLowerCase().includes("duplicate")) {
          logger.debug("Duplicate liquidation insert skipped", { signature: signature.slice(0, 12) });
          return false;
        }
        throw new Error(`Failed to upsert liquidation: ${error.message}`);
      }

      logger.debug("Liquidation indexed", {
        slab: slabAddress.slice(0, 8),
        assetIndex,
        slot,
        signature: signature.slice(0, 12),
      });

      return true;
    }

    return false;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
