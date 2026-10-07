import { breakerAlertPolls, createBreakerTracker, fetchParsedTxsTolerant } from "../lib/tolerantTxFetch.js";
import { recordSkippedSignatures } from "../lib/skippedSignatures.js";
import { Connection, PublicKey, type ParsedTransactionWithMeta } from "@solana/web3.js";
import { IX_TAG, detectSlabLayout, isV17Account, parseWrapperConfigV17, V17_HEADER_LEN } from "@percolatorct/sdk";
import { config, getConnection, getMarkets, eventBus, decodeBase58, withRetry, createLogger, captureException } from "@percolatorct/shared";
import { insertTradeRow } from "../db/insertTradeRow.js";
import {
  parsePercolatorLiquidations,
  decodeV18SingleFill,
  decodeV18BatchLegs,
  computeFeeUsd,
  isRebalanceReduceTag,
  decodeRebalanceReduce,
  REBALANCE_REDUCE_MARKET_ACCOUNT_IDX,
} from "../parsers/percolatorTxParser.js";
import { resolveRebalanceReduce } from "../db/traderNetPosition.js";
import { fetchStoredLegs, fetchStoredLegsMany, fillAlreadyStored, cpiFillAlreadyStored, reduceAlreadyStored, type StoredLeg } from "../db/storedLegs.js";
import { cpiEvidenceFromParsed } from "../parsers/percolatorTxParser.js";
import { resolveCpiLeg } from "../parsers/matcherFill.js";
import { readAssetEffectivePriceE6 } from "../parsers/markPrice.js";
import { makeMatcherContextReader } from "../lib/matcherCtx.js";

const logger = createLogger("indexer:trade-indexer");

/** The matcher-context read failed in transport; this signature is retried once before falling back. */
class CtxReadRetry extends Error {
  constructor(readonly signature: string) { super("matcher context read failed; signature will be retried"); }
}

/** Alert (error log + Sentry) when a slab's cursor has been held by the mass-skip breaker for K consecutive polls. */
const breakerTracker = createBreakerTracker(breakerAlertPolls(), (slab, polls) => {
  logger.error("ALERT: cursor held by the mass-skip circuit breaker for consecutive polls: signatures are unreadable and are NOT being skipped; investigate the RPC / reader", { slab, consecutivePolls: polls });
  captureException(new Error(`indexer cursor held by mass-skip breaker for ${polls} consecutive polls (slab ${slab})`), { tags: { context: "indexer-breaker-held" }, extra: { slab, polls } });
});

/**
 * v18 trade tags to index.
 *
 * TradeCpiV2 (alias TradeCpiV=105) is NOT a valid v18 wrapper instruction — removed.
 * BatchTradeNoCpi (66) and BatchTradeCpi (67) emit fills and are included.
 */
const TRADE_TAGS = new Set<number>([
  IX_TAG.TradeNoCpi,      // 6
  IX_TAG.TradeCpi,        // 10
  IX_TAG.BatchTradeNoCpi, // 66
  IX_TAG.BatchTradeCpi,   // 67
]);

function readPositiveIntEnv(name: string, fallback: number, max?: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 0) return fallback;
  return max ? Math.min(parsed, max) : parsed;
}

/** How many recent signatures to fetch per slab per cycle */
const MAX_SIGNATURES = readPositiveIntEnv("TRADE_MAX_SIGNATURES", 50, 100);

/** Poll interval for trade indexing (5 minutes — backup/backfill only, primary is webhook) */
const POLL_INTERVAL_MS = readPositiveIntEnv("TRADE_POLL_INTERVAL_MS", 5 * 60_000);

/** Initial backfill: fetch more signatures on first run */
const BACKFILL_SIGNATURES = readPositiveIntEnv("TRADE_BACKFILL_SIGNATURES", 100, 100);

/** Helius historical-data batch cap is 100. Keep ours lower to avoid bursty catches. */
const TX_BATCH_SIZE = readPositiveIntEnv("INDEXER_TX_BATCH_SIZE", 10, 100);
const TX_FETCH_RETRIES = readPositiveIntEnv("INDEXER_TX_FETCH_RETRIES", 2, 5);
const STARTUP_BACKFILL_ENABLED = process.env.INDEXER_STARTUP_BACKFILL_ENABLED !== "false";

/**
 * TradeIndexerPolling — backup/backfill trade indexer using on-chain polling.
 *
 * Primary indexing is now webhook-driven (see HeliusWebhookManager + webhook routes).
 * This poller runs on startup for backfill, then every 5 minutes as a catchall.
 *
 * Polls all active markets periodically to catch any missed trades.
 */
export class TradeIndexerPolling {
  /** Track last indexed signature per slab to avoid re-processing */
  private lastSignature = new Map<string, string>();
  private _running = false;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  /** signature -> 1: its matcher-context read already failed once (see CtxReadRetry). Bounded. */
  private ctxReadAttempts = new Map<string, number>();
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

    logger.info("TradeIndexerPolling started (backup mode)", {
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
    logger.info("TradeIndexer stopped");
  }

  /**
   * Backfill: fetch recent trades for all known markets on startup
   */
  private async backfill(): Promise<void> {
    if (this.hasBackfilled || !this._running) return;

    try {
      const markets = await getMarkets();
      if (markets.length === 0) {
        // #111: do NOT mark backfill complete with no markets yet — discovery may still be
        // running on cold start. Return WITHOUT the flag so pollAllMarkets re-triggers backfill
        // once markets appear; otherwise pre-startup history is permanently missed.
        logger.info("No markets found for backfill yet — will retry next cycle");
        return;
      }

      logger.info("Starting trade backfill", { marketCount: markets.length });
      for (const market of markets) {
        if (!this._running) break;
        try {
          await this.indexTradesForSlab(market.slab_address, BACKFILL_SIGNATURES);
        } catch (err) {
          logger.error("Backfill error", {
            slabAddress: market.slab_address.slice(0, 8),
            error: err instanceof Error ? err.message : err
          });
          captureException(err, {
            tags: {
              context: "trade-indexer-backfill",
              slabAddress: market.slab_address,
            },
          });
        }
        // Small delay between markets to avoid rate limits
        await sleep(1_000);
      }
      this.hasBackfilled = true;
      logger.info("Trade backfill complete");
    } catch (err) {
      logger.error("Backfill failed", { error: err instanceof Error ? err.message : err });
      captureException(err, {
        tags: { context: "trade-indexer-backfill" },
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
   * Poll all active markets for new trades
   */
  private async pollAllMarkets(): Promise<void> {
    if (!this._running) return;

    // #111: re-trigger the startup backfill if it hasn't completed (market discovery may not
    // have populated markets when it first ran). backfill()'s own guard makes this a no-op once done.
    if (!this.hasBackfilled) {
      await this.backfill();
    }

    try {
      const markets = await getMarkets();
      for (const market of markets) {
        if (!this._running) break;
        try {
          await this.indexTradesForSlab(market.slab_address, MAX_SIGNATURES);
        } catch (err) {
          logger.error("Poll error", { 
            slabAddress: market.slab_address.slice(0, 8),
            error: err instanceof Error ? err.message : err
          });
        }
        // Small delay between markets
        await sleep(500);
      }
    } catch (err) {
      logger.error("Poll failed", { error: err instanceof Error ? err.message : err });
      captureException(err, {
        tags: { context: "trade-indexer-poll" },
      });
    }
  }

  /**
   * Debounce processing — cranks happen in batches, wait a bit
   * to collect all slabs before processing
   */
  private async indexTradesForSlab(slabAddress: string, maxSigs = MAX_SIGNATURES): Promise<void> {
    const connection = getConnection();
    const slabPk = new PublicKey(slabAddress);
    const programIds = new Set(config.allProgramIds);

    // Fetch recent signatures for this slab account
    const opts: { limit: number; until?: string } = { limit: maxSigs };
    const lastSig = this.lastSignature.get(slabAddress);
    if (lastSig) opts.until = lastSig;

    let signatures;
    try {
      signatures = await withRetry(
        () => connection.getSignaturesForAddress(slabPk, opts),
        { 
          maxRetries: 3, 
          baseDelayMs: 1000, 
          label: `getSignaturesForAddress(${slabAddress.slice(0, 8)})` 
        }
      );
    } catch (err) {
      logger.warn("Failed to get signatures after retries", {
        slabAddress: slabAddress.slice(0, 8),
        error: err instanceof Error ? err.message : err,
      });
      return;
    }

    if (signatures.length === 0) return;

    // #147: Do NOT advance the cursor here. We commit lastSignature only after
    // the entire batch is fetched and processed successfully. Advancing it before
    // getParsedTransactions means an RPC failure permanently skips those trades
    // (the cursor is already past them on the next poll).

    // Filter out errored transactions
    // getSignaturesForAddress is newest-first. Process OLDEST-first: a RebalanceReduce (tag 44) is
    // resolved against the trader's indexed fills, so every earlier transaction in this window must
    // be indexed before a later close is looked at (otherwise [sell 30, close 10] resolves the close
    // against the pre-sell position and writes the wrong side). The cursor still commits the newest
    // signature only after the whole window succeeded, and per-leg dedup makes a re-pass idempotent,
    // so the order does not change what is retried.
    const validSigs = signatures.filter(s => !s.err).map(s => s.signature).reverse();
    if (validSigs.length === 0) {
      // No valid txs in this window but signatures were fetched — safe to advance.
      this.lastSignature.set(slabAddress, signatures[0].signature);
      return;
    }

    let indexed = 0;
    let batchFailed = false;
    let holdCursor = false;
    const pass: { earlier: Set<string>; incomplete: boolean; stored?: Map<string, StoredLeg[]> | null } = { earlier: new Set<string>(), incomplete: false }; // oldest-first: everything in `earlier` precedes the current tx

    // Fetch transactions in Helius-supported historical batches instead of
    // parallel single-tx calls. This reduces request bursts and avoids 429 loops.
    for (let i = 0; i < validSigs.length; i += TX_BATCH_SIZE) {
      const batch = validSigs.slice(i, i + TX_BATCH_SIZE);
      // X-1: isolate an unreadable (e.g. v1-version) transaction instead of letting one poisoned signature
      // hold this slab's cursor forever. Transient RPC failures still hold the cursor (#147).
      const fetched = await fetchParsedTxsTolerant<ParsedTransactionWithMeta>(
        connection,
        batch,
        (fn, label) => withRetry(fn, { maxRetries: TX_FETCH_RETRIES, baseDelayMs: 1000, label }),
      );
      if (fetched.skipped.length > 0) {
        pass.incomplete = true; // an unreadable earlier tx may have changed a position unseen
        // Durable + loud: FULL signature and slab, counter, DB table (or JSONL fallback). Re-index from there.
        await recordSkippedSignatures(
          fetched.skipped.map((sk) => ({ signature: sk.signature, source: "trade-indexer" as const, slab: slabAddress, error: sk.error })),
        );
      }
      if (fetched.massSkip) breakerTracker.held(slabAddress);
      else if (!fetched.failed) breakerTracker.clear(slabAddress);
      if (fetched.massSkip) {
        logger.error("Mass-skip circuit breaker tripped: cursor held, nothing skipped (RPC or reader is broken, not a poison pill)", {
          slabAddress,
          skipped: fetched.massSkip.skipped,
          batch: fetched.massSkip.total,
        });
      }
      if (fetched.failed) {
        // #147: RPC failure — stop processing and do NOT advance the cursor.
        // The next poll will re-fetch from the same lastSignature and retry.
        logger.warn("getParsedTransactions failed — cursor not advanced, will retry next poll", {
          slabAddress: slabAddress.slice(0, 8),
          error: fetched.error instanceof Error ? fetched.error.message : fetched.error,
        });
        batchFailed = true;
        break;
      }
      const txs = fetched.txs;
      // One stored-legs query for the whole fetched batch (not one per transaction).
      pass.stored = await fetchStoredLegsMany(batch);

      for (let j = 0; j < txs.length; j++) {
        const tx = txs[j];
        if (!tx) { pass.incomplete = true; continue; }
        const sig = batch[j];

        try {
          const didIndex = await this.processTransaction(tx, sig, slabAddress, programIds, pass);
          if (didIndex) indexed++;
          pass.earlier.add(sig);
        } catch (err) {
          pass.incomplete = true;
          // Non-fatal: skip this tx, continue with others
          logger.warn("Failed to process transaction", {
            signature: sig.slice(0, 12),
            error: err instanceof Error ? err.message : err,
          });
        }
      }
    }

    // #147: Only commit the cursor after the full batch is fetched and processed.
    // If any batch fetch failed we leave the cursor in place so the next poll retries.
    if (!batchFailed && !holdCursor) {
      this.lastSignature.set(slabAddress, signatures[0].signature);
    }

    if (indexed > 0) {
      logger.info("Trades indexed", { count: indexed, slabAddress: slabAddress.slice(0, 8) });
    }
  }

  private async processTransaction(
    tx: ParsedTransactionWithMeta,
    signature: string,
    slabAddress: string,
    programIds: Set<string>,
    /**
     * Oldest-first pass state: signatures of this slab's window already indexed before this one, and
     * whether any earlier one could not be (failed / unreadable / skipped), which makes a tag 44 unresolvable.
     */
    pass: { earlier: Set<string>; incomplete: boolean; stored?: Map<string, StoredLeg[]> | null } = { earlier: new Set(), incomplete: false },
  ): Promise<boolean> {
    if (!tx.meta || tx.meta.err) return false;

    const message = tx.transaction.message;

    // Liquidation markers (crank action=1) for the slab being polled. No size/price/side;
    // excluded from volume/candles. leg_index offset (1000+) keeps clear of fill legs.
    const liqs = parsePercolatorLiquidations(tx, signature, Array.from(programIds))
      .filter((l) => l.slabAddress === slabAddress);
    for (const [i, liq] of liqs.entries()) {
      try {
        await insertTradeRow({
          slab_address: liq.slabAddress,
          trader: liq.portfolio,
          side: null,
          size: null,
          price: null,
          fee: 0,
          tx_signature: signature,
          asset_index: liq.assetIndex,
          leg_index: 1000 + i,
          is_liquidation: true,
        });
      } catch (err) {
        logger.warn("liquidation insert failed", { signature: signature.slice(0, 12), err: String(err) });
      }
    }

    // leg_index is the fill's position among ALL fills in the tx (every trade
    // instruction and every batch leg, 0-based) — the scheme webhook.ts (`fillSeq`)
    // and EventStreamService (index into the flattened parsePercolatorFills result)
    // already use. The dedup key (tx_signature, asset_index, leg_index) is shared
    // by all three paths, so this path must walk EVERY trade instruction and number
    // the same way: the playground sends an order larger than the matcher's
    // per-fill cap as several single-leg TradeCpi instructions in ONE tx.
    let fillSeq = 0;
    let insertedAny = false;
    // (trader|slab|asset) positions already touched by an earlier fill or tag 44 in this tx: a tag 44
    // after one of them starts from a position this indexer cannot see (the earlier fill is not in the
    // table yet, and a clipped earlier reduce has an unknown size), so it is not resolved.
    const touched = new Set<string>();
    // Rows already stored for this tx, read once and only if a fill needs it (F2 + old-numbering check).
    let stored: StoredLeg[] | null | undefined;
    const getStored = async (): Promise<StoredLeg[] | null> => {
      if (stored === undefined) stored = pass.stored?.get(signature) ?? (await fetchStoredLegs(signature));
      return stored;
    };
    // Leg numbers of this tx's tag 44s (their own inferred rows are not TradeCpi fills), by a dry walk
    // of the same numbering the loop below uses.
    const reduceLegs = new Set<number>();
    {
      let seq = 0;
      for (const ix of message.instructions) {
        if ("parsed" in ix || !programIds.has(ix.programId.toBase58())) continue;
        const d = decodeBase58(ix.data);
        if (!d || d.length < 1) continue;
        if (isRebalanceReduceTag(d[0])) { if (decodeRebalanceReduce(d)) reduceLegs.add(seq++); continue; }
        if (!TRADE_TAGS.has(d[0])) continue;
        seq += (d[0] === IX_TAG.BatchTradeNoCpi || d[0] === IX_TAG.BatchTradeCpi) ? decodeV18BatchLegs(d[0], d).length : decodeV18SingleFill(d[0], d) ? 1 : 0;
      }
    }
    // Occurrence rank of identical fills in this tx (a split order is identical legs).
    const identSeen = new Map<string, number>();
    const ordinalOf = (slab: string, asset: number, trader: string, side: string, size: string): number => {
      const k = `${slab}|${asset}|${trader}|${side}|${size}`;
      const n = (identSeen.get(k) ?? 0) + 1;
      identSeen.set(k, n);
      return n;
    };
    // Rank among this tx's TradeCpi legs of the same (slab, asset, trader, side), size-independent.
    const cpiSeen = new Map<string, number>();
    const cpiOrdinalOf = (slab: string, asset: number, trader: string, side: string): number => {
      const k = `${slab}|${asset}|${trader}|${side}`;
      const n = (cpiSeen.get(k) ?? 0) + 1;
      cpiSeen.set(k, n);
      return n;
    };
    // No transaction-level "already indexed?" gate: tradeExistsBySignature filters on
    // (signature, network) only, so after ONE leg is written (by any path, or by an
    // earlier pass that then threw on a later leg) it would hide every remaining leg
    // for good — the cursor advances past the tx. Dedup is per leg instead, via the
    // unique index (tx_signature, asset_index, leg_index): insertTradeRow swallows
    // 23505 and reports whether the row was new. Same model as the webhook batch path.
    //
    // Fallback price (slab read) resolved lazily, ONCE per transaction, shared by
    // every leg and instruction that lacks a wire exec_price. A tx that is already
    // fully indexed therefore costs at most one slab read, not one per leg.
    // #221: per ASSET. This is a LATEST-state read of the asset's booked effective_price (the poll
    // path has no tx post-state), so it is APPROXIMATE: it drifts with oracle pushes. Used only
    // where there is no matcher CPI to read the exact price from (tag 44, TradeNoCpi without a price).
    const fallbackPriceByAsset = new Map<number, number>();
    const resolveFallbackPrice = async (assetIndex: number): Promise<number> => {
      let price = fallbackPriceByAsset.get(assetIndex);
      if (price === undefined) {
        price = this.extractPriceFromLogs(tx);
        if (price === 0) {
          price = await this.readMarkPriceFromSlab(getConnection(), slabAddress, assetIndex);
        }
        fallbackPriceByAsset.set(assetIndex, price);
      }
      return price;
    };

    // TradeCpi / BatchTradeCpi: the instruction carries the REQUESTED size; the executed size and
    // the booked price come from the matcher call (see parsers/matcherFill.ts). Reader is only
    // used when a fill needs the matcher context account.
    // Context reads: 2 s per call. A transport error is REPORTED the first time (the signature is
    // retried on the next pass, cursor held) because a size written after a transient failure is
    // permanent (unique index); the retry then falls back to the post-clip request size.
    const readMatcherContext = makeMatcherContextReader(() => getConnection(), tx.slot);
    const onReadError: "report" | "fallback" = (this.ctxReadAttempts.get(signature) ?? 0) >= 1 ? "fallback" : "report";
    let retryNeeded = false;
    const returnDataRaw = (tx.meta as { returnData?: unknown }).returnData as { programId?: unknown; data?: unknown } | null | undefined;
    const returnData = returnDataRaw && Array.isArray(returnDataRaw.data) && typeof returnDataRaw.data[0] === "string"
      ? { programId: String(returnDataRaw.programId), data: Uint8Array.from(Buffer.from(returnDataRaw.data[0], "base64")) }
      : null;
    const unverified: string[] = [];

    for (const [ixIdx, ix] of message.instructions.entries()) {
      // Skip parsed instructions (system, token, etc.)
      if ("parsed" in ix) continue;

      // Check if this instruction is for one of our programs
      const programId = ix.programId.toBase58();
      if (!programIds.has(programId)) continue;

      // Decode instruction tag from data
      const data = decodeBase58(ix.data);
      if (!data || data.length < 1) continue;

      const tag = data[0];

      // RebalanceReduce (tag 44): how a position is closed while the asset is ADL
      // reduce-only. Not a TRADE_TAG (no counterparty, no side on the wire), but it is
      // the trader's close and must land in their history like a TradeCpi close.
      // It takes a slot in the tx-wide fill numbering (same counter as every other fill,
      // and the same slot webhook.ts / parsePercolatorFills give it), so a tag 44 and a
      // TradeCpi in one transaction can never share (tx_signature, asset_index, leg_index).
      if (isRebalanceReduceTag(tag)) {
        const reduce = decodeRebalanceReduce(data);
        if (!reduce) continue; // malformed / reduce_q 0: the program rejects it, no fill
        const legIndex = fillSeq++;
        const trader = ix.accounts[0]?.toBase58();
        if (ix.accounts[REBALANCE_REDUCE_MARKET_ACCOUNT_IDX]?.toBase58() !== slabAddress || !trader) continue;
        // NOT `return`: later instructions (other trades, a second tag 44 on another asset) still count.
        // F2: already stored (webhook redelivery, startup backfill)? Then do not resolve it again:
        // later rows would make it look "not provably earlier" and it would be reported as missing.
        if (reduceAlreadyStored(await getStored(), { slab: slabAddress, assetIndex: reduce.assetIndex, legIndex, trader, reduceQ: reduce.reduceQ })) continue;
        const reduceKey = `${trader}|${slabAddress}|${reduce.assetIndex}`;
        const resolution = await resolveRebalanceReduce({
          trader,
          slabAddress,
          assetIndex: reduce.assetIndex,
          reduceQ: reduce.reduceQ,
          signature,
          txTimeSec: tx.blockTime ?? null,
          repeatInTx: touched.has(reduceKey),
          earlierSignatures: pass.earlier,
          earlierIncomplete: pass.incomplete,
        });
        touched.add(reduceKey);
        if (!resolution.ok) {
          // Never a warn-only miss: durable + loud + retryable (skipped_signatures, #212).
          await recordSkippedSignatures([{ signature, source: "trade-indexer", slab: slabAddress, error: resolution.reason }]);
          continue;
        }
        const price = await resolveFallbackPrice(reduce.assetIndex); // tag 44: no matcher CPI, effective_price is its only exact source
        // The engine charges no trading fee on tag 44.
        const inserted = await insertTradeRow({
          slab_address: slabAddress,
          trader,
          side: resolution.side,
          size: resolution.sizeValue.toString(),
          price,
          fee: 0,
          tx_signature: signature,
          asset_index: reduce.assetIndex,
          leg_index: legIndex,
        });
        if (!inserted) continue; // duplicate leg: already indexed, no event, no count
        eventBus.publish("trade.executed", slabAddress, { signature, trader, side: resolution.side, size: resolution.sizeValue.toString() });
        insertedAny = true;
        continue;
      }

      if (!TRADE_TAGS.has(tag)) continue;

      // v18 single-fill / batch-fill decode lives in percolatorTxParser.ts
      // (decodeV18SingleFill / decodeV18BatchLegs) — see that file for the
      // exact byte layout. TradeNoCpi and TradeCpi have DIFFERENT single-fill
      // layouts in v18 (unlike v17, where they matched).
      const isBatch = (tag === IX_TAG.BatchTradeNoCpi || tag === IX_TAG.BatchTradeCpi);

      // Desync fix 6: verify this instruction is for the slab we're polling.
      // For TradeNoCpi/BatchTradeNoCpi: market is at accounts[2].
      // For TradeCpi/BatchTradeCpi: market is at accounts[1].
      // Without this guard, a batch tx touching multiple slabs would double-index
      // fills under the wrong slab.
      const isNoCpiTag = (tag === IX_TAG.TradeNoCpi || tag === IX_TAG.BatchTradeNoCpi);
      const marketAccountIdx = isNoCpiTag ? 2 : 1;
      const ixMarket = ix.accounts[marketAccountIdx]?.toBase58();
      if (ixMarket && ixMarket !== slabAddress) {
        // Not ours to write, but the other paths still number these fills.
        fillSeq += isBatch ? decodeV18BatchLegs(tag, data).length : decodeV18SingleFill(tag, data) ? 1 : 0;
        continue;
      }

      if (isBatch) {
        const legs = decodeV18BatchLegs(tag, data);
        if (legs.length === 0) continue;

        const traderKey = ix.accounts[0];
        if (!traderKey) continue;
        const trader = traderKey.toBase58();

        const base58PubkeyRegex = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;
        const base58SigRegex = /^[1-9A-HJ-NP-Za-km-z]{64,88}$/;
        if (!base58PubkeyRegex.test(trader) || !base58SigRegex.test(signature)) return false;

        // H2/H3: each leg is deduped per-leg by 23505 (see above), never per tx.

        // #205: fall back to the slab read only when the leg itself doesn't carry
        // a wire exec_price (BatchTradeCpi legs never do — see decodeV18BatchLegs).
        const resolvePrice = async (leg: (typeof legs)[number]): Promise<number> =>
          leg.execPriceE6 !== undefined ? Number(leg.execPriceE6) / 1_000_000 : resolveFallbackPrice(leg.assetIndex);

        const i128Max = (1n << 127n) - 1n;

        const batchCpi = tag === IX_TAG.BatchTradeCpi ? cpiEvidenceFromParsed(ix, ixIdx, tx.meta.innerInstructions as any, returnData, true) : null;

        for (const leg of legs) {
          // Consume the leg number BEFORE any skip so later legs keep the tx-wide numbering.
          const legIndex = fillSeq++;
          if (leg.sizeValue > i128Max) continue;
          touched.add(`${trader}|${slabAddress}|${leg.assetIndex}`);
          // Already stored (same leg, or this fill under the old per-instruction numbering)? Skip it
          // before any price read; the insert's own dedup stays the backstop if this lookup failed.
          if (batchCpi) {
            if (cpiFillAlreadyStored(await getStored(), { slab: slabAddress, assetIndex: leg.assetIndex, legIndex, trader, side: leg.side, ordinal: cpiOrdinalOf(slabAddress, leg.assetIndex, trader, leg.side) }, reduceLegs)) continue;
          } else {
            const ordinal = ordinalOf(slabAddress, leg.assetIndex, trader, leg.side, leg.sizeValue.toString());
            if (fillAlreadyStored(await getStored(), { slab: slabAddress, assetIndex: leg.assetIndex, legIndex, trader, side: leg.side, size: leg.sizeValue.toString(), ordinal }, reduceLegs)) continue;
          }
          let legSize = leg.sizeValue;
          let price: number | undefined;
          if (batchCpi) {
            const r = await resolveCpiLeg({ evidence: batchCpi, assetIndex: leg.assetIndex, side: leg.side, wireSizeAbs: leg.sizeValue, legPos: leg.legIndex, readContext: readMatcherContext, onReadError });
            if (r.kind === "read-error") { retryNeeded = true; continue; }
            if (r.kind === "skip") {
              if (r.reason !== "zero-fill") unverified.push(`${r.reason}: ${r.detail}`);
              continue;
            }
            if (r.kind === "fill") {
              legSize = r.sizeValue;
              price = Number(r.priceE6) / 1_000_000;
              if (!r.exact) logger.debug("TradeCpi executed size unverified: wrote the matcher-requested size (upper bound)", { signature: signature.slice(0, 12) });
            } // "legacy": wire size + the existing price logic, exactly as before this parser
          }
          if (price === undefined) price = await resolvePrice(leg);
          const fee = computeFeeUsd(legSize, price, leg.feeBps);

          const inserted = await insertTradeRow({
            slab_address: slabAddress,
            trader,
            side: leg.side,
            size: legSize.toString(),
            price,
            fee,
            tx_signature: signature,
            asset_index: leg.assetIndex,
            leg_index: legIndex,
          });
          if (!inserted) continue; // duplicate leg: already indexed, no event, no count
          eventBus.publish("trade.executed", slabAddress, { signature, trader, side: leg.side, size: legSize.toString() });
          insertedAny = true;
        }
        continue;
      }

      // Single-fill (TradeNoCpi=6 / TradeCpi=10) — see decodeV18SingleFill for the
      // v18 byte layout (the two tags have DIFFERENT offsets in v18).
      const decoded = decodeV18SingleFill(tag, data);
      if (!decoded) continue;
      let { sizeValue } = decoded;
      const { side } = decoded;
      const legIndex = fillSeq++;

      // Determine trader from account keys
      const traderKey = ix.accounts[0];
      if (!traderKey) continue;
      const trader = traderKey.toBase58();
      touched.add(`${trader}|${slabAddress}|${decoded.assetIndex}`);
      {
        if (tag === IX_TAG.TradeCpi) {
          if (cpiFillAlreadyStored(await getStored(), { slab: slabAddress, assetIndex: decoded.assetIndex, legIndex, trader, side, ordinal: cpiOrdinalOf(slabAddress, decoded.assetIndex, trader, side) }, reduceLegs)) continue;
        } else {
          const ordinal = ordinalOf(slabAddress, decoded.assetIndex, trader, side, sizeValue.toString());
          if (fillAlreadyStored(await getStored(), { slab: slabAddress, assetIndex: decoded.assetIndex, legIndex, trader, side, size: sizeValue.toString(), ordinal }, reduceLegs)) continue;
        }
      }

      // #205: TradeNoCpi carries the actual fill price on the wire (exec_price) —
      // authoritative and RPC-free. TradeCpi doesn't (only limit_price, the
      // requested cap), so it still falls back to the slab read.
      let price: number | undefined;
      if (decoded.execPriceE6 !== undefined) {
        price = Number(decoded.execPriceE6) / 1_000_000;
      } else if (tag === IX_TAG.TradeCpi) {
        // #213/#221: executed size + booked price from the matcher call, never the request/mark.
        const r = await resolveCpiLeg({
          evidence: cpiEvidenceFromParsed(ix, ixIdx, tx.meta.innerInstructions as any, returnData, false),
          assetIndex: decoded.assetIndex, side, wireSizeAbs: sizeValue, legPos: 0, readContext: readMatcherContext, onReadError,
        });
        if (r.kind === "read-error") { retryNeeded = true; continue; }
        if (r.kind === "skip") {
          if (r.reason !== "zero-fill") unverified.push(`${r.reason}: ${r.detail}`);
          continue; // zero fill: no row (the leg number is already consumed)
        }
        if (r.kind === "fill") {
          sizeValue = r.sizeValue;
          price = Number(r.priceE6) / 1_000_000;
          if (!r.exact) logger.debug("TradeCpi executed size unverified: wrote the matcher-requested size (upper bound)", { signature: signature.slice(0, 12) });
        } // "legacy": wire size + the existing price logic, exactly as before this parser
      }
      if (price === undefined) price = await resolveFallbackPrice(decoded.assetIndex);
      const fee = computeFeeUsd(sizeValue, price, decoded.feeBps);

      // Validate inputs
      const base58PubkeyRegex = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;
      const base58SigRegex = /^[1-9A-HJ-NP-Za-km-z]{64,88}$/;
      
      if (!base58PubkeyRegex.test(trader)) {
        logger.warn("Invalid trader pubkey format", { trader: trader.slice(0, 12) });
        continue; // this leg only — `return` would silently drop the legs after it
      }
      
      if (!base58SigRegex.test(signature)) {
        logger.warn("Invalid signature format", { signature: signature.slice(0, 12) });
        return false;
      }
      
      // Validate size is within i128 range
      const i128Max = (1n << 127n) - 1n;
      if (sizeValue > i128Max) {
        logger.warn("Trade size out of i128 range", { sizeValue: sizeValue.toString().slice(0, 30) });
        continue; // this leg only — `return` would silently drop the legs after it
      }

      const inserted = await insertTradeRow({
        slab_address: slabAddress,
        trader,
        side,
        size: sizeValue.toString(),
        price,
        fee,
        tx_signature: signature,
        asset_index: decoded.assetIndex,
        leg_index: legIndex,
      });
      if (!inserted) continue; // duplicate leg: already indexed, no event, no count

      eventBus.publish("trade.executed", slabAddress, { signature, trader, side, size: sizeValue.toString() });
      insertedAny = true;
    }

    if (unverified.length > 0) {
      // Fill not decodable (or strict policy): nothing written, signature kept for re-index.
      await recordSkippedSignatures([{ signature, source: "trade-indexer", slab: slabAddress, error: `TradeCpi ${unverified[0]}`.slice(0, 480) }]);
    }
    if (retryNeeded) {
      // Transport failure reading the matcher context: do not write a size that can never be
      // corrected. Hold the cursor; the next pass re-reads (stored legs are skipped), then falls back.
      if (this.ctxReadAttempts.size > 2000) this.ctxReadAttempts.clear();
      this.ctxReadAttempts.set(signature, 1);
      throw new CtxReadRetry(signature);
    }
    this.ctxReadAttempts.delete(signature);
    return insertedAny;
  }

  /**
   * Deliberately neutered: always returns 0 so the caller falls through to
   * `readMarkPriceFromSlab()`.
   *
   * The previous fuzzy regex scraped ANY integer in [1_000, 1e12] from `Program log:`
   * lines and treated it as `price_e6`. Percolator emits log lines via
   * `sol_log_64(idx, price, 0, 0, 0)` — a format that carries MULTIPLE hex numbers —
   * and on most non-liquidation txs the parser picked the wrong one, writing e.g.
   * $13.15 as the price of real SOL trades at ~$84. See 2026-04-20 parser overhaul.
   *
   * The correct source of truth for a Hyperp fill is the slab's `mark_price_e6`
   * at the tx slot; we route every price request through `readMarkPriceFromSlab`.
   */
  private extractPriceFromLogs(_tx: ParsedTransactionWithMeta): number {
    return 0;
  }

  /**
   * Fallback: read mark_price_e6 from the slab account's on-chain state.
   * This gives the current mark price (close to execution price for recent trades).
   *
   * Desync fix (finding 9 / TradeIndexer path): v17 accounts use parseWrapperConfigV17
   * to read markEwmaE6 at absolute offset 248 (WrapperConfigV17.mark_ewma_e6).
   * detectSlabLayout returns null for v17 account sizes — do not use it for v17.
   */
  private async readMarkPriceFromSlab(connection: Connection, slabAddress: string, assetIndex: number): Promise<number> {
    try {
      const info = await withRetry(
        () => connection.getAccountInfo(new PublicKey(slabAddress)),
        {
          maxRetries: 3,
          baseDelayMs: 1000,
          label: `getAccountInfo(${slabAddress.slice(0, 8)})`,
        }
      );
      if (!info?.data) return 0;

      const rawData = new Uint8Array(info.data);

      // #221: the asset's booked effective_price (v18); the mark EWMA below is only the fallback.
      const effectiveE6 = readAssetEffectivePriceE6(rawData, assetIndex);
      if (effectiveE6 !== null) return effectiveE6 / 1_000_000;

      // v17 path: read mark_ewma_e6 from WrapperConfigV17
      if (isV17Account(rawData)) {
        try {
          const cfg = parseWrapperConfigV17(rawData, V17_HEADER_LEN);
          const markEwmaE6 = cfg.markEwmaE6;
          if (markEwmaE6 > 0n && markEwmaE6 < 1_000_000_000_000n) {
            return Number(markEwmaE6) / 1_000_000;
          }
        } catch {
          // parseWrapperConfigV17 failed
        }
        return 0;
      }

      // v12 path: Auto-detect V0 vs V1 layout from the actual slab data length.
      // V0 (deployed devnet): ENGINE_OFF=480, no mark_price field (engineMarkPriceOff=-1).
      // V1 (future upgrade): ENGINE_OFF=640, mark_price at +400.
      const layout = detectSlabLayout(info.data.length);
      if (!layout || layout.engineMarkPriceOff < 0) return 0; // V0 has no mark_price

      const off = layout.engineOff + layout.engineMarkPriceOff;
      if (info.data.length < off + 8) return 0;

      const dv = new DataView(info.data.buffer, info.data.byteOffset, info.data.byteLength);
      const markPriceE6 = dv.getBigUint64(off, true);

      if (markPriceE6 > 0n && markPriceE6 < 1_000_000_000_000n) {
        return Number(markPriceE6) / 1_000_000;
      }
    } catch (err) {
      logger.warn("Failed to read mark price from slab", {
        slabAddress: slabAddress.slice(0, 8),
        error: err instanceof Error ? err.message : err,
      });
    }
    return 0;
  }

  /**
   * #153 — NEUTERED: always returns 0.
   *
   * The previous implementation derived `trades.fee` from the trader's net
   * native-SOL balance delta (pre/post balances at the trader's account index).
   * In a trade tx the trader's SOL delta is dominated by collateral/margin
   * movement and the Solana network fee — not the protocol fee — so `trades.fee`
   * was being populated with collateral movements for sub-1-SOL moves, and
   * genuine protocol fees ≥ 1 SOL were silently dropped to `0`.
   *
   * Following the same precedent as webhook.ts #153 / extractFeeFromTransfers,
   * we return `0` until the absolute fee can be sourced from the fee-vault
   * balance delta or a verified program event.
   *
   * @param _tx     Parsed transaction (unused after neuter).
   * @param _trader Trader public key (unused after neuter).
   * @returns Always 0.
   */
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  private extractFeeFromBalances(_tx: ParsedTransactionWithMeta, _trader: string): number {
    return 0;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
