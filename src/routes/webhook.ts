import { Hono } from "hono";
import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { PublicKey } from "@solana/web3.js";
import { IX_TAG, detectSlabLayout, isV17Account, parseWrapperConfigV17, V17_HEADER_LEN } from "@percolatorct/sdk";
import { config, eventBus, decodeBase58, withRetry, captureException, createLogger, getConnection } from "@percolatorct/shared";
import { insertTradeRows, tradeKey } from "../db/insertTradeRow.js";
import { isBlockedSlab } from "../blocklist.js";
import { parseLiquidation } from "../parsers/liquidations.js";
import {
  decodeV18SingleFill,
  decodeV18BatchLegs,
  computeFeeUsd,
  isRebalanceReduceTag,
  decodeRebalanceReduce,
  REBALANCE_REDUCE_MARKET_ACCOUNT_IDX,
} from "../parsers/percolatorTxParser.js";
import { resolveRebalanceReduce } from "../db/traderNetPosition.js";

import { cpiEvidence, resolveCpiLeg, noteUnverifiedSize } from "../parsers/matcherFill.js";
import { readAssetEffectivePriceE6 } from "../parsers/markPrice.js";
import { makeMatcherContextReader } from "../lib/matcherCtx.js";
import { recordSkippedSignatures } from "../lib/skippedSignatures.js";
import { fetchStoredLegs, fetchStoredLegsMany, fillAlreadyStored, reduceAlreadyStored, type StoredLeg } from "../db/storedLegs.js";
import { CURRENT_NETWORK } from "../network.js";

const logger = createLogger("indexer:webhook");

/** Context key: raw body bytes only after HMAC/static-token verification (defense in depth). */
type WebhookVariables = { verifiedWebhookBody: Buffer };

/**
 * v18 trade tags for webhook parsing.
 * TradeCpiV2 (alias TradeCpiV=105) is NOT a valid v18 wrapper instruction — removed.
 * BatchTradeNoCpi (66) and BatchTradeCpi (67) are now included.
 */
const TRADE_TAGS = new Set<number>([
  IX_TAG.TradeNoCpi,      // 6
  IX_TAG.TradeCpi,        // 10
  IX_TAG.BatchTradeNoCpi, // 66
  IX_TAG.BatchTradeCpi,   // 67
]);
const PROGRAM_IDS = new Set(config.allProgramIds);
const BASE58_PUBKEY = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;
/** Solana signatures are base58-encoded 64-byte values (87–88 chars). */
const BASE58_SIGNATURE = /^[1-9A-HJ-NP-Za-km-z]{64,88}$/;
/** Maximum valid trade size: signed i128 max */
const I128_MAX = (1n << 127n) - 1n;

/**
 * Maximum allowed webhook request body size (10 MB).
 *
 * Helius enhanced transaction payloads are typically 10–50 KB per transaction.
 * Even a batch of 100 transactions rarely exceeds 5 MB. A 10 MB cap prevents
 * memory-exhaustion DoS attacks while leaving ample headroom for legitimate
 * Helius payloads.
 */
const MAX_BODY_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB

/**
 * Maximum number of transactions allowed per single webhook invocation.
 *
 * Helius typically sends 1–10 transactions per webhook call. Capping at 500
 * prevents an attacker (who has obtained the webhook secret) from sending a
 * single request that triggers thousands of DB inserts and exceeds the 15-second
 * Helius timeout window.
 */
const MAX_TRANSACTIONS_PER_REQUEST = 500;

/**
 * Helius Enhanced Transaction webhook receiver.
 * Parses trade instructions from enhanced tx data and stores them.
 */
// PERC-692: Fail fast if webhook secret is not configured in production or on mainnet
const IS_PRODUCTION = process.env.NODE_ENV === "production";
const IS_MAINNET = CURRENT_NETWORK === "mainnet";
if (!config.webhookSecret) {
  if (IS_PRODUCTION || IS_MAINNET) {
    logger.error("FATAL: HELIUS_WEBHOOK_SECRET must be set in production or on mainnet — webhook auth would be bypassed");
    process.exit(1);
  } else {
    logger.warn("HELIUS_WEBHOOK_SECRET not set — webhook auth disabled (dev only)");
  }
}

/**
 * SEC (#127): Prototype-pollution guard.
 *
 * Any user-supplied object parsed from JSON may carry `__proto__`, `constructor`,
 * or `prototype` keys. Writing to those via object-spread or property assignment
 * would pollute Object.prototype and affect every subsequent object in the process.
 *
 * We reject the ENTIRE webhook request if any object in the payload contains one
 * of these keys rather than silently dropping the offending field. This is the
 * correct policy: a legitimate Helius payload will never carry these keys, and a
 * payload that does is either a probe or an adversarial input.
 */
const PROTO_POISON_KEYS = new Set(["__proto__", "constructor", "prototype"]);

function hasPoisonKey(obj: object): boolean {
  for (const key of Object.keys(obj)) {
    if (PROTO_POISON_KEYS.has(key)) return true;
  }
  return false;
}

/**
 * Validated webhook transaction type — replaces `any[]` so downstream code
 * cannot accidentally read unvalidated fields without a cast.
 */
interface ValidatedInstruction {
  programId?: string;
  data?: string;
  accounts?: string[];
  /** Helius enhanced format nests the CPIs of an instruction here. */
  innerInstructions?: ValidatedInstruction[];
}
interface ValidatedInnerGroup {
  instructions?: ValidatedInstruction[];
}
interface ValidatedAccountDatum {
  account?: string;
  data?: unknown;
  nativeBalanceChange?: unknown;
}
export interface ValidatedTransaction {
  signature?: string;
  transactionError?: unknown;
  instructions?: ValidatedInstruction[];
  innerInstructions?: ValidatedInnerGroup[];
  accountData?: ValidatedAccountDatum[];
  logs?: unknown[];
  logMessages?: unknown[];
  // Allow extra top-level fields (Helius Enhanced Tx has many) but forbid poison keys.
  [key: string]: unknown;
}

function isValidTransactionArray(parsed: unknown): parsed is ValidatedTransaction[] {
  if (!Array.isArray(parsed)) return false;
  for (const tx of parsed) {
    if (tx === null || typeof tx !== "object") return false;
    // SEC (#127): reject prototype-pollution attempts at every nesting level.
    if (hasPoisonKey(tx as object)) return false;
    if (tx.signature !== undefined && typeof tx.signature !== "string") return false;
    if (tx.instructions !== undefined) {
      if (!Array.isArray(tx.instructions)) return false;
      for (const ix of tx.instructions) {
        if (ix === null || typeof ix !== "object") return false;
        if (hasPoisonKey(ix as object)) return false;
        if (ix.programId !== undefined && typeof ix.programId !== "string") return false;
        if (ix.data !== undefined && typeof ix.data !== "string") return false;
        if (ix.accounts !== undefined) {
          if (!Array.isArray(ix.accounts)) return false;
          // Each account entry must be a string (pubkey) in the Helius enhanced format.
          for (const acc of ix.accounts) {
            if (typeof acc !== "string") return false;
          }
        }
        if (ix.innerInstructions !== undefined) {
          if (!Array.isArray(ix.innerInstructions)) return false;
          for (const sub of ix.innerInstructions) {
            if (sub === null || typeof sub !== "object") return false;
            if (hasPoisonKey(sub as object)) return false;
            if (sub.programId !== undefined && typeof sub.programId !== "string") return false;
            if (sub.data !== undefined && typeof sub.data !== "string") return false;
          }
        }
      }
    }
    if (tx.innerInstructions !== undefined) {
      if (!Array.isArray(tx.innerInstructions)) return false;
      for (const inner of tx.innerInstructions) {
        if (inner === null || typeof inner !== "object") return false;
        if (hasPoisonKey(inner as object)) return false;
        if (inner.instructions !== undefined) {
          if (!Array.isArray(inner.instructions)) return false;
          for (const ix of inner.instructions) {
            if (ix === null || typeof ix !== "object") return false;
            if (hasPoisonKey(ix as object)) return false;
            if (ix.programId !== undefined && typeof ix.programId !== "string") return false;
            if (ix.data !== undefined && typeof ix.data !== "string") return false;
            if (ix.accounts !== undefined) {
              if (!Array.isArray(ix.accounts)) return false;
              for (const acc of ix.accounts) {
                if (typeof acc !== "string") return false;
              }
            }
          }
        }
      }
    }
    if (tx.accountData !== undefined) {
      if (!Array.isArray(tx.accountData)) return false;
      for (const ad of tx.accountData) {
        if (ad === null || typeof ad !== "object") return false;
        if (hasPoisonKey(ad as object)) return false;
        if (ad.account !== undefined && typeof ad.account !== "string") return false;
      }
    }
  }
  return true;
}

export function webhookRoutes(discovery?: any): Hono<{ Variables: WebhookVariables }> {
  const app = new Hono<{ Variables: WebhookVariables }>();

  /**
   * Auth gate for `/webhook/trades`: read body once, verify signature, stash bytes for the POST handler.
   * New routes on this app that mutate data must either use this middleware or a dedicated verifier —
   * never call `insertTrade` from an HTTP handler that skips this layer.
   */
  app.use("/webhook/trades", async (c, next) => {
    if (c.req.method !== "POST") {
      c.header("Allow", "POST");
      return c.json({ error: "Method not allowed" }, 405);
    }

    // SEC: Capture request metadata for audit logging. This provides a forensic
    // trail for investigating suspicious webhook activity (replay attacks, auth
    // probing, payload manipulation). No secrets are logged.
    const requestMeta = {
      contentLength: c.req.header("content-length"),
      contentType: c.req.header("content-type"),
      userAgent: c.req.header("user-agent"),
      hasAuth: !!c.req.header("authorization"),
      hasHmac: !!c.req.header("x-helius-hmac-sha256"),
    };

    // SEC: Reject oversized payloads early to prevent OOM DoS.
    // Check Content-Length header before reading the body into memory.
    const contentLength = parseInt(c.req.header("content-length") ?? "0", 10);
    if (contentLength > MAX_BODY_SIZE_BYTES) {
      logger.warn("Webhook request rejected: body too large", { contentLength, maxAllowed: MAX_BODY_SIZE_BYTES });
      return c.json({ error: "Payload too large" }, 413);
    }

    // PERC-750 / #149: Read raw body with streaming size enforcement.
    //
    // The Content-Length pre-check above rejects obviously oversized requests, but
    // Content-Length can be absent or spoofed (e.g. sent low to bypass the check while
    // streaming a large body). We enforce the cap a second time by consuming the request
    // stream chunk-by-chunk and aborting as soon as accumulated bytes exceed the limit —
    // before any chunk after the limit is buffered in memory. This prevents OOM DoS
    // regardless of what the Content-Length header says.
    let rawBody: Buffer;
    try {
      const reader = c.req.raw.body?.getReader();
      if (!reader) {
        // No body stream — treat as empty body.
        rawBody = Buffer.alloc(0);
      } else {
        const chunks: Uint8Array[] = [];
        let totalBytes = 0;
        let tooLarge = false;

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            totalBytes += value.byteLength;
            if (totalBytes > MAX_BODY_SIZE_BYTES) {
              tooLarge = true;
              // Cancel the remaining stream to release the underlying connection.
              reader.cancel().catch(() => {});
              break;
            }
            chunks.push(value);
          }
        } finally {
          reader.releaseLock();
        }

        if (tooLarge) {
          logger.warn("Webhook request rejected: streamed body too large", { totalBytes, maxAllowed: MAX_BODY_SIZE_BYTES });
          return c.json({ error: "Payload too large" }, 413);
        }

        rawBody = Buffer.concat(chunks.map(c => Buffer.from(c)));
      }
    } catch {
      logger.warn("Webhook request failed: could not read body", requestMeta);
      return c.json({ error: "Failed to read request body" }, 400);
    }

    // #149 secondary size check removed: streaming enforcement above already aborts and
    // returns 413 before the full body is buffered, making this check redundant.

    // PERC-1063 / PERC-750: Fail-closed — 503 if secret not configured, 401 if verification fails.
    if (!config.webhookSecret) {
      logger.error("Webhook request rejected: HELIUS_WEBHOOK_SECRET not configured", requestMeta);
      return c.json({ error: "Webhook auth not configured" }, 503);
    }

    const authHeader = c.req.header("authorization") ?? "";
    const hmacHeader = c.req.header("x-helius-hmac-sha256") ?? "";
    if (!verifyWebhookSignature(rawBody, authHeader, config.webhookSecret, hmacHeader || undefined)) {
      // SEC: Log failed auth attempts with request metadata for intrusion detection.
      // Do NOT log the auth header value itself — it could be a valid secret from a
      // misconfigured client.
      logger.warn("Webhook signature verification failed", {
        ...requestMeta,
        bodyLength: rawBody.length,
      });
      return c.json({ error: "Unauthorized" }, 401);
    }

    c.set("verifiedWebhookBody", rawBody);
    return next();
  });

  app.post("/webhook/trades", async (c) => {
    const rawBody = c.get("verifiedWebhookBody");
    if (!rawBody) {
      logger.error("POST /webhook/trades reached without verified body — check middleware order");
      return c.json({ error: "Internal server error" }, 500);
    }

    // Parse body from the already-read buffer (avoids consuming the stream twice).
    let transactions: ValidatedTransaction[];
    try {
      const parsed = JSON.parse(rawBody.toString("utf-8"));
      const arr = Array.isArray(parsed) ? parsed : [parsed];
      if (!isValidTransactionArray(arr)) {
        logger.warn("Webhook request failed: invalid transaction format", { bodyLength: rawBody.length });
        return c.json({ error: "Invalid transaction format" }, 400);
      }
      transactions = arr;
    } catch {
      logger.warn("Webhook request failed: invalid JSON", { bodyLength: rawBody.length });
      return c.json({ error: "Invalid JSON" }, 400);
    }

    // SEC: Reject batches that exceed the transaction cap to prevent DB overload.
    if (transactions.length > MAX_TRANSACTIONS_PER_REQUEST) {
      logger.warn("Webhook request rejected: too many transactions", {
        count: transactions.length,
        maxAllowed: MAX_TRANSACTIONS_PER_REQUEST,
      });
      return c.json({ error: "Too many transactions in request" }, 400);
    }

    // SEC: Log successful webhook receipt for audit trail
    logger.info("Webhook received", {
      transactionCount: transactions.length,
      bodyLength: rawBody.length,
    });

    // Process synchronously — Helius has a 15s timeout, and we need to confirm
    // processing before returning 200. If we return early, Helius may retry
    // and we'd get duplicates (insertTrade handles 23505 but still wastes work).
    // GH#42: Return 500 if persistent DB failures occurred so Helius retries the webhook.
    // insertTrade is idempotent (unique constraint on tx_signature), so retries are safe.
    try {
      await processTransactions(transactions, discovery);
    } catch (err) {
      logger.error("Webhook processing error — returning 500 for Helius retry", {
        error: err instanceof Error ? err.message : err,
        transactionCount: transactions.length,
      });
      return c.json({ error: "Processing failed, please retry" }, 500);
    }

    return c.json({ received: transactions.length }, 200);
  });

  return app;
}

/**
 * Verify Helius webhook request authenticity. (PERC-750)
 *
 * Two mutually exclusive modes:
 *
 * 1. **HMAC-SHA256 body signature** (stronger — body-bound):
 *    When `hmacHeader` is non-empty, only this path runs. The value must equal
 *    hex(HMAC-SHA256(rawBody, secret)). If verification fails, the request is
 *    rejected — **`Authorization` is not used as a fallback**, so a client cannot
 *    satisfy an invalid HMAC by also sending a valid static token.
 *
 * 2. **Static token** (Helius `authHeader` when no HMAC header is sent):
 *    Used only when `hmacHeader` is empty/omitted. `Authorization` is compared
 *    timing-safely to the configured secret.
 *
 * All comparisons use `crypto.timingSafeEqual` to reduce timing side-channels.
 *
 * @param rawBody    Raw request body bytes (must be read before JSON.parse).
 * @param authHeader Value of the `Authorization` request header.
 * @param secret     Configured `HELIUS_WEBHOOK_SECRET`.
 * @param hmacHeader Optional value of the `x-helius-hmac-sha256` request header.
 */
export function verifyWebhookSignature(
  rawBody: Buffer,
  authHeader: string,
  secret: string,
  hmacHeader?: string,
): boolean {
  // Mode 1: HMAC-SHA256 — exclusive; never fall through to static token on failure.
  if (hmacHeader) {
    const expectedHmac = createHmac("sha256", secret).update(rawBody).digest("hex");
    const hmacBytes = Buffer.from(hmacHeader, "utf8");
    const expectedBytes = Buffer.from(expectedHmac, "utf8");
    // timingSafeEqual requires equal-length buffers; length mismatch is an immediate reject.
    if (hmacBytes.length !== expectedBytes.length) return false;
    return timingSafeEqual(hmacBytes, expectedBytes);
  }

  // Mode 2: Static token — timing-safe comparison (current Helius authHeader behavior).
  // HMAC both values to produce equal-length digests, preventing a timing
  // side-channel that would leak the secret's length via early return on
  // length mismatch (the old `authBytes.length !== secretBytes.length` guard).
  if (!authHeader) return false;
  // HMAC is used only for equal-length output — the hash has no security role here.
  const authDigest = createHash("sha256").update(authHeader).digest();
  const secretDigest = createHash("sha256").update(secret).digest();
  return timingSafeEqual(authDigest, secretDigest);
}

async function processTransactions(transactions: ValidatedTransaction[], discovery: any): Promise<void> {
  let indexed = 0;
  let insertFailures = 0;

  // Extract every fill in the delivery first, then write them in one round-trip.
  // A delivery carries many transactions and each can carry several legs; inserting
  // per-leg serialized that many round-trips inside Helius's ~15s window.
  const pending: Awaited<ReturnType<typeof extractTradesFromEnhancedTx>> = [];
  let blockedFills = 0;
  // #160: extraction failures are COUNTED, not swallowed. See the throw at the end.
  let extractionFailures = 0;
  let firstExtractionError: unknown = null;
  // Writes everything extracted so far and publishes what was newly written. Called once at the end,
  // and also right before a RebalanceReduce (tag 44) is resolved: a close is inferred from the
  // trader's indexed fills, so the earlier transactions of this delivery must already be stored.
  // Returns false when the write failed (the failure is counted and surfaces as a 500 below).
  const flush = async (): Promise<boolean> => {
    if (pending.length === 0) return true;
    const batch = pending.splice(0, pending.length);
    try {
      // insertTradeRows upserts with ignoreDuplicates, so already-indexed legs are
      // skipped without failing the batch — no separate duplicate branch needed.
      // GH#42: retry so transient DB failures don't silently lose trades.
      // Short base delay (100ms) to avoid blocking the 15s Helius webhook window.
      const inserted = await withRetry(() => insertTradeRows(batch), {
        maxRetries: 2,
        baseDelayMs: 100,
        label: `insertTradeRows(${batch.length})`,
      });
      indexed += inserted.length;
      // Keep the delivery-wide stored-legs snapshot in step with what THIS flush wrote (a signature
      // repeated later in the delivery must see it); nothing else changes those rows.
      for (const t of batch) {
        if (!inserted.some((k) => tradeKey(k) === tradeKey(t))) continue;
        delivery.stored?.get(t.tx_signature)?.push({ slab_address: t.slab_address, asset_index: t.asset_index, leg_index: t.leg_index, trader: t.trader, side: t.side, size: t.size, is_liquidation: t.is_liquidation });
      }

      // Publish only what was actually written, so a re-delivery of already-indexed
      // trades doesn't re-emit events to subscribers.
      const written = new Set(inserted.map(tradeKey));
      for (const trade of batch) {
        if (!written.has(tradeKey(trade))) continue;
        eventBus.publish("trade.executed", trade.slab_address, {
          signature: trade.tx_signature,
          trader: trade.trader,
          side: trade.side,
          size: trade.size,
          price: trade.price,
          fee: trade.fee,
        });
      }
      return true;
    } catch (err) {
      // All retries exhausted — capture to Sentry so we know this happened
      insertFailures += batch.length;
      logger.error("Trade batch insert failed after retries", {
        count: batch.length,
        slabAddress: batch[0]?.slab_address.slice(0, 8),
        error: err instanceof Error ? err.message : err,
      });
      captureException(err instanceof Error ? err : new Error(String(err)), {
        tags: { context: "webhook-insert-failure" },
        extra: {
          count: batch.length,
          firstSignature: batch[0]?.tx_signature?.slice(0, 16),
          slabAddress: batch[0]?.slab_address.slice(0, 16),
        },
      });
      return false;
    }
  };

  // Ascending slot order (Helius does not promise it within a delivery): a tag 44 must see the fills
  // before it. Stable, so transactions without a slot keep their delivery order.
  const slotOf = (t: ValidatedTransaction): number => (typeof t.slot === "number" ? t.slot : Number.POSITIVE_INFINITY);
  const delivery: DeliveryState = { earlier: new Set<string>(), incomplete: false };
  const ordered = [...transactions].sort((x, y) => {
    const sx = slotOf(x), sy = slotOf(y);
    return sx === sy ? 0 : sx < sy ? -1 : 1;
  });
  // ONE stored-legs query for the whole delivery (chunked), not one per transaction inside Helius's window.
  delivery.stored = await fetchStoredLegsMany(ordered.map((t) => t.signature).filter((x): x is string => typeof x === "string" && x.length > 0));
  delivery.peersOf = (tx) => ordered.filter((o) => o !== tx && (typeof o.slot !== "number" || typeof tx.slot !== "number" || o.slot === tx.slot));
  for (const tx of ordered) {
    try {
      for (const trade of await extractTradesFromEnhancedTx(tx, discovery, flush, delivery)) {
        // Retired markets are deleted from `markets`, and trades carry an FK to
        // it — so a fill here would fail the insert and burn the batch's retries.
        // Skip cleanly instead. See src/blocklist.ts.
        if (isBlockedSlab(trade.slab_address)) {
          blockedFills++;
          continue;
        }
        pending.push(trade);
      }
      if (tx.signature) delivery.earlier.add(tx.signature);
    } catch (err) {
      // #160: this used to warn and continue, so processTransactions resolved and
      // the route answered 200. Helius does not retry a 2xx, so a transaction that
      // threw during EXTRACTION was silently and permanently dropped — no insert
      // was ever attempted for it, and nothing downstream could tell.
      //
      // GH#42 fixed the same class on the far side of the insert (retry, then 500).
      // This is the near side: a parse/extraction throw before insertTradeRows is
      // reached. Same failure, earlier in the pipeline, and still silent.
      extractionFailures++;
      delivery.incomplete = true; // a later tag 44 must not be resolved past a transaction we could not read
      if (firstExtractionError === null) firstExtractionError = err;
      logger.error("Trade extraction failed — will 500 so Helius redelivers", {
        signature: tx.signature?.slice(0, 16),
        error: err instanceof Error ? err.message : err,
      });
      captureException(err instanceof Error ? err : new Error(String(err)), {
        tags: { context: "webhook-extraction-failure" },
        extra: { signature: tx.signature?.slice(0, 16) },
      });
    }
  }
  if (blockedFills > 0) {
    logger.debug("Skipped fills on blocked slabs", { count: blockedFills });
  }

  await flush();

  if (indexed > 0) {
    logger.info("Trades indexed", { count: indexed });
  }

  // Surface persistent DB failures to the caller so Helius can retry
  if (insertFailures > 0) {
    throw new Error(`${insertFailures} trade insert(s) failed after retries`);
  }

  // #160: and extraction failures, for the same reason.
  //
  // Thrown AFTER the insert above, deliberately: every trade that WAS extracted is
  // already durably written, and insertTradeRows upserts with ignoreDuplicates, so
  // the redelivery this triggers re-inserts nothing. Failing before the insert would
  // discard good fills in order to report a bad one.
  //
  // A permanently-unparseable transaction will therefore be redelivered until Helius
  // gives up. That is the correct trade: a bounded number of idempotent retries plus
  // a loud Sentry trail beats silently losing a fill. If a payload shape appears that
  // can never parse, that is a parser bug — and this is how we find out about it.
  if (extractionFailures > 0) {
    throw new Error(
      `${extractionFailures} transaction(s) failed trade extraction` +
        (firstExtractionError instanceof Error ? `: ${firstExtractionError.message}` : ""),
    );
  }
}

interface TradeData {
  slab_address: string;
  trader: string;
  side: "long" | "short" | null;   // null for liquidation markers
  size: string | null;
  price: number | null;
  fee: number;
  tx_signature: string;
  asset_index: number;   // H2/H3
  leg_index: number;     // H2/H3 — reassigned tx-globally before return
  is_liquidation: boolean;
  /** Holds a tx-wide leg number for a tag 44 that could not be written; dropped after renumbering. */
  placeholder?: boolean;
  /** This entry is a RebalanceReduce (tag 44), written or held; its leg number is not a TradeCpi fill's. */
  reduce?: boolean;
}

interface DeliveryState {
  earlier: Set<string>;
  incomplete: boolean;
  /** Stored rows for the whole delivery, read in one query; refreshed with our own flushes. null/absent = read lazily per tx. */
  stored?: Map<string, StoredLeg[]> | null;
  /** Other transactions of this delivery whose order relative to `tx` is not known from the payload (same slot, or no slot). */
  peersOf?: (tx: ValidatedTransaction) => ValidatedTransaction[];
}

async function extractTradesFromEnhancedTx(
  tx: ValidatedTransaction,
  discovery: any,
  /**
   * Writes everything extracted from EARLIER transactions of this delivery and reports whether that
   * succeeded. Called right before a RebalanceReduce (tag 44) is resolved, so the close sees the
   * fills that precede it. Omitted = nothing earlier to flush.
   */
  flush: () => Promise<boolean> = async () => true,
  /** Delivery state: signatures already extracted before this tx (chain order), and whether an earlier one failed. */
  delivery: DeliveryState = { earlier: new Set(), incomplete: false },
): Promise<TradeData[]> {
  const trades: TradeData[] = [];
  const signature = tx.signature ?? "";
  if (!signature) return trades;

  // Skip failed transactions — Helius enhanced format uses `transactionError`
  // (non-null on failure). Without this guard, failed txs are parsed and
  // indexed as phantom trades. Mirrors TradeIndexer.processTransaction check.
  if (tx.transactionError != null) {
    logger.debug("Skipping failed transaction", { signature: signature.slice(0, 12) });
    return trades;
  }

  // Validate signature format (base58, 64-byte = 87–88 chars).
  // TradeIndexer validates this (line 293) but webhook didn't — garbage
  // signatures would pollute the DB and bypass duplicate detection.
  if (!BASE58_SIGNATURE.test(signature)) return trades;

  const instructions = tx.instructions ?? [];
  // (trader|slab|asset) positions already touched by an earlier fill or tag 44 in THIS tx: a tag 44
  // after one of them starts from a position that is not in the table yet, so it is not resolved.
  const touched = new Set<string>();
  // Rows already stored for this tx, read once and only when a fill needs it.
  let stored: StoredLeg[] | null | undefined;
  const getStored = async (): Promise<StoredLeg[] | null> => {
    if (stored === undefined) stored = delivery.stored?.get(signature) ?? (await fetchStoredLegs(signature));
    return stored;
  };

  for (const ix of instructions) {
    const programId = ix.programId ?? "";
    if (!PROGRAM_IDS.has(programId)) continue;

    // Decode instruction data (base58)
    const data = ix.data ? decodeBase58(ix.data) : null;
    if (!data || data.length < 2) continue;

    const tag = data[0];
    // Liquidation marker (crank tag 5, action 1) — not a TRADE_TAG, handle before the skip.
    // No size/price/side; excluded from volume/candles. leg_index reassigned tx-globally.
    const liq = parseLiquidation(tag, data, ix.accounts ?? []);
    if (liq) {
      if (!discovery || discovery.getMarkets().has(liq.slabAddress)) {
        trades.push({
          slab_address: liq.slabAddress,
          trader: liq.portfolio,
          side: null,
          size: null,
          price: null,
          fee: 0,
          tx_signature: signature,
          asset_index: liq.assetIndex,
          leg_index: 0,
          is_liquidation: true,
        });
      }
      continue;
    }

    // RebalanceReduce (tag 44): the close used while the asset is ADL reduce-only.
    // Not a TRADE_TAG (no side on the wire) — see the TradeIndexer twin of this block.
    // It holds a slot in the tx-wide fill numbering (renumbered below, same as every other
    // fill) even when it cannot be written, so a TradeCpi after it keeps the leg_index the
    // poll path and EventStreamService give it.
    if (isRebalanceReduceTag(tag)) {
      const accounts: string[] = ix.accounts ?? [];
      const trader = accounts[0] ?? "";
      const slabAddress = accounts[REBALANCE_REDUCE_MARKET_ACCOUNT_IDX] ?? "";
      const reduce = decodeRebalanceReduce(data);
      if (!reduce) continue;
      const placeholder = (): void => {
        trades.push({
          slab_address: slabAddress, trader, side: null, size: null, price: null, fee: 0,
          tx_signature: signature, asset_index: reduce.assetIndex, leg_index: 0,
          is_liquidation: false, placeholder: true, reduce: true,
        });
      };
      // Every path out of here that writes nothing still holds the leg number (placeholder), so a
      // fill after this tag 44 keeps the number the poll path gives it.
      if (!BASE58_PUBKEY.test(trader) || !BASE58_PUBKEY.test(slabAddress)) {
        logger.warn("RebalanceReduce with an invalid account key: not indexed, leg number held", { signature: signature.slice(0, 12) });
        placeholder();
        continue;
      }
      if (discovery && !discovery.getMarkets().has(slabAddress)) {
        logger.debug("RebalanceReduce on a market this indexer does not track: leg number held", { signature: signature.slice(0, 12), slabAddress });
        placeholder();
        continue;
      }
      // This tag 44's final leg number: the count of fills (written or held) before it in the tx.
      const legIndex = trades.filter((t) => !t.is_liquidation).length;
      // F2: already stored (redelivery / backfill)? Do not resolve it again.
      if (reduceAlreadyStored(await getStored(), { slab: slabAddress, assetIndex: reduce.assetIndex, legIndex, trader, reduceQ: reduce.reduceQ })) {
        placeholder();
        continue;
      }
      const reduceKey = `${trader}|${slabAddress}|${reduce.assetIndex}`;
      // F1: earlier transactions of this delivery must be in the table before this close is resolved.
      // If that write failed, the position is not known: do not guess.
      const earlierWritten = await flush();
      // Same slot (or no slot): the order of transactions that share this position is not in the payload,
      // so a later fill could be counted as earlier. Do not resolve.
      const ambiguousPeer = (delivery.peersOf?.(tx) ?? []).some((peer) =>
        peer.transactionError == null &&
        (peer.instructions ?? []).some((pi) => {
          const a = pi.accounts ?? [];
          return a.includes(trader) && a.includes(slabAddress);
        }),
      );
      const resolution = ambiguousPeer
        ? { ok: false as const, reason: "RebalanceReduce (tag 44): another transaction in this delivery shares its slot (or has no slot) and touches the same trader and market, so the order is unknown" }
        : earlierWritten ? await resolveRebalanceReduce({
        trader,
        slabAddress,
        assetIndex: reduce.assetIndex,
        reduceQ: reduce.reduceQ,
        signature,
        txTimeSec: typeof tx.timestamp === "number" ? tx.timestamp : null,
        repeatInTx: touched.has(reduceKey),
        earlierSignatures: delivery.earlier,
        earlierIncomplete: delivery.incomplete,
      }) : { ok: false as const, reason: "RebalanceReduce (tag 44): the write of earlier transactions in this delivery failed, so the position before this close is not known" };
      touched.add(reduceKey);
      if (!resolution.ok) {
        // Never a warn-only miss: recorded durably and loudly in skipped_signatures (recorded, NOT retried).
        await recordSkippedSignatures([{ signature, source: "trade-indexer", slab: slabAddress, error: resolution.reason }]);
        placeholder();
        continue;
      }
      trades.push({
        slab_address: slabAddress,
        trader,
        side: resolution.side,
        size: resolution.sizeValue.toString(),
        price: await extractPrice(tx, slabAddress, reduce.assetIndex), // tag 44 has no matcher CPI: effective_price is its only exact source
        fee: 0, // the engine charges no trading fee on tag 44
        tx_signature: signature,
        asset_index: reduce.assetIndex,
        leg_index: 0, // renumbered tx-wide below
        is_liquidation: false,
        reduce: true,
      });
      continue;
    }

    if (!TRADE_TAGS.has(tag)) continue;

    // v18 wire format — decode lives in percolatorTxParser.ts (decodeV18SingleFill /
    // decodeV18BatchLegs); see that file for the exact byte layout. TradeNoCpi and
    // TradeCpi have DIFFERENT single-fill layouts in v18 (unlike v17, where they
    // matched), which is why decodeV18SingleFill takes the tag.
    const isBatch = (tag === IX_TAG.BatchTradeNoCpi || tag === IX_TAG.BatchTradeCpi);
    // H2/H3: capture asset_index + leg_index so batch legs dedupe on the composite key.
    // #205: feeBps/execPriceE6 carried through from the decoder — see there for
    // which variants carry a real execPriceE6 (only *NoCpi).
    const legs: { sizeValue: bigint; side: "long" | "short"; assetIndex: number; legIndex: number; feeBps?: number; execPriceE6?: bigint }[] = isBatch
      ? decodeV18BatchLegs(tag, data)
      : (() => {
          const decoded = decodeV18SingleFill(tag, data);
          return decoded ? [{ ...decoded, legIndex: 0 }] : [];
        })();

    if (legs.length === 0) continue;

    // Account layout (v17/v18 — unchanged by the v18 wire migration, which only
    // moved instruction-DATA offsets, not account ordering) — desync fix 5:
    // TradeCpi market is at accounts[1], not accounts[2].
    //   TradeNoCpi (tag 6) / BatchTradeNoCpi (tag 66):
    //     [0]=signer_a, [1]=signer_b, [2]=market (writable), [3]=account_a, [4]=account_b
    //   TradeCpi (tag 10) / BatchTradeCpi (tag 67):
    //     [0]=signer_a, [1]=market (writable), [2]=account_a (taker portfolio), [3]=account_b (LP), ...
    const accounts: string[] = ix.accounts ?? [];
    const trader = accounts[0] ?? "";
    const isNoCpi = (tag === IX_TAG.TradeNoCpi || tag === IX_TAG.BatchTradeNoCpi);
    const marketIdx = isNoCpi ? 2 : 1;
    const slabAddress = accounts.length > marketIdx ? accounts[marketIdx] : "";
    if (!trader || !slabAddress) continue;

    // Validate pubkey formats
    if (!BASE58_PUBKEY.test(trader) || !BASE58_PUBKEY.test(slabAddress)) continue;

    // L-5: Validate slabAddress against live known-slab set
    if (discovery && !discovery.getMarkets().has(slabAddress)) {
      logger.warn("Skipping trade: slab address not in known-slab set", { slabAddress, signature });
      continue;
    }

    // #205: resolve the account/RPC fallback price at most once per instruction
    // (not once per leg) — only legs without a wire execPriceE6 need it.
    // #221: per ASSET (a batch can carry legs on different assets); the fallback is that asset's
    // booked effective_price.
    const fallbackPriceByAsset = new Map<number, number>();
    // #213/#221: TradeCpi/BatchTradeCpi carry the REQUESTED size on the wire. Executed size and
    // the booked price come from the matcher call nested under this instruction.
    const isCpiTag = tag === IX_TAG.TradeCpi || tag === IX_TAG.BatchTradeCpi;
    const cpi = isCpiTag
      ? cpiEvidence(
          accounts,
          Array.isArray(ix.innerInstructions)
            ? ix.innerInstructions.filter((i) => typeof i.programId === "string" && typeof i.data === "string").map((i) => ({ programId: i.programId as string, data: i.data as string }))
            : null,
        )
      : null;
    const readMatcherContext = makeMatcherContextReader(() => getConnection(), typeof tx.slot === "number" ? tx.slot : null);

    for (const leg of legs) {
      if (leg.sizeValue > I128_MAX) continue;
      touched.add(`${trader}|${slabAddress}|${leg.assetIndex}`);

      let price: number;
      let legSize = leg.sizeValue;
      if (cpi) {
        // Already stored at this leg number (redelivery / backfill)? Do NOT spend an RPC read on the
        // matcher context: hold the number and move on. Final leg number = fills (written or held)
        // before this one in the tx; the stored-legs snapshot is the delivery-wide batched one.
        const provisionalLeg = trades.filter((t) => !t.is_liquidation).length;
        const storedNow = await getStored();
        if (storedNow?.some((r) => !r.is_liquidation && r.slab_address === slabAddress && r.asset_index === leg.assetIndex && r.leg_index === provisionalLeg)) {
          trades.push({ slab_address: slabAddress, trader, side: null, size: null, price: null, fee: 0, tx_signature: signature, asset_index: leg.assetIndex, leg_index: 0, is_liquidation: false, placeholder: true });
          continue;
        }
        const r = await resolveCpiLeg({ evidence: cpi, assetIndex: leg.assetIndex, side: leg.side, wireSizeAbs: leg.sizeValue, legPos: leg.legIndex, readContext: readMatcherContext });
        if (r.kind === "skip") {
          if (r.reason !== "zero-fill") {
            await recordSkippedSignatures([{ signature, source: "trade-indexer", slab: slabAddress, error: `TradeCpi ${r.reason}: ${r.detail}`.slice(0, 480) }]);
          }
          // Hold the leg number (renumbered below) so the other paths' numbering still lines up.
          trades.push({ slab_address: slabAddress, trader, side: null, size: null, price: null, fee: 0, tx_signature: signature, asset_index: leg.assetIndex, leg_index: 0, is_liquidation: false, placeholder: true });
          continue;
        }
        legSize = r.sizeValue;
        price = Number(r.priceE6) / 1_000_000;
        if (!r.exact) { noteUnverifiedSize(); logger.debug("TradeCpi executed size unverified: wrote the matcher-requested size (upper bound)", { signature: signature.slice(0, 12) }); }
      } else if (leg.execPriceE6 !== undefined) {
        price = Number(leg.execPriceE6) / 1_000_000;
      } else {
        let fallback = fallbackPriceByAsset.get(leg.assetIndex);
        if (fallback === undefined) {
          fallback = await extractPrice(tx, slabAddress, leg.assetIndex);
          fallbackPriceByAsset.set(leg.assetIndex, fallback);
        }
        price = fallback;
      }
      const fee = computeFeeUsd(legSize, price, leg.feeBps);

      trades.push({
        slab_address: slabAddress,
        trader,
        side: leg.side,
        size: legSize.toString(),
        price,
        fee,
        tx_signature: signature,
        asset_index: leg.assetIndex,
        leg_index: leg.legIndex,
        is_liquidation: false,
      });
    }
  }

  // (Removed: a loop over `tx.innerInstructions` groups. The Helius enhanced format has no
  // tx-level `innerInstructions`; CPIs are nested under each `instructions[i].innerInstructions`,
  // so that loop matched nothing on real payloads. The only CPI that matters for trades is the
  // matcher call under a TradeCpi, read above through `cpiEvidence`.)

  // H2/H3: reassign leg_index so (tx_signature, asset_index, leg_index) is unique
  // across outer + inner instructions. Parsing is deterministic, so the same tx
  // re-processes to the same keys (idempotent).
  //
  // #195: fills are numbered 0-based and liquidation markers are offset by 1000,
  // SEPARATELY. This must match TradeIndexer.ts and EventStreamService.ts, which
  // both already number this way — the dedup key is shared across all three
  // ingestion paths, so a path that numbers differently does not dedup against
  // the others, it duplicates against them.
  //
  // This used to renumber the whole array by position (`t.leg_index = i`) with
  // markers interleaved among the fills. A tx carrying a liquidation marker
  // followed by a real fill gave that fill leg_index 1 here (the marker took slot
  // 0) and leg_index 0 on the poll/backfill paths, so `ignoreDuplicates` on the
  // upsert never collapsed them and the fill was stored TWICE — double-counting
  // its ABS(size) into volume_24h_by_slab, trade counts and candle volume.
  // Attacker-constructible: bundle a trade with a liquidation crank in one tx.
  //
  // Single-fill, no-marker txs were unaffected (leg_index 0 everywhere), which is
  // why this survived: the common case agreed and only mixed txs diverged.
  let fillSeq = 0;
  let markerSeq = 0;
  for (const t of trades) {
    t.leg_index = t.is_liquidation ? 1000 + markerSeq++ : fillSeq++;
  }

  // Placeholders (tag 44s that could not be written) only held their number.
  const reduceLegs = new Set(trades.filter((t) => t.reduce).map((t) => t.leg_index));
  const fills = trades.filter((t) => !t.placeholder);

  // A fill already stored under another leg number (this tx indexed under the old per-instruction
  // numbering) is the SAME fill: drop it here instead of storing it twice. The rank among identical
  // fills matters, because a split order is several identical legs.
  if (fills.some((t) => !t.is_liquidation)) {
    const storedLegs = await getStored();
    if (storedLegs && storedLegs.length > 0) {
      const rank = new Map<string, number>();
      return fills.filter((t) => {
        if (t.is_liquidation || t.side === null || t.size === null) return true;
        const k = `${t.slab_address}|${t.asset_index}|${t.trader}|${t.side}|${t.size}`;
        const ordinal = (rank.get(k) ?? 0) + 1;
        rank.set(k, ordinal);
        return !fillAlreadyStored(storedLegs, {
          slab: t.slab_address, assetIndex: t.asset_index, legIndex: t.leg_index, trader: t.trader, side: t.side, size: t.size, ordinal,
        }, reduceLegs);
      });
    }
  }
  return fills;
}

/**
 * Extract execution price from an enhanced transaction.
 *
 * Strategy (in order):
 * 1. Read mark_price_e6 from the slab account's post-state data (Helius
 *    enhanced txs include `accountData` with base64-encoded post-state).
 * 2. Return 0 — log-based extraction is neutered (#150: trusting Program log:
 *    lines from any CPI program enables price poisoning; the backfill script
 *    covers fills that land with price=0).
 */
/**
 * #205: was sync (account-post-state only, then give up). Helius enhanced
 * webhook payloads don't reliably carry `accountData` for the slab, so that
 * strategy alone left every such trade at price=0 forever — nothing else in
 * the pipeline ever re-visits an already-written row. Added strategy 2: a
 * live RPC re-read of the slab, same source `readMarkPriceFromSlab` (the
 * polling path, `TradeIndexer.ts`) already uses as its ONLY strategy. This is
 * the current mark at webhook-processing time, not the fill's exact price —
 * the same approximation the poll path has always made, and the best
 * available source for TradeCpi/BatchTradeCpi fills, which don't carry a fill
 * price on the wire at all (see decodeV18SingleFill/decodeV18BatchLegs).
 */
async function extractPrice(tx: ValidatedTransaction, slabAddress: string, assetIndex: number): Promise<number> {
  // #221: the asset's booked effective_price. Exact when it comes from the slab POST-STATE in this
  // payload (strategy 1); the fresh-RPC read (strategy 2) is the LATEST value, which drifts with
  // oracle pushes, so it is APPROXIMATE. Prefer the matcher CPI's oracle_price_e6 wherever there is one.
  // Strategy 1: read mark_price_e6 from slab post-state account data (no RPC).
  const priceFromAccount = extractPriceFromAccountData(tx, slabAddress, assetIndex);
  if (priceFromAccount > 0) return priceFromAccount;

  // Strategy 2: fresh RPC read of the slab's current mark.
  const priceFromRpc = await readFreshMarkPriceE6(slabAddress, assetIndex);
  if (priceFromRpc > 0) return priceFromRpc;

  // Strategy 3: parse program logs (neutered — see extractPriceFromLogs).
  return extractPriceFromLogs(tx);
}

/**
 * Parse `mark_price_e6` (or, pre-v12.17, `config.mark_ewma_e6`) out of a raw
 * slab account buffer. Shared by both the post-state path (bytes embedded in
 * the Helius payload) and the fresh-RPC fallback (bytes from `getAccountInfo`)
 * so the v17/v0/v1 layout logic has one copy, not two that can drift.
 */
function parseMarkPriceE6FromAccountBytes(raw: Uint8Array, assetIndex: number): number {
  // #221: on a v18 market the asset's effective_price is the price the engine books a fill at; the
  // mark EWMA below is skewed by the trade itself and stays only for non-v18 layouts / unreadable slots.
  const effectiveE6 = readAssetEffectivePriceE6(raw, assetIndex);
  if (effectiveE6 !== null) return effectiveE6 / 1_000_000;

  // Desync fix 8: v17 account — read mark_ewma_e6 from WrapperConfigV17 at offset 16+232=248.
  // detectSlabLayout returns null for v17 account sizes (no v17 tier registered).
  if (isV17Account(raw)) {
    try {
      const cfg = parseWrapperConfigV17(raw, V17_HEADER_LEN);
      const markEwmaE6 = cfg.markEwmaE6;
      if (markEwmaE6 > 0n && markEwmaE6 < 1_000_000_000_000n) {
        return Number(markEwmaE6) / 1_000_000;
      }
    } catch {
      // parseWrapperConfigV17 failed — fall through (returns 0 below)
    }
    return 0;
  }

  // Auto-detect layout version from the actual slab data length.
  // V0 (legacy devnet): ENGINE_OFF=480, no mark_price field (engineMarkPriceOff=-1).
  // V1: ENGINE_OFF=640, mark_price at +400.
  // v12.17: no stored engine.mark_price; fall back to config.mark_ewma_e6
  //         (configMarkEwmaOff, absolute offset inside the slab).
  const layout = detectSlabLayout(raw.length);
  if (!layout) return 0;

  const dv = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);

  // Primary: engine.mark_price for layouts that have it.
  if (layout.engineMarkPriceOff >= 0) {
    const off = layout.engineOff + layout.engineMarkPriceOff;
    if (raw.length >= off + 8) {
      const markPriceE6 = dv.getBigUint64(off, true);
      if (markPriceE6 > 0n && markPriceE6 < 1_000_000_000_000n) {
        return Number(markPriceE6) / 1_000_000;
      }
    }
  }

  // Fallback for v12.17+: config.mark_ewma_e6. The Passive matcher quotes
  // fills against this value, so it is the correct "fill price" proxy when
  // engine.mark_price is absent.
  if (layout.configMarkEwmaOff != null && layout.configMarkEwmaOff >= 0) {
    const off = layout.configMarkEwmaOff;
    if (raw.length >= off + 8) {
      const markEwmaE6 = dv.getBigUint64(off, true);
      if (markEwmaE6 > 0n && markEwmaE6 < 1_000_000_000_000n) {
        return Number(markEwmaE6) / 1_000_000;
      }
    }
  }
  return 0;
}

/**
 * Read mark_price_e6 from the slab account's post-state data.
 * Helius enhanced transactions include `accountData[]` with each account's
 * post-state as a base64-encoded `data` field.
 */
function extractPriceFromAccountData(tx: ValidatedTransaction, slabAddress: string, assetIndex: number): number {
  const accountData: any[] = tx.accountData ?? [];
  for (const acc of accountData) {
    if (acc.account !== slabAddress) continue;
    // Helius provides data as base64 string or { data: [base64, "base64"] }
    let raw: Uint8Array | null = null;
    if (typeof acc.data === "string") {
      try { raw = Uint8Array.from(Buffer.from(acc.data, "base64")); } catch { /* skip */ }
    } else if (Array.isArray(acc.data) && typeof acc.data[0] === "string") {
      try { raw = Uint8Array.from(Buffer.from(acc.data[0], "base64")); } catch { /* skip */ }
    }
    if (!raw) continue;

    const price = parseMarkPriceE6FromAccountBytes(raw, assetIndex);
    if (price > 0) return price;
  }
  return 0;
}

/**
 * #205 strategy 2: a live `getAccountInfo` read of the slab, mirroring
 * `TradeIndexer.readMarkPriceFromSlab` exactly (same retry policy, same byte
 * parser). Failures are logged and swallowed — a webhook delivery must not
 * fail the whole batch because one RPC read timed out; the trade is still
 * indexed, just with price=0 (unchanged from before this fix in that case).
 */
async function readFreshMarkPriceE6(slabAddress: string, assetIndex: number): Promise<number> {
  try {
    const info = await withRetry(
      () => getConnection().getAccountInfo(new PublicKey(slabAddress)),
      { maxRetries: 3, baseDelayMs: 1000, label: `getAccountInfo(${slabAddress.slice(0, 8)})` },
    );
    if (!info?.data) return 0;
    return parseMarkPriceE6FromAccountBytes(new Uint8Array(info.data), assetIndex);
  } catch (err) {
    logger.warn("Failed to read fresh mark price from slab", {
      slabAddress: slabAddress.slice(0, 8),
      error: err instanceof Error ? err.message : err,
    });
    return 0;
  }
}

/**
 * #150 — NEUTERED: always returns 0.
 *
 * The previous implementation scraped ANY "Program log:" line from the tx's log
 * array and treated the first integer in [1_000, 1e12] as price_e6. Because
 * Percolator txs may include CPI calls to system programs, AMMs, or arbitrary
 * third-party programs, log lines emitted by non-Percolator programs were
 * silently trusted. An attacker who can craft a tx with an inner CPI to a
 * program that emits a plausible-looking log line could poison every fill in
 * that tx with an arbitrary price.
 *
 * The correct source of truth for a fill price is `extractPriceFromAccountData`,
 * which reads `mark_price_e6` / `mark_ewma_e6` from the slab's post-state data
 * included in the Helius enhanced payload. When that is absent (e.g. the slab
 * account wasn't included in accountData), the price is stored as 0 and the
 * backfill-price-zero-trades.ts script is used to retroactively populate it.
 *
 * This matches what TradeIndexer.ts already does (see extractPriceFromLogs
 * there, which has been a no-op since the 2026-04-20 parser overhaul).
 */
function extractPriceFromLogs(_tx: ValidatedTransaction): number {
  return 0;
}

/**
 * #153 — NEUTERED: always returns 0.
 *
 * The previous implementation derived `trades.fee` from the trader's net
 * native-SOL balance delta, accepting any value in the (10_000, 1_000_000_000)
 * lamport range as "the fee". In a trade tx the trader's SOL delta is dominated
 * by collateral/margin movement plus the network fee — not the protocol fee —
 * so `trades.fee` was being populated with collateral movements for sub-1-SOL
 * moves, and genuine fees ≥ 1 SOL were silently recorded as `0`.
 *
 * The correct source of truth for the absolute protocol fee is the fee-vault
 * balance delta (the account that receives the fee) or an explicit fee field in
 * the fill receipt event, neither of which is currently available in the Helius
 * enhanced-transaction payload without knowing the protocol fee account address
 * at parse time.
 *
 * Following the same precedent as #150 (extractPriceFromLogs neutered to avoid
 * log-injection), we record `fee = 0` when the fee is not recoverable from a
 * trusted source rather than misattributing collateral movements. A backfill
 * script can populate the column retroactively once the fee-vault address is
 * plumbed through.
 *
 * @param _tx      Enhanced transaction (unused after neuter).
 * @param _trader  Trader public key (unused after neuter).
 * @returns Always 0.
 */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function extractFeeFromTransfers(_tx: ValidatedTransaction, _trader: string): number {
  return 0;
}
