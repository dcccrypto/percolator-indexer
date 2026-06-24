import { Hono } from "hono";
import { createHash, createHmac, timingSafeEqual } from "node:crypto";
import { IX_TAG, detectSlabLayout, isV17Account, parseWrapperConfigV17, V17_HEADER_LEN } from "@percolatorct/sdk";
import { config, insertTrade, eventBus, decodeBase58, parseTradeSize, withRetry, captureException, createLogger } from "@percolatorct/shared";
import { CURRENT_NETWORK } from "../network.js";

const logger = createLogger("indexer:webhook");

/** Context key: raw body bytes only after HMAC/static-token verification (defense in depth). */
type WebhookVariables = { verifiedWebhookBody: Buffer };

/**
 * v17 trade tags for webhook parsing.
 * TradeCpiV2 (alias TradeCpiV=105) is NOT a valid v17 wrapper instruction — removed.
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

/** Supabase duplicate constraint — not retriable */
function isDuplicateError(err: unknown): boolean {
  if (!err) return false;
  const msg = err instanceof Error ? err.message : String(err);
  // Postgres unique constraint violation code 23505
  return msg.includes("23505") || msg.toLowerCase().includes("duplicate");
}

async function processTransactions(transactions: ValidatedTransaction[], discovery: any): Promise<void> {
  let indexed = 0;
  let insertFailures = 0;

  for (const tx of transactions) {
    try {
      const trades = extractTradesFromEnhancedTx(tx, discovery);
      for (const trade of trades) {
        try {
          // GH#42: Wrap insertTrade with retry so transient DB failures don't silently
          // lose trades. Duplicate constraint (23505) is not retriable — skip immediately.
          // Short base delay (100ms) to avoid blocking the 15s Helius webhook window.
          await withRetry(() => insertTrade(trade), {
            maxRetries: 2,
            baseDelayMs: 100,
            label: `insertTrade(${trade.tx_signature.slice(0, 12)})`,
          });
          eventBus.publish("trade.executed", trade.slab_address, {
            signature: trade.tx_signature,
            trader: trade.trader,
            side: trade.side,
            size: trade.size,
            price: trade.price,
            fee: trade.fee,
          });
          indexed++;
        } catch (err) {
          if (isDuplicateError(err)) {
            // Duplicate insert — expected, not an error
            logger.debug("Duplicate trade insert skipped", { signature: trade.tx_signature.slice(0, 12) });
          } else {
            // All retries exhausted — capture to Sentry so we know this happened
            insertFailures++;
            logger.error("Trade insert failed after retries", {
              signature: trade.tx_signature.slice(0, 12),
              slabAddress: trade.slab_address.slice(0, 8),
              error: err instanceof Error ? err.message : err,
            });
            captureException(err instanceof Error ? err : new Error(String(err)), {
              tags: { context: "webhook-insert-failure" },
              extra: {
                signature: trade.tx_signature.slice(0, 16),
                slabAddress: trade.slab_address.slice(0, 16),
              },
            });
          }
        }
      }
    } catch (err) {
      logger.warn("Failed to process transaction", { error: err instanceof Error ? err.message : err });
    }
  }

  if (indexed > 0) {
    logger.info("Trades indexed", { count: indexed });
  }

  // Surface persistent DB failures to the caller so Helius can retry
  if (insertFailures > 0) {
    throw new Error(`${insertFailures} trade insert(s) failed after retries`);
  }
}

interface TradeData {
  slab_address: string;
  trader: string;
  side: "long" | "short";
  size: string;
  price: number;
  fee: number;
  tx_signature: string;
}

function extractTradesFromEnhancedTx(tx: ValidatedTransaction, discovery: any): TradeData[] {
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

  for (const ix of instructions) {
    const programId = ix.programId ?? "";
    if (!PROGRAM_IDS.has(programId)) continue;

    // Decode instruction data (base58)
    const data = ix.data ? decodeBase58(ix.data) : null;
    if (!data || data.length < 2) continue;

    const tag = data[0];
    if (!TRADE_TAGS.has(tag)) continue;

    // v17 wire format:
    //   Single fill (TradeNoCpi=6, TradeCpi=10):
    //     tag(1)+asset_index(u16=2)+size_q(i128=16)+... — min 19 bytes, size at [3:19]
    //   Batch fill (BatchTradeNoCpi=66, BatchTradeCpi=67):
    //     tag(1)+n_legs(u8=1)+[asset_index(u16=2)+size_q(i128=16)+exec_price(8)+8B]*n — min 2+34 bytes
    //     Each leg is expanded separately.
    const isBatch = (tag === IX_TAG.BatchTradeNoCpi || tag === IX_TAG.BatchTradeCpi);
    const legs: { sizeValue: bigint; side: "long" | "short" }[] = [];

    if (isBatch) {
      if (data.length < 2) continue;
      const nLegs = data[1];
      if (nLegs === 0) continue;
      for (let i = 0; i < nLegs; i++) {
        const legOff = 2 + i * 34;
        if (legOff + 34 > data.length) break;
        const { sizeValue, side } = parseTradeSize(data.slice(legOff + 2, legOff + 18));
        if (sizeValue === 0n) continue;
        legs.push({ sizeValue, side });
      }
    } else {
      if (data.length < 19) continue;
      const { sizeValue, side } = parseTradeSize(data.slice(3, 19));
      if (sizeValue === 0n) continue;
      legs.push({ sizeValue, side });
    }

    if (legs.length === 0) continue;

    // Account layout (v17) — desync fix 5: TradeCpi market is at accounts[1], not accounts[2].
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

    // Extract price from slab account data or program logs
    const price = extractPrice(tx, slabAddress);
    const fee = extractFeeFromTransfers(tx, trader);

    for (const leg of legs) {
      if (leg.sizeValue > I128_MAX) continue;

      trades.push({
        slab_address: slabAddress,
        trader,
        side: leg.side,
        size: leg.sizeValue.toString(),
        price,
        fee,
        tx_signature: signature,
      });
    }
  }

  // Also check inner instructions (for TradeCpi routed through matcher)
  const innerInstructions = tx.innerInstructions ?? [];
  for (const inner of innerInstructions) {
    const innerIxs = inner.instructions ?? [];
    for (const ix of innerIxs) {
      const programId = ix.programId ?? "";
      if (!PROGRAM_IDS.has(programId)) continue;

      const data = ix.data ? decodeBase58(ix.data) : null;
      if (!data || data.length < 2) continue;

      const tag = data[0];
      if (!TRADE_TAGS.has(tag)) continue;

      const isBatchInner = (tag === IX_TAG.BatchTradeNoCpi || tag === IX_TAG.BatchTradeCpi);
      const legs: { sizeValue: bigint; side: "long" | "short" }[] = [];

      if (isBatchInner) {
        if (data.length < 2) continue;
        const nLegs = data[1];
        if (nLegs === 0) continue;
        for (let i = 0; i < nLegs; i++) {
          const legOff = 2 + i * 34;
          if (legOff + 34 > data.length) break;
          const { sizeValue, side } = parseTradeSize(data.slice(legOff + 2, legOff + 18));
          if (sizeValue === 0n) continue;
          legs.push({ sizeValue, side });
        }
      } else {
        if (data.length < 19) continue;
        const { sizeValue, side } = parseTradeSize(data.slice(3, 19));
        if (sizeValue === 0n) continue;
        legs.push({ sizeValue, side });
      }

      if (legs.length === 0) continue;

      // Same v17 account layout with CPI dispatch fix (desync fix 5)
      const accounts: string[] = ix.accounts ?? [];
      const trader = accounts[0] ?? "";
      const isNoCpiInner = (tag === IX_TAG.TradeNoCpi || tag === IX_TAG.BatchTradeNoCpi);
      const innerMarketIdx = isNoCpiInner ? 2 : 1;
      const slabAddress = accounts.length > innerMarketIdx ? accounts[innerMarketIdx] : "";
      if (!trader || !slabAddress) continue;

      if (!BASE58_PUBKEY.test(trader) || !BASE58_PUBKEY.test(slabAddress)) continue;

      // L-5: Validate slabAddress against live known-slab set
      if (discovery && !discovery.getMarkets().has(slabAddress)) {
        logger.warn("Skipping inner trade: slab address not in known-slab set", { slabAddress, signature });
        continue;
      }

      const price = extractPrice(tx, slabAddress);
      const fee = extractFeeFromTransfers(tx, trader);

      for (const leg of legs) {
        if (leg.sizeValue > I128_MAX) continue;

        // Avoid duplicates within same tx (match on trader + side + size + slab)
        if (trades.some((t) => t.tx_signature === signature && t.trader === trader && t.slab_address === slabAddress && t.side === leg.side && t.size === leg.sizeValue.toString())) continue;

        trades.push({
          slab_address: slabAddress,
          trader,
          side: leg.side,
          size: leg.sizeValue.toString(),
          price,
          fee,
          tx_signature: signature,
        });
      }
    }
  }

  return trades;
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
function extractPrice(tx: ValidatedTransaction, slabAddress: string): number {
  // Strategy 1: read mark_price_e6 from slab post-state account data
  const priceFromAccount = extractPriceFromAccountData(tx, slabAddress);
  if (priceFromAccount > 0) return priceFromAccount;

  // Strategy 2: parse program logs
  return extractPriceFromLogs(tx);
}

/**
 * Read mark_price_e6 from the slab account's post-state data.
 * Helius enhanced transactions include `accountData[]` with each account's
 * post-state as a base64-encoded `data` field.
 */
function extractPriceFromAccountData(tx: ValidatedTransaction, slabAddress: string): number {
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
        // parseWrapperConfigV17 failed — fall through to log-based extraction
      }
      continue;
    }

    // Auto-detect layout version from the actual slab data length.
    // V0 (legacy devnet): ENGINE_OFF=480, no mark_price field (engineMarkPriceOff=-1).
    // V1: ENGINE_OFF=640, mark_price at +400.
    // v12.17: no stored engine.mark_price; fall back to config.mark_ewma_e6
    //         (configMarkEwmaOff, absolute offset inside the slab).
    const layout = detectSlabLayout(raw.length);
    if (!layout) continue;

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
  }
  return 0;
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
