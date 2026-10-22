/**
 * Per-IP rate limiter for `/webhook/trades` (M-5), extracted from `index.ts` so it is tested
 * against the real code — `index.ts` calls `start()` at module scope, so a test cannot import it.
 * Extraction, the env-configurable budget and the 900/min default come from GH#234 / PR #235
 * (@0x-SquidSol).
 *
 * Budget. The webhook is registered on the program id for every transaction type
 * (`HeliusWebhookManager.ts`, `transactionTypes: ["ANY"]`) and the handler takes 1-10
 * transactions per call (`routes/webhook.ts`). The live v2 wrapper measured 438 tx/min on
 * 2026-10-19, i.e. 44-438 POSTs/min from Helius, so the old fixed 100/min could 429 genuine
 * deliveries. The default is now 900, overridable with WEBHOOK_RATE_LIMIT_PER_MIN.
 *
 * Charging. Every request is charged on the way in, including ones the route later rejects
 * (400/401/405/413). PR #235 charged only served requests because on `main` the key was the
 * caller-written leftmost X-Forwarded-For entry, so an attacker could spend a Helius sender's
 * bucket. On v2.2 the key is the proxy-appended hop (S3-C11, `lib/clientIp.ts`), so an
 * unauthenticated flood spends only the flooder's own bucket — and the route reads the whole
 * body (up to the size cap) before its auth check, so throttling that flood is the point.
 *
 * The HMAC auth guard is the load-bearing protection on this route; this is a secondary DoS
 * mitigation.
 */
import type { Context, MiddlewareHandler } from "hono";

export const DEFAULT_WEBHOOK_RATE_LIMIT_PER_MIN = 900;
export const WEBHOOK_RATE_LIMIT_WINDOW_MS = 60_000;

/**
 * Parse WEBHOOK_RATE_LIMIT_PER_MIN. Anything that is not a positive integer falls back to the
 * default. Deliberately not `parseInt`: a `NaN` budget would make `count > NaN` always false and
 * silently disable the limiter.
 */
export function webhookRateLimitPerMin(
  raw: string | undefined = process.env.WEBHOOK_RATE_LIMIT_PER_MIN,
  fallback: number = DEFAULT_WEBHOOK_RATE_LIMIT_PER_MIN,
): number {
  const v = raw?.trim() ?? "";
  if (v === "") return fallback;
  const parsed = Number(v);
  if (!Number.isInteger(parsed) || parsed <= 0) return fallback;
  return parsed;
}

export interface RateLimitBucket {
  count: number;
  resetTime: number;
}

export interface WebhookRateLimiterOptions {
  maxRequests: number;
  windowMs: number;
  /** The bucket key for a request. Production passes the S3-C11 `clientIp` resolution. */
  keyOf: (c: Context) => string;
  /** Injectable clock for tests. */
  now?: () => number;
  onLimit?: (info: { ip: string; count: number }) => void;
}

export interface WebhookRateLimiter {
  middleware: MiddlewareHandler;
  /** Drop expired buckets; call on an interval to bound memory. */
  pruneExpired: () => void;
  /** Introspection handle for tests. */
  buckets: ReadonlyMap<string, RateLimitBucket>;
}

export function createWebhookRateLimiter(opts: WebhookRateLimiterOptions): WebhookRateLimiter {
  const { maxRequests, windowMs, keyOf } = opts;
  const now = opts.now ?? (() => Date.now());
  const buckets = new Map<string, RateLimitBucket>();

  const middleware: MiddlewareHandler = async (c, next) => {
    const ip = keyOf(c);
    const t = now();
    const existing = buckets.get(ip);
    const bucket = !existing || t > existing.resetTime ? { count: 0, resetTime: t + windowMs } : existing;
    buckets.set(ip, bucket);
    bucket.count++;

    if (bucket.count > maxRequests) {
      opts.onLimit?.({ ip, count: bucket.count });
      c.header("Retry-After", Math.ceil((bucket.resetTime - t) / 1000).toString());
      return c.json({ error: "Too many requests, please try again later" }, 429);
    }
    await next();
  };

  const pruneExpired = (): void => {
    const t = now();
    for (const [ip, data] of buckets.entries()) {
      if (t > data.resetTime) buckets.delete(ip);
    }
  };

  return { middleware, pruneExpired, buckets };
}
