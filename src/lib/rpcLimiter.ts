/**
 * Paces RPC calls for the trade indexer in place of fixed sleeps between markets.
 *
 * Every call takes the next free slot, `minIntervalMs` after the previous one, so the request rate is capped no matter
 * how the callers are arranged (for the history reads it wraps; other calls are not paced) (the old code slept 1 s / 0.5 s per market and fired each market's calls back to back).
 * A rate-limit error (HTTP 429 and friends) is retried here after a backoff, and pushes the slot of every other call
 * out as well, so one 429 slows the whole indexer down instead of only the call that hit it. Any other error is
 * rethrown untouched, and so is a rate-limit error once `maxRateLimitRetries` retries are used up (the callers'
 * existing hold-the-cursor handling then applies).
 */
export interface RpcLimiterOptions {
  /** Minimum gap between the starts of two calls. 0 disables pacing. */
  minIntervalMs: number;
  /** Retries after a rate-limit error before it is rethrown. */
  maxRateLimitRetries?: number;
  /** First backoff after a rate-limit error; doubles per retry up to maxBackoffMs. */
  baseBackoffMs?: number;
  maxBackoffMs?: number;
  /** Injectable for tests. */
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  onRateLimited?: (info: { attempt: number; backoffMs: number }) => void;
}

export function isRateLimitError(err: unknown): boolean {
  const e = err as { code?: unknown; status?: unknown; message?: unknown } | null;
  const code = typeof e?.code === "number" ? e.code : typeof e?.status === "number" ? e.status : undefined;
  if (code === 429 || code === -32429) return true;
  const msg = err instanceof Error ? err.message : typeof e?.message === "string" ? e.message : String(err);
  return /\b429\b|too many requests|rate.?limit/i.test(msg);
}

export class RpcLimiter {
  private nextSlot = 0;
  /** Number of rate-limit errors seen (each retry counts). */
  rateLimited = 0;
  private readonly opts: Required<Omit<RpcLimiterOptions, "onRateLimited">> & Pick<RpcLimiterOptions, "onRateLimited">;

  constructor(opts: RpcLimiterOptions) {
    this.opts = {
      maxRateLimitRetries: 1, // web3.js and withRetry already retry a 429 too; keep the product small
      baseBackoffMs: 500,
      maxBackoffMs: 15_000,
      now: () => Date.now(),
      sleep: (ms) => new Promise((r) => setTimeout(r, ms)),
      ...opts,
    };
  }

  /** Reserve the next slot (synchronously, so concurrent callers queue in order) and wait for it. */
  private async acquire(): Promise<void> {
    const now = this.opts.now();
    const at = Math.max(now, this.nextSlot);
    this.nextSlot = at + this.opts.minIntervalMs;
    if (at > now) await this.opts.sleep(at - now);
  }

  async run<T>(fn: () => Promise<T>, maxRetries = this.opts.maxRateLimitRetries): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      await this.acquire();
      try {
        return await fn();
      } catch (err) {
        if (!isRateLimitError(err)) throw err;
        this.rateLimited++;
        const backoffMs = Math.min(this.opts.maxBackoffMs, this.opts.baseBackoffMs * 2 ** attempt);
        // Everyone waits, not just this call, including when this call gives up (the caller's own retry then starts late too).
        this.nextSlot = Math.max(this.nextSlot, this.opts.now() + backoffMs);
        if (attempt >= maxRetries) throw err;
        this.opts.onRateLimited?.({ attempt: attempt + 1, backoffMs });
      }
    }
  }
}

const PACED = ["getSignaturesForAddress", "getParsedTransactions", "getParsedTransaction"] as const;

/** The connection with its transaction-history reads routed through the limiter. Other members pass through. */
export function paceConnection<C extends object>(connection: C, limiter: RpcLimiter, maxRetries?: number): C {
  return new Proxy(connection, {
    get(target, prop, receiver) {
      const v = Reflect.get(target, prop, receiver);
      if (typeof v === "function" && (PACED as readonly unknown[]).includes(prop)) {
        return (...args: unknown[]) => limiter.run(() => (v as (...a: unknown[]) => Promise<unknown>).apply(target, args), maxRetries);
      }
      return typeof v === "function" ? v.bind(target) : v;
    },
  });
}
