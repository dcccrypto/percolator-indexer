/**
 * Cached, single-flight health probe (#163).
 *
 * `/health` is unauthenticated and is not covered by the rate limiter (which is
 * scoped to /webhook/trades). Every request used to trigger a live `getSlot()`
 * and a Supabase query, so sustained unauthenticated traffic burned the
 * indexer's metered RPC credits and DB quota — the same budget discovery, stats
 * collection and trade polling depend on, so flooding it could stall indexing.
 *
 * Two mechanisms, and both are needed:
 *
 *  1. **TTL cache** — sequential requests inside the window reuse the last
 *     result instead of re-probing.
 *  2. **Single-flight** — concurrent requests share the in-flight probe.
 *     A TTL alone does NOT bound a burst: 100 simultaneous requests all miss
 *     the cache before the first probe resolves and fan out to 100 round-trips,
 *     which is exactly the amplification being closed.
 *
 * The TTL is deliberately short so a real outage still surfaces quickly to
 * Docker healthchecks and load balancers.
 */

export interface HealthChecks {
  db: boolean;
  rpc: boolean;
}

export interface HealthResult {
  status: "ok" | "degraded" | "down";
  checks: HealthChecks;
  /** True when served from cache rather than a fresh probe. Diagnostics only —
   *  never exposed to unauthenticated callers (see I-3 masking in index.ts). */
  cached: boolean;
}

export interface HealthCheckerOptions {
  probeRpc: () => Promise<boolean>;
  probeDb: () => Promise<boolean>;
  /** Cache window. Short by design: a real outage must still surface fast. */
  ttlMs?: number;
  /** Injectable clock for tests. */
  now?: () => number;
}

export const DEFAULT_HEALTH_TTL_MS = 5_000;

function classify(checks: HealthChecks): HealthResult["status"] {
  const values = Object.values(checks);
  const failed = values.filter((v) => !v).length;
  if (failed === 0) return "ok";
  if (failed === values.length) return "down";
  return "degraded";
}

export function createHealthChecker(opts: HealthCheckerOptions): () => Promise<HealthResult> {
  const ttlMs = opts.ttlMs ?? DEFAULT_HEALTH_TTL_MS;
  const now = opts.now ?? Date.now;

  let cached: { at: number; result: HealthResult } | null = null;
  let inFlight: Promise<HealthResult> | null = null;

  async function probe(): Promise<HealthResult> {
    // Probes are independent; run them concurrently so one slow backend does
    // not serialise the other. Each already resolves to a boolean rather than
    // throwing, so Promise.all cannot reject here.
    const [rpc, db] = await Promise.all([opts.probeRpc(), opts.probeDb()]);
    const checks: HealthChecks = { rpc, db };
    return { status: classify(checks), checks, cached: false };
  }

  return async function check(): Promise<HealthResult> {
    if (cached && now() - cached.at < ttlMs) {
      return { ...cached.result, cached: true };
    }

    // Single-flight: a burst arriving before the first probe resolves shares it
    // instead of each issuing its own RPC + DB round-trip.
    if (inFlight) {
      const result = await inFlight;
      return { ...result, cached: true };
    }

    inFlight = probe()
      .then((result) => {
        cached = { at: now(), result };
        return result;
      })
      .finally(() => {
        // Cleared even on failure, so a thrown probe cannot wedge the endpoint
        // into permanently reusing a rejected promise.
        inFlight = null;
      });

    return inFlight;
  };
}
