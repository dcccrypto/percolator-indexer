/**
 * Poison-pill tolerant transaction fetching (X-1).
 *
 * Anyone can land a transaction that lists a Percolator slab as a read-only account. Since Solana Transaction v1
 * (SIMD-0385) is live, such a transaction can be one this indexer's RPC client cannot return (a version error such
 * as -32015, or a client-side parse error on an old web3.js). A failed batch fetch used to hold the slab's cursor
 * forever (#147), so ONE unreadable signature would stop that slab's trade indexing permanently.
 *
 * Policy: a batch that fails is retried signature by signature. A signature whose fetch fails with an
 * "unreadable transaction" error is skipped and logged. Any OTHER error (429, network, 5xx) still holds the cursor,
 * so #147's guarantee for transient failures is unchanged.
 */

/** Highest transaction version this indexer asks the RPC for (maxSupportedTransactionVersion). */
export const MAX_REQUESTED_TX_VERSION = 1;

/**
 * "This one transaction is of a version the client cannot return": EXACTLY the node's -32015 error
 * (`.code === -32015`, or the exact text "Transaction version (N) is not supported ...") and only for a version N
 * ABOVE what we asked for. Everything else is an unhealthy RPC or a bug, not a poison pill, and must hold the cursor:
 * request-body/param errors, parse errors, proxy HTML, and a "(0)" / "(1)" version complaint when we asked for 1
 * (a node anomaly that would otherwise skip EVERY transaction).
 */
export function isUnreadableTxError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : typeof err === "string" ? err : "";
  const text = /Transaction version \((\d+)\) is not supported/.exec(msg);
  if (text && Number(text[1]) <= MAX_REQUESTED_TX_VERSION) return false;
  const code = typeof err === "object" && err !== null ? (err as { code?: unknown }).code : undefined;
  return code === -32015 || text !== null;
}

/** Rate-limit / network / gateway failures: the RPC is unhealthy; never isolate per signature, never skip. */
export function isTransientRpcError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : typeof err === "string" ? err : "";
  const code = typeof err === "object" && err !== null ? (err as { code?: unknown }).code : undefined;
  return (
    code === 429 || code === -32429 || code === -32005 ||
    /\b(429|502|503|504)\b|too many requests|rate.?limit|ECONNRESET|ETIMEDOUT|ENOTFOUND|EAI_AGAIN|fetch failed|socket hang up|network|timed? ?out|gateway|service unavailable/i.test(msg)
  );
}

/**
 * Mass-skip circuit breaker: at most ONE signature may be skipped per batch, whatever the batch size (so a lone poison
 * tx on a quiet slab cannot hold the cursor forever); a second unreadable signature means the RPC or reader is broken,
 * not poisoned. A signature is only skipped when it is classified unreadable on its OWN per-signature retry.
 */
export const MAX_SKIPS_ABSOLUTE = 1;

export interface TxFetcher<T> {
  getParsedTransactions(sigs: string[], opts: { maxSupportedTransactionVersion: number }): Promise<(T | null)[]>;
  getParsedTransaction(sig: string, opts: { maxSupportedTransactionVersion: number }): Promise<T | null>;
}

export interface TolerantFetchResult<T> {
  /** Same length and order as the input; null = not found OR skipped. */
  txs: (T | null)[];
  /** Signatures skipped because they are unreadable by this client. */
  skipped: { signature: string; error: string }[];
  /** True if a non-poison failure happened: the caller must NOT advance its cursor. */
  failed: boolean;
  /** The non-poison error, when `failed`. */
  error?: unknown;
  /** Set when the mass-skip circuit breaker tripped. */
  massSkip?: { skipped: number; total: number };
}

/**
 * @param connection - Fetcher (a web3.js Connection satisfies it).
 * @param sigs - Signatures of one batch.
 * @param retry - Retry wrapper applied to each RPC call (the indexer's `withRetry`).
 * @returns Parsed transactions in input order, the skipped poison signatures, and whether to hold the cursor.
 */
export async function fetchParsedTxsTolerant<T>(
  connection: TxFetcher<T>,
  sigs: string[],
  retry: <R>(fn: () => Promise<R>, label: string) => Promise<R>,
): Promise<TolerantFetchResult<T>> {
  const opts = { maxSupportedTransactionVersion: 1 };
  try {
    const txs = await retry(() => connection.getParsedTransactions(sigs, opts), `getParsedTransactions(${sigs.length})`);
    return { txs, skipped: [], failed: false };
  } catch (batchErr) {
    // Isolate ONLY a version-unsupported batch error. Rate limits, network errors, bad params, parse errors and
    // anything else mean the RPC is unhealthy: hold the cursor, do not fan out, do not skip.
    if (isTransientRpcError(batchErr) || !isUnreadableTxError(batchErr)) return { txs: sigs.map(() => null), skipped: [], failed: true, error: batchErr };
    const txs: (T | null)[] = [];
    const skipped: { signature: string; error: string }[] = [];
    for (const sig of sigs) {
      try {
        txs.push(await retry(() => connection.getParsedTransaction(sig, opts), `getParsedTransaction(${sig.slice(0, 8)})`));
      } catch (err) {
        if (isUnreadableTxError(err)) {
          skipped.push({ signature: sig, error: err instanceof Error ? err.message : String(err) });
          txs.push(null);
          continue;
        }
        return { txs: [...txs, ...sigs.slice(txs.length).map(() => null)], skipped, failed: true, error: err };
      }
    }
    if (skipped.length > MAX_SKIPS_ABSOLUTE) {
      // Circuit breaker: this is not a poison pill, it is a broken reader or RPC. Hold the cursor, skip nothing.
      return {
        txs: sigs.map(() => null),
        skipped: [],
        failed: true,
        error: new Error(`mass skip refused: ${skipped.length}/${sigs.length} signatures unreadable (limit ${MAX_SKIPS_ABSOLUTE} per batch); cursor held`),
        massSkip: { skipped: skipped.length, total: sigs.length },
      };
    }
    return { txs, skipped, failed: false };
  }
}
