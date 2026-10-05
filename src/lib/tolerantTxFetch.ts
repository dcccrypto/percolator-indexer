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

/** Errors that mean "this particular transaction cannot be returned to this client", not "the RPC is unhealthy". */
export function isUnreadableTxError(err: unknown): boolean {
  const msg = err instanceof Error ? err.message : typeof err === "string" ? err : JSON.stringify(err ?? "");
  return /-32015|Transaction version \(\S+\) is not supported|maxSupportedTransactionVersion|At path:\s*(?:transaction|version|meta)|Expected the value to satisfy a union|failed to deserialize|unsupported transaction version|Reached end of buffer/i.test(msg);
}

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
    // Isolate: fetch one by one. A single-signature batch has nothing to isolate.
    if (sigs.length === 1 && !isUnreadableTxError(batchErr)) return { txs: [null], skipped: [], failed: true, error: batchErr };
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
    return { txs, skipped, failed: false };
  }
}
