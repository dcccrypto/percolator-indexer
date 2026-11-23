/**
 * A fake RPC answer carries its own signature, as a real one does: `transaction.signatures[0] === sig`.
 *
 * fetchParsedTxsTolerant pairs each returned transaction with a requested signature by that field (BATCH-ORDER-1:
 * a JSON-RPC batch may answer in any order), so a fake getParsedTransaction(s) that hands back a fixture under a
 * test signature must stamp it. Returns a copy (fixtures are shared); null / undefined pass through.
 */
export function signedAs<T>(sig: string, tx: T): T {
  if (tx === null || tx === undefined || typeof tx !== "object") return tx;
  const t = tx as unknown as { transaction?: { signatures?: string[] } & Record<string, unknown> };
  const rest = Array.isArray(t.transaction?.signatures) ? t.transaction!.signatures!.slice(1) : [];
  return { ...t, transaction: { ...(t.transaction ?? {}), signatures: [sig, ...rest] } } as unknown as T;
}
