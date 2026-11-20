/**
 * vi.mock factory for src/lib/webhookChainTx.js: the chain "confirms" every payload transaction exactly as sent.
 *
 * For webhook tests about parsing, leg numbering, prices and DB writes, which hand-build the payload and do not
 * model the RPC transaction. The chain lookup itself (unknown signature, swapped accounts, read failure, batching,
 * memo) is tested against the real module in tests/lib/webhookChainTx.test.ts and
 * tests/routes/webhook.unverified-payload.test.ts.
 */
export async function chainTxEchoModule(orig: () => Promise<typeof import("../../src/lib/webhookChainTx.js")>) {
  const real = await orig();
  return {
    ...real,
    txsFromChain: async (payload: ReadonlyArray<unknown>) => ({ txs: [...payload], unknown: [], mismatched: [] }),
  };
}
