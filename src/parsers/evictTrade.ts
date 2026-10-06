import { IX_TAG } from "@percolatorct/sdk";
import { IX_TAG_V22 } from "@percolatorct/sdk";

/**
 * v2.2 EvictAndTradeCpi (tag 119) is `[119]` + the body of a TradeCpi, with ONE account (the victim portfolio)
 * prepended to the TradeCpi account list. For fill indexing, rewrite it to the TradeCpi (tag 10) it wraps and drop the
 * prepended account. Any other instruction (or a malformed 119) is returned unchanged.
 */
export function unwrapEvictAndTradeIx<A>(data: Uint8Array, accounts: readonly A[]): { data: Uint8Array; accounts: A[] } {
  if (data.length >= 2 && data[0] === IX_TAG_V22.EvictAndTradeCpi && accounts.length >= 2) {
    const out = new Uint8Array(data);
    out[0] = IX_TAG.TradeCpi;
    return { data: out, accounts: accounts.slice(1) };
  }
  return { data, accounts: [...accounts] };
}
