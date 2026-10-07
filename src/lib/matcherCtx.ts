import { PublicKey, type Connection } from "@solana/web3.js";
import { decodeMatcherReturn, type MatcherReturn, type ReadMatcherContext } from "../parsers/matcherFill.js";

/**
 * Reader for the LP matcher context account's last answer (offset 0, 64 bytes). The single-fill
 * TradeCpi route writes the matcher's exec_size/exec_price there and nowhere else; it is only
 * trusted by `resolveCpiLeg` when its req_id equals the one in the transaction being indexed,
 * so a later trade having overwritten it degrades to "unverified", never to a wrong size.
 *
 * `minContextSlot` = the transaction's slot, so a node that has not yet reached it errors
 * instead of returning older state (an older req_id would just fail the match, but fail closed).
 * Any RPC failure returns null.
 */
export function makeMatcherContextReader(getConn: () => Connection, minContextSlot?: number | null): ReadMatcherContext {
  return async (address: string): Promise<MatcherReturn | null> => {
    try {
      const info = await getConn().getAccountInfo(new PublicKey(address), {
        commitment: "confirmed",
        ...(typeof minContextSlot === "number" ? { minContextSlot } : {}),
      });
      if (!info?.data) return null;
      return decodeMatcherReturn(new Uint8Array(info.data));
    } catch {
      return null;
    }
  };
}
