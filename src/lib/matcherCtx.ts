import { PublicKey, type Connection } from "@solana/web3.js";
import { decodeMatcherReturn, type ContextRead, type ReadMatcherContext } from "../parsers/matcherFill.js";

export interface ContextReaderOptions {
  /** per-call timeout (default 2000 ms) */
  timeoutMs?: number;
  /** extra attempts after a transport error / timeout (default 0) */
  retries?: number;
  retryDelayMs?: number;
  /** absolute ms-epoch deadline for ALL reads sharing this options object's owner (e.g. one webhook delivery) */
  deadlineAt?: number;
}

const DEFAULT_TIMEOUT_MS = 2000;

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`timeout after ${ms} ms`)), ms);
    p.then((v) => { clearTimeout(t); resolve(v); }, (e) => { clearTimeout(t); reject(e); });
  });
}

/**
 * Reader for the LP matcher context account's last answer (offset 0, 64 bytes). The single-fill
 * TradeCpi route writes the matcher's exec_size/exec_price there and nowhere else; `resolveCpiLeg`
 * trusts it only when its req_id etc. equal the transaction being indexed, so a later trade having
 * overwritten it degrades to "unverified", never to a wrong size.
 *
 * A transport failure (RPC error, timeout, node behind `minContextSlot`, deadline passed) is returned
 * as `{kind:"error"}`, DISTINCT from "the read worked and the answer is not ours": the first can
 * succeed on a retry, the second never can, and the unique index makes a size written after a
 * transient failure permanent.
 */
export function makeMatcherContextReader(getConn: () => Connection, minContextSlot?: number | null, opts: ContextReaderOptions = {}): ReadMatcherContext {
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const retries = opts.retries ?? 0;
  return async (address: string): Promise<ContextRead> => {
    let last = "unknown";
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (opts.deadlineAt !== undefined && Date.now() >= opts.deadlineAt) return { kind: "error", detail: "deadline exceeded" };
      try {
        const remaining = opts.deadlineAt !== undefined ? Math.max(1, opts.deadlineAt - Date.now()) : timeoutMs;
        const info = await withTimeout(
          getConn().getAccountInfo(new PublicKey(address), {
            commitment: "confirmed",
            ...(typeof minContextSlot === "number" ? { minContextSlot } : {}),
          }),
          Math.min(timeoutMs, remaining),
        );
        if (!info?.data) return { kind: "ok", ret: null }; // account missing: a definite answer, not ours
        return { kind: "ok", ret: decodeMatcherReturn(new Uint8Array(info.data)) };
      } catch (err) {
        last = err instanceof Error ? err.message : String(err);
        if (attempt < retries) await new Promise((r) => setTimeout(r, opts.retryDelayMs ?? 300));
      }
    }
    return { kind: "error", detail: last.slice(0, 200) };
  };
}
