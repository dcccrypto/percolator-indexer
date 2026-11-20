import { PublicKey, type Connection } from "@solana/web3.js";
import { decodeMatcherReturn, type ContextRead, type MatcherReturn, type ReadMatcherContext } from "../parsers/matcherFill.js";

export interface ContextReaderOptions {
  /** per-call timeout (default 2000 ms) */
  timeoutMs?: number;
  /** extra attempts after a transport error / timeout (default 0) */
  retries?: number;
  retryDelayMs?: number;
  /** absolute ms-epoch deadline for ALL reads sharing this options object's owner (e.g. one webhook delivery) */
  deadlineAt?: number;
  /**
   * Share answers between readers of one webhook delivery (see `ContextReadMemo`). Only a successful
   * read whose node slot is known is stored, and it is reused only for a transaction at or before that
   * slot. Errors are never stored.
   */
  memo?: ContextReadMemo;
}

/**
 * Per-delivery memo of matcher context reads: address -> the newest successful answer and the slot the
 * node answered at. One delivery carries many TradeCpi txs against the same few LP contexts.
 *
 * Why reuse is never worse than a fresh read: the context holds the last answer, and `resolveCpiLeg`
 * accepts it only when its req_id equals the tx being indexed. If a fresh read at slot T2 shows the tx's
 * answer, nothing overwrote it between the tx's slot and T2, so an earlier read at any slot T1 with
 * tx.slot <= T1 <= T2 shows the same answer. That is why the stored answer needs a slot >= the tx's slot.
 */
export type ContextReadMemo = Map<string, { slot: number; ret: MatcherReturn | null }>;

export function newContextReadMemo(): ContextReadMemo {
  return new Map();
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
  const memo = opts.memo;
  return async (address: string): Promise<ContextRead> => {
    let last = "unknown";
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (opts.deadlineAt !== undefined && Date.now() >= opts.deadlineAt) return { kind: "error", detail: "deadline exceeded" };
      const hit = memo && typeof minContextSlot === "number" ? memo.get(address) : undefined;
      if (hit && hit.slot >= (minContextSlot as number)) return { kind: "ok", ret: hit.ret };
      try {
        const remaining = opts.deadlineAt !== undefined ? Math.max(1, opts.deadlineAt - Date.now()) : timeoutMs;
        const config = {
          commitment: "confirmed" as const,
          ...(typeof minContextSlot === "number" ? { minContextSlot } : {}),
        };
        let info: { data?: Uint8Array | Buffer } | null;
        let slot: number | null = null;
        const conn = getConn();
        if (memo && typeof conn.getAccountInfoAndContext === "function") {
          // Same single getAccountInfo request on the wire (web3.js getAccountInfo wraps this call);
          // the context slot is what makes the answer reusable.
          const res = await withTimeout(conn.getAccountInfoAndContext(new PublicKey(address), config), Math.min(timeoutMs, remaining));
          info = res?.value ?? null;
          slot = typeof res?.context?.slot === "number" ? res.context.slot : null;
        } else {
          info = await withTimeout(conn.getAccountInfo(new PublicKey(address), config), Math.min(timeoutMs, remaining));
          // Without the context slot all we know is the node was at or past minContextSlot.
          if (typeof minContextSlot === "number") slot = minContextSlot;
        }
        // account missing: a definite answer, not ours
        const ret = info?.data ? decodeMatcherReturn(new Uint8Array(info.data)) : null;
        if (memo && slot !== null) {
          const prev = memo.get(address);
          if (!prev || slot >= prev.slot) memo.set(address, { slot, ret });
        }
        return { kind: "ok", ret };
      } catch (err) {
        last = err instanceof Error ? err.message : String(err);
        if (attempt < retries) await new Promise((r) => setTimeout(r, opts.retryDelayMs ?? 300));
      }
    }
    return { kind: "error", detail: last.slice(0, 200) };
  };
}
