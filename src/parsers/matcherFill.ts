/**
 * What a TradeCpi (tag 10) / BatchTradeCpi (tag 67) transaction really executed (#213, #221).
 *
 * The instruction the user signs carries the REQUESTED size (`size_q`), the taker's fee CAP
 * (`fee_bps`) and a limit price. None of those is the fill. The wrapper (deployed devnet
 * commit 7c906e45, `src/v16_program.rs`):
 *
 *  1. reads the asset's `effective_price` from the market (the "oracle price"),
 *  2. clips the requested size to the LP's headroom; a clip to ZERO returns Ok(()) WITHOUT
 *     calling the matcher: the transaction succeeds and nothing changes (handle_trade_cpi),
 *  3. CPIs the matcher with (req_id, asset_index, lp_account_id, oracle_price_e6, req_size
 *     = the CLIPPED size, ext) — matcher instruction tag 0 (single) / tag 3 (batch),
 *  4. the matcher answers with exec_size (<= req_size, 0 allowed) and exec_price. The single
 *     route reads that answer from the matcher CONTEXT ACCOUNT (offset 0, 64 bytes); the batch
 *     route reads it from `set_return_data`,
 *  5. books exec_size at the asset's `effective_price` — NOT at the matcher's exec_price
 *     (F-TRADENOCPI-FEE comment in handle_trade_nocpi_zero_copy). exec_price only feeds the
 *     taker limit check, the oracle band and hybrid-mark discovery.
 *
 * So, from the transaction alone:
 *  - the booked PRICE is the `oracle_price_e6` the wrapper passed to the matcher: exact;
 *  - a ZERO fill by headroom clip is provable: there is no matcher CPI at all;
 *  - the size handed to the matcher (post clip) is an UPPER BOUND of the executed size;
 *  - the executed size itself is NOT in the transaction: single route writes it to the matcher
 *    context account only (no log, no return data), and the wrapper logs nothing. It is
 *    recoverable only from that account (valid until the LP's next trade overwrites it, and
 *    only when its `req_id` equals the one in this transaction's matcher CPI) or, for a batch,
 *    from `meta.returnData` when present.
 */

import { decodeBase58, createLogger } from "@percolatorct/shared";

let _logger: ReturnType<typeof createLogger> | null = null;
const summaryLogger = () => (_logger ??= createLogger("indexer:tradecpi-fill"));

export const MATCHER_RETURN_BYTES = 64;
/** Account index of the matcher program in TradeCpi / BatchTradeCpi (`[5]` is its context). */
export const CPI_MATCHER_PROGRAM_ACCOUNT_IDX = 4;
export const CPI_MATCHER_CONTEXT_ACCOUNT_IDX = 5;

const MATCHER_TAG_SINGLE = 0;
const MATCHER_TAG_BATCH = 3;
const SINGLE_CALL_MIN_LEN = 43;
const BATCH_HEADER_LEN = 18;
const BATCH_LEG_LEN = 26;

const FLAG_VALID = 1;
const FLAG_REJECTED = 4;

export interface MatcherCallLeg {
  assetIndex: number;
  /** The wrapper's `effective_price` for the asset: the price the engine books the fill at. */
  oraclePriceE6: bigint;
  /** Signed size handed to the matcher (already clipped to LP headroom). */
  reqSize: bigint;
}

export interface MatcherCall {
  batch: boolean;
  reqId: bigint;
  lpAccountId: bigint;
  legs: MatcherCallLeg[];
}

export interface MatcherReturn {
  abiVersion: number;
  flags: number;
  execPriceE6: bigint;
  /** Signed. 0 = the matcher declined (a zero fill: no position change). */
  execSize: bigint;
  reqId: bigint;
  lpAccountId: bigint;
  oraclePriceE6: bigint;
  assetIndex: bigint;
}

function dv(b: Uint8Array): DataView {
  return new DataView(b.buffer, b.byteOffset, b.byteLength);
}
function i128(v: DataView, off: number): bigint {
  const lo = v.getBigUint64(off, true);
  const hi = v.getBigInt64(off + 8, true);
  return (hi << 64n) | lo;
}

/** Decode the matcher instruction the wrapper sent (tag 0 single, tag 3 batch). */
export function decodeMatcherCall(data: Uint8Array): MatcherCall | null {
  if (data.length < 1) return null;
  const v = dv(data);
  if (data[0] === MATCHER_TAG_SINGLE) {
    if (data.length < SINGLE_CALL_MIN_LEN) return null;
    return {
      batch: false,
      reqId: v.getBigUint64(1, true),
      lpAccountId: v.getBigUint64(11, true),
      legs: [{ assetIndex: v.getUint16(9, true), oraclePriceE6: v.getBigUint64(19, true), reqSize: i128(v, 27) }],
    };
  }
  if (data[0] === MATCHER_TAG_BATCH) {
    if (data.length < BATCH_HEADER_LEN) return null;
    const n = data[1];
    if (data.length < BATCH_HEADER_LEN + n * BATCH_LEG_LEN) return null;
    const legs: MatcherCallLeg[] = [];
    for (let i = 0; i < n; i++) {
      const o = BATCH_HEADER_LEN + i * BATCH_LEG_LEN;
      legs.push({ assetIndex: v.getUint16(o, true), oraclePriceE6: v.getBigUint64(o + 2, true), reqSize: i128(v, o + 10) });
    }
    return { batch: true, reqId: v.getBigUint64(2, true), lpAccountId: v.getBigUint64(10, true), legs };
  }
  return null;
}

/** Decode one 64-byte MatcherReturn (matcher context offset 0, or one chunk of batch return data). */
export function decodeMatcherReturn(buf: Uint8Array): MatcherReturn | null {
  if (buf.length < MATCHER_RETURN_BYTES) return null;
  const v = dv(buf);
  return {
    abiVersion: v.getUint32(0, true),
    flags: v.getUint32(4, true),
    execPriceE6: v.getBigUint64(8, true),
    execSize: i128(v, 16),
    reqId: v.getBigUint64(32, true),
    lpAccountId: v.getBigUint64(40, true),
    oraclePriceE6: v.getBigUint64(48, true),
    assetIndex: v.getBigUint64(56, true),
  };
}

/** A CPI'd instruction in whatever source shape, reduced to what we need. */
export interface InnerIxLike {
  programId: string;
  /** base58 instruction data */
  data: string;
}

export type CpiEvidence =
  /** The source did not include inner instructions: nothing can be proven. */
  | { kind: "unknown"; /** the instruction is a BatchTradeCpi (which always calls the matcher) */ batch?: boolean }
  /** Inner instructions are present and the wrapper never called the matcher: zero fill. */
  | { kind: "no-matcher-call" }
  | { kind: "call"; call: MatcherCall; matcherContext: string | undefined; batchReturns: MatcherReturn[] | null };

/**
 * @param outerAccounts the wrapper instruction's account list (`[4]` matcher program, `[5]` context)
 * @param inner         that instruction's inner instructions; `null`/`undefined` = not provided by the source
 * @param returnData    transaction-level return data, if the source has it
 * @param isBatch       the wrapper instruction is a BatchTradeCpi. A batch ALWAYS calls the matcher
 *                      (no headroom clip, a zero-size leg reverts), so a missing call there means
 *                      incomplete inner-instruction data, NOT a zero fill: `unknown`.
 */
export function cpiEvidence(
  outerAccounts: readonly string[],
  inner: readonly InnerIxLike[] | null | undefined,
  returnData: { programId: string; data: Uint8Array } | null | undefined,
  isBatch: boolean,
): CpiEvidence {
  const unknown: CpiEvidence = isBatch ? { kind: "unknown", batch: true } : { kind: "unknown" };
  if (!inner) return unknown;
  const matcherProgram = outerAccounts[CPI_MATCHER_PROGRAM_ACCOUNT_IDX];
  if (!matcherProgram) return unknown;
  for (const ix of inner) {
    if (ix.programId !== matcherProgram) continue;
    const bytes = decodeBase58(ix.data);
    const call = bytes ? decodeMatcherCall(bytes) : null;
    if (!call || call.batch !== isBatch) return unknown;
    let batchReturns: MatcherReturn[] | null = null;
    if (call.batch && returnData && returnData.programId === matcherProgram && returnData.data.length === call.legs.length * MATCHER_RETURN_BYTES) {
      batchReturns = call.legs.map((_, i) => decodeMatcherReturn(returnData.data.subarray(i * MATCHER_RETURN_BYTES, (i + 1) * MATCHER_RETURN_BYTES))!);
    }
    return { kind: "call", call, matcherContext: outerAccounts[CPI_MATCHER_CONTEXT_ACCOUNT_IDX], batchReturns };
  }
  return isBatch ? unknown : { kind: "no-matcher-call" };
}

/**
 * - `zero-fill`: provably nothing changed on chain. No row, nothing to report.
 * - `size-unverified`: only under the strict policy (`TRADECPI_UNVERIFIED_SIZE=skip`). No row;
 *   callers record the signature as skipped.
 */
export type CpiSkipReason = "zero-fill" | "size-unverified";

export type CpiLegResolution =
  | { kind: "skip"; reason: CpiSkipReason; detail: string }
  /**
   * The matcher call could not be decoded, or does not line up with the instruction (a wrapper
   * layout this parser does not know, inner instructions absent from the source). The caller must
   * do exactly what the indexer did before this parser existed: the wire size and its existing
   * price logic. Counted as `legacy`.
   */
  | { kind: "legacy"; detail: string }
  /** Only with `onReadError: "report"`: the context read failed in transport; the caller decides (retry). */
  | { kind: "read-error"; detail: string }
  /**
   * `exact: true`: the matcher's own answer. `exact: false`: the executed size could not be proven;
   * `sizeValue` is the matcher-requested (post headroom clip) size, an UPPER BOUND of what was
   * executed, at the exact booked price.
   */
  | { kind: "fill"; sizeValue: bigint; priceE6: bigint; exact: boolean };

/** Result of reading the matcher context: `ok` (the read worked; `ret` may be anything) or a transport error. */
export type ContextRead = { kind: "ok"; ret: MatcherReturn | null } | { kind: "error"; detail: string };
export type ReadMatcherContext = (address: string) => Promise<ContextRead>;

// ---- counters + summary log (visibility) -------------------------------------------------------
export interface TradecpiCounters {
  /** executed size proven by the matcher's own answer */
  exact: number;
  /** written with the matcher-requested size (an upper bound) */
  unverified: number;
  /** provably nothing executed: no row */
  zeroFill: number;
  /** strict policy: not written */
  skipped: number;
  /** context reads that failed in transport and ended as a fallback (subset of `unverified`/`skipped`) */
  readError: number;
  /** undecodable evidence: written as the indexer did before this parser (wire size, legacy price) */
  legacy: number;
  /** subset of `legacy`: a BatchTradeCpi of a recognised wrapper whose inner instructions lack the matcher call */
  legacyBatch: number;
}
const zero = (): TradecpiCounters => ({ exact: 0, unverified: 0, zeroFill: 0, skipped: 0, readError: 0, legacy: 0, legacyBatch: 0 });
let counters = zero();
let lastSummary = { at: Date.now(), total: 0 };
const SUMMARY_EVERY_MS = 60_000;
const SUMMARY_EVERY_FILLS = 500;
const totalOf = (c: TradecpiCounters): number => c.exact + c.unverified + c.zeroFill + c.skipped + c.legacy;

/** Process-lifetime counters (`tradecpi_size_unverified_total` is `unverified`). */
export function getTradecpiCounters(): TradecpiCounters {
  return { ...counters };
}
export function getTradecpiSizeUnverifiedCount(): number {
  return counters.unverified;
}
/** Test hook. */
export function resetTradecpiCounters(): void {
  counters = zero();
  lastSummary = { at: Date.now(), total: 0 };
}
/** Info-level summary every 60 s or 500 fills, whichever comes first. */
function note(kind: keyof TradecpiCounters, alsoReadError = false, alsoLegacyBatch = false): void {
  counters[kind]++;
  if (alsoReadError) counters.readError++;
  if (alsoLegacyBatch) counters.legacyBatch++;
  const total = totalOf(counters);
  if (total - lastSummary.total >= SUMMARY_EVERY_FILLS || Date.now() - lastSummary.at >= SUMMARY_EVERY_MS) {
    summaryLogger().info("TradeCpi fill summary (process lifetime)", { ...counters, sinceLast: total - lastSummary.total });
    lastSummary = { at: Date.now(), total };
  }
}

/**
 * What to do when the executed size cannot be proven (matcher context overwritten or unreadable).
 * `request` (DEFAULT): write the matcher-requested (post-clip) size at the booked price: an upper
 * bound, but strictly closer to the truth than the wire size at the mark EWMA the indexer used to
 * write, and the user's trade stays in their history. `skip` (opt-in strict): write nothing and
 * record the signature in skipped_signatures.
 */
export type UnverifiedSizePolicy = "skip" | "request";

export function unverifiedSizePolicy(env: Record<string, string | undefined> = process.env): UnverifiedSizePolicy {
  return env.TRADECPI_UNVERIFIED_SIZE?.trim().toLowerCase() === "skip" ? "skip" : "request";
}

function abs(n: bigint): bigint {
  return n < 0n ? -n : n;
}

function returnMatches(r: MatcherReturn, call: MatcherCall, leg: MatcherCallLeg): boolean {
  return (
    (r.flags & FLAG_VALID) !== 0 &&
    (r.flags & FLAG_REJECTED) === 0 &&
    r.reqId === call.reqId &&
    r.lpAccountId === call.lpAccountId &&
    r.assetIndex === BigInt(leg.assetIndex) &&
    r.oraclePriceE6 === leg.oraclePriceE6 &&
    abs(r.execSize) <= abs(leg.reqSize) &&
    (r.execSize === 0n || (r.execSize > 0n) === (leg.reqSize > 0n))
  );
}

/**
 * Executed size and booked price of one wire leg of a TradeCpi / BatchTradeCpi.
 *
 * @param legPos position of the leg inside its instruction (0 for a single fill)
 */
export async function resolveCpiLeg(args: {
  evidence: CpiEvidence;
  assetIndex: number;
  side: "long" | "short";
  wireSizeAbs: bigint;
  legPos: number;
  readContext: ReadMatcherContext;
  policy?: UnverifiedSizePolicy;
  /**
   * `fallback` (default): a transport error on the context read is treated like "not matched" and
   * the policy applies (and the failure is counted as `readError`). `report`: return `read-error`
   * so the caller can retry once the read may succeed (the unique index makes a written size permanent).
   */
  onReadError?: "fallback" | "report";
  /** for the warn log of a legacy batch */
  signature?: string;
}): Promise<CpiLegResolution> {
  const { evidence } = args;
  const policy = args.policy ?? unverifiedSizePolicy();
  if (evidence.kind === "no-matcher-call") {
    note("zeroFill");
    return { kind: "skip", reason: "zero-fill", detail: "the wrapper clipped the request to zero LP headroom and never called the matcher: no position change" };
  }
  if (evidence.kind === "unknown") {
    note("legacy", false, evidence.batch === true);
    if (evidence.batch === true) {
      summaryLogger().warn("BatchTradeCpi without a decodable matcher call: written as the legacy row (wire size, legacy price)", { signature: args.signature });
    }
    return { kind: "legacy", detail: "no inner instructions in this source (or an undecodable / mismatched matcher call): booked price and executed size cannot be read" };
  }
  const { call } = evidence;
  const leg = call.legs[args.legPos];
  const wireSigned = args.side === "long" ? args.wireSizeAbs : -args.wireSizeAbs;
  if (
    !leg ||
    leg.assetIndex !== args.assetIndex ||
    leg.oraclePriceE6 === 0n ||
    leg.reqSize === 0n ||
    (leg.reqSize > 0n) !== (wireSigned > 0n) ||
    abs(leg.reqSize) > args.wireSizeAbs
  ) {
    note("legacy");
    return { kind: "legacy", detail: "the matcher call does not line up with the instruction's leg" };
  }

  let ret: MatcherReturn | null = null;
  let readFailed: string | null = null;
  if (evidence.batchReturns) ret = evidence.batchReturns[args.legPos] ?? null;
  else if (!call.batch && evidence.matcherContext) {
    const read = await args.readContext(evidence.matcherContext);
    if (read.kind === "error") readFailed = read.detail;
    else ret = read.ret;
  }
  if (ret && returnMatches(ret, call, leg)) {
    if (ret.execSize === 0n) {
      note("zeroFill");
      return { kind: "skip", reason: "zero-fill", detail: "the matcher returned exec_size 0 for this request: no position change" };
    }
    note("exact");
    return { kind: "fill", sizeValue: abs(ret.execSize), priceE6: leg.oraclePriceE6, exact: true };
  }
  if (readFailed !== null && args.onReadError === "report") {
    return { kind: "read-error", detail: readFailed };
  }

  if (policy === "request") {
    // Upper bound only (the matcher may have filled less); price is still the booked one.
    // The size is the POST-CLIP request (`reqSize`), never the instruction's wire size.
    note("unverified", readFailed !== null);
    return { kind: "fill", sizeValue: abs(leg.reqSize), priceE6: leg.oraclePriceE6, exact: false };
  }
  note("skipped", readFailed !== null);
  return {
    kind: "skip",
    reason: "size-unverified",
    detail: `executed size not in the transaction and the matcher context no longer holds req_id ${call.reqId} (requested ${abs(leg.reqSize)} after headroom clip, instruction asked ${args.wireSizeAbs})`,
  };
}
