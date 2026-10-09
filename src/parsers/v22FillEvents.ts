/**
 * v2.2 wrapper LOG events: FILL (executed fills), REDUCE (forced / unilateral reductions) and MOVE (value moves,
 * including sub 4 `G9_RESTORE_PNL`). Source of truth for the encoding and the trust model is the wrapper's
 * `docs/v22-fill-events.md` (release/v22-wrapper-rem c6ee0b6e, `src/fill_events_v22.rs`); the reference walker is
 * `tests/support/fill_events.rs` (`wrapper_frame_tokens`, `tx_events`, `decode`). This file follows both.
 *
 * THE ATTRIBUTION RULE (security review 2026-10-07, M-1: the first rule was forgeable by any LP's matcher):
 *   - only successful transactions (`meta.err == null`) are read;
 *   - each `logMessages` element is ONE atomic line: never joined and re-split (one `msg!` can hold newlines);
 *   - a frame is pushed only on `Program <canonical base58 pubkey> invoke [n]` with n = stack depth + 1, and popped
 *     only on `Program <id on top of the stack> success` or a line starting `Program <that id> failed: `;
 *   - a `Program data: ` line belongs to the program on top of the stack and is taken only when that is a pinned
 *     wrapper program id (configuration, never read from the transaction);
 *   - ANY inconsistency makes the WHOLE transaction's events UNKNOWN, as do the `Log truncated` element and null / empty
 *     logs. Unknown is never "no fill": it is recorded as an `events_unknown` marker so the positions can be
 *     reconciled from account state.
 *
 * WHAT IS TRUSTED: `executed_q`, `price_e6`, `fee_atoms`, `backing_fee_atoms`, the keys, `flags`, `asset_gen` and every REDUCE
 * and MOVE field are the wrapper's own statements. `requested_q` (instruction data) and `quoted_price_e6` (the matcher's
 * answer, an LP-chosen program) are copied from untrusted input: they are stored only under `*_untrusted` names and
 * never feed volume. VOLUME IS `executed_q` at `price_e6` only. `fee_atoms` is the total across both portfolios and any
 * LP-requested fee: not "the taker's fee", not protocol revenue.
 *
 * What the decoder does with what it does not know: an unknown kind, version, MOVE sub or REDUCE reason, a known kind of
 * the wrong length, or a token that is not base64 is SKIPPED (counted), never an error, and never guessed at.
 */
import { PublicKey } from "@solana/web3.js";
import { IX_TAG, IX_TAG_P2B, IX_TAG_V22, LAYOUT_V22 } from "@percolatorct/sdk";
import type { RawInstruction, V22EventRow } from "./v22Events.js";

// ── Constants (wrapper `fill_events_v22.rs`) ───────────────────────────────────────────────────────────────────────

export const FILL_EVENT_KIND = { FILL: 1, REDUCE: 2, MOVE: 3 } as const;
/** The one event layout version this decoder knows. Anything else is skipped. */
export const FILL_EVENT_VERSION = 1;
export const EVENT_HEADER_LEN = 35;
export const FILL_FIXED_LEN = EVENT_HEADER_LEN + 32 + 32 + 1; // 100
export const FILL_REC_LEN = 75;
export const REDUCE_LEN = 134;
export const MOVE_LEN = 62;
/** `NO_ASSET`: a MOVE that is not about one asset. */
export const NO_ASSET = 0xffff;
export const LOG_TRUNCATED = "Log truncated";
/** Engine position units per whole unit (`POS_SCALE`). */
export const POS_SCALE = 1_000_000n;

export const FILL_FLAG = { CLIPPED: 1, PARTIAL: 2, ZERO: 4, MATCHER: 8 } as const;

export const REDUCE_REASONS: Readonly<Record<number, { name: string; units: "basis" | "adl_effective" | "vault_lp_trade" }>> = {
  1: { name: "rebalance_reduce", units: "basis" },
  2: { name: "adl_wind_down", units: "adl_effective" },
  3: { name: "liquidation", units: "adl_effective" },
  4: { name: "dust_sweep", units: "vault_lp_trade" },
  5: { name: "eviction", units: "vault_lp_trade" },
};

export const MOVE_SUBS: Readonly<Record<number, string>> = { 1: "earn_exit", 2: "g9", 3: "rent_routed", 4: "g9_restore_pnl" };

/**
 * Log-event rows live in the same table as the instruction events and share its unique key
 * (signature, ix_index, inner_index, network). Real inner-instruction indices are tiny, so a log event takes
 * `inner_index = LOG_EVENT_INNER_BASE + ordinal`, where `ordinal` is the row's position among the transaction's log-event rows.
 */
export const LOG_EVENT_INNER_BASE = 1_000_000;

/** Wrapper instructions that emit events. A transaction without one of them is not expected to carry any. */
/**
 * SDK tag lookups are guarded: a test double of the SDK (or a stripped build) may lack a group, and a module-load throw here would
 * take every importer down. A tag that cannot be resolved is simply absent; a test pins the full set against the real SDK.
 */
const sdkTag = (get: () => number | undefined): number | undefined => {
  try {
    return get();
  } catch {
    return undefined;
  }
};
export const EVENT_EMITTING_TAGS: ReadonlySet<number> = new Set<number>(
  [
    sdkTag(() => IX_TAG.PermissionlessCrank), // liquidation REDUCE (best effort)
    sdkTag(() => IX_TAG.TradeNoCpi),
    sdkTag(() => IX_TAG.TradeCpi),
    sdkTag(() => IX_TAG.RebalanceReduce),
    sdkTag(() => IX_TAG.BatchTradeNoCpi),
    sdkTag(() => IX_TAG.BatchTradeCpi),
    sdkTag(() => IX_TAG.ExecuteRedemption),
    sdkTag(() => IX_TAG_P2B.AdlWindDown),
    sdkTag(() => IX_TAG_V22.SettleHoldingRent),
    sdkTag(() => IX_TAG_V22.InsuranceBackstopDraw),
    sdkTag(() => IX_TAG_V22.SweepBandDustLeg),
    sdkTag(() => IX_TAG_V22.EvictAndTradeCpi),
  ].filter((t): t is number => typeof t === "number"),
);
/** The subset whose missing event is worth an `events_unknown` marker (a crank is far too frequent, and its event is best effort). */
const MARKER_TAGS: ReadonlySet<number> = new Set([...EVENT_EMITTING_TAGS].filter((t) => t !== sdkTag(() => IX_TAG.PermissionlessCrank)));

// ── Attribution ────────────────────────────────────────────────────────────────────────────────────────────────────

export type UnknownReason = "no_logs" | "truncated" | "bad_invoke_depth" | "bad_return" | "data_outside_frame" | "unclosed_frame" | "logs_malformed";

export interface AttributedToken {
  b64: string;
  /** Top-level instruction index (the number of depth-1 frames opened before this line, minus one). */
  ixIndex: number;
}

export type Attribution = { ok: true; tokens: AttributedToken[] } | { ok: false; reason: UnknownReason };

/** `Program <id> <tail>`: id must be the canonical base58 text of a 32-byte key. `Program log: ..` / `Program data: ..` never match. */
function runtimeProgramLine(line: string): { id: string; tail: string } | null {
  if (!line.startsWith("Program ")) return null;
  const rest = line.slice("Program ".length);
  const sp = rest.indexOf(" ");
  if (sp <= 0) return null;
  const id = rest.slice(0, sp);
  if (id.length < 32 || id.length > 44) return null;
  try {
    if (new PublicKey(id).toBase58() !== id) return null;
  } catch {
    return null;
  }
  return { id, tail: rest.slice(sp + 1) };
}

/** `invoke [n]`: decimal digits only, nothing after the bracket. */
function invokeDepth(tail: string): number | null {
  const m = /^invoke \[(\d+)\]$/.exec(tail);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isSafeInteger(n) ? n : null;
}

/**
 * The strict frame walk. `logs` is `meta.logMessages`; `wrapperIds` the pinned wrapper program ids. Returns the base64
 * tokens printed in a wrapper frame (in order), or why the transaction's events are unknown.
 */
export function attributeWrapperTokens(logs: unknown, wrapperIds: ReadonlySet<string>): Attribution {
  if (!Array.isArray(logs) || logs.length === 0) return { ok: false, reason: "no_logs" };
  for (const l of logs) if (typeof l !== "string") return { ok: false, reason: "logs_malformed" };
  const lines = logs as string[];
  if (lines.some((l) => l === LOG_TRUNCATED)) return { ok: false, reason: "truncated" };

  const stack: string[] = [];
  const out: AttributedToken[] = [];
  let topLevel = -1;
  for (const line of lines) {
    const rp = runtimeProgramLine(line);
    if (rp) {
      const depth = invokeDepth(rp.tail);
      if (depth !== null) {
        if (depth !== stack.length + 1) return { ok: false, reason: "bad_invoke_depth" };
        if (depth === 1) topLevel++;
        stack.push(rp.id);
      } else if (rp.tail === "success" || rp.tail.startsWith("failed: ")) {
        if (stack[stack.length - 1] !== rp.id) return { ok: false, reason: "bad_return" };
        stack.pop();
      }
      // any other runtime line about a program (`consumed N of M compute units`) is ignored
      continue;
    }
    if (line.startsWith("Program data: ")) {
      const top = stack[stack.length - 1];
      if (top === undefined) return { ok: false, reason: "data_outside_frame" };
      if (wrapperIds.has(top)) {
        for (const t of line.slice("Program data: ".length).split(" ")) if (t !== "") out.push({ b64: t, ixIndex: topLevel });
      }
    }
  }
  if (stack.length > 0) return { ok: false, reason: "unclosed_frame" };
  return { ok: true, tokens: out };
}

// ── Token decoding ─────────────────────────────────────────────────────────────────────────────────────────────────

export interface FillRecord {
  assetIndex: number;
  assetGen: bigint;
  flags: number;
  /** UNTRUSTED (instruction data). */
  requestedQ: bigint;
  executedQ: bigint;
  priceE6: bigint;
  /** UNTRUSTED (the matcher's quote, or the caller's wire price). */
  quotedPriceE6: bigint;
  /** Total engine trade fee across both portfolios, saturating at u64::MAX. */
  feeAtoms: bigint;
  backingFeeAtoms: bigint;
}

export type DecodedFillEvent =
  | { kind: "fill"; ixTag: number; market: string; taker: string; lp: string; recs: FillRecord[] }
  | { kind: "reduce"; ixTag: number; market: string; portfolio: string; counterparty: string | null; assetIndex: number; assetGen: bigint; reason: number; signedReducedQ: bigint; priceE6: bigint }
  | { kind: "move"; ixTag: number; market: string; sub: number; assetIndex: number | null; a: bigint; b: bigint; c: bigint };

export type SkipReason = "malformed" | "unknown_kind" | "unknown_version" | "bad_length" | "unknown_sub" | "unknown_reason";

const B64 = /^[A-Za-z0-9+/]*={0,2}$/;

function dv(b: Uint8Array): DataView {
  return new DataView(b.buffer, b.byteOffset, b.byteLength);
}
const u16 = (b: Uint8Array, o: number): number => dv(b).getUint16(o, true);
const u64 = (b: Uint8Array, o: number): bigint => dv(b).getBigUint64(o, true);
/** i128 two's complement, little-endian. */
const i128 = (b: Uint8Array, o: number): bigint => {
  const v = dv(b);
  const lo = v.getBigUint64(o, true);
  const hi = v.getBigInt64(o + 8, true);
  return (hi << 64n) | lo;
};
const key = (b: Uint8Array, o: number): string => new PublicKey(b.subarray(o, o + 32)).toBase58();
const ZERO_KEY = new Uint8Array(32);
const isZeroKey = (b: Uint8Array, o: number): boolean => b.subarray(o, o + 32).every((x, i) => x === ZERO_KEY[i]);

/**
 * Decode the raw bytes behind one token. Kind and version are bytes 0 and 1 in every version and are checked before any
 * length. Returns the event, or the reason it was skipped.
 */
export function decodeFillEventBytes(b: Uint8Array): { ok: true; event: DecodedFillEvent } | { ok: false; skip: SkipReason } {
  if (b.length < 2) return { ok: false, skip: "malformed" };
  if (b[0] < 1 || b[0] > 3) return { ok: false, skip: "unknown_kind" };
  if (b[1] !== FILL_EVENT_VERSION) return { ok: false, skip: "unknown_version" };
  if (b.length < EVENT_HEADER_LEN) return { ok: false, skip: "bad_length" };
  const ixTag = b[2];
  const market = key(b, 3);
  if (b[0] === FILL_EVENT_KIND.FILL) {
    if (b.length < FILL_FIXED_LEN) return { ok: false, skip: "bad_length" };
    const n = b[99];
    if (b.length !== FILL_FIXED_LEN + FILL_REC_LEN * n) return { ok: false, skip: "bad_length" };
    const recs: FillRecord[] = [];
    for (let i = 0; i < n; i++) {
      const o = FILL_FIXED_LEN + FILL_REC_LEN * i;
      recs.push({
        assetIndex: u16(b, o),
        assetGen: u64(b, o + 2),
        flags: b[o + 10],
        requestedQ: i128(b, o + 11),
        executedQ: i128(b, o + 27),
        priceE6: u64(b, o + 43),
        quotedPriceE6: u64(b, o + 51),
        feeAtoms: u64(b, o + 59),
        backingFeeAtoms: u64(b, o + 67),
      });
    }
    return { ok: true, event: { kind: "fill", ixTag, market, taker: key(b, 35), lp: key(b, 67), recs } };
  }
  if (b[0] === FILL_EVENT_KIND.REDUCE) {
    if (b.length !== REDUCE_LEN) return { ok: false, skip: "bad_length" };
    const reason = b[109];
    if (!(reason in REDUCE_REASONS)) return { ok: false, skip: "unknown_reason" };
    return {
      ok: true,
      event: {
        kind: "reduce",
        ixTag,
        market,
        portfolio: key(b, 35),
        counterparty: isZeroKey(b, 67) ? null : key(b, 67),
        assetIndex: u16(b, 99),
        assetGen: u64(b, 101),
        reason,
        signedReducedQ: i128(b, 110),
        priceE6: u64(b, 126),
      },
    };
  }
  if (b.length !== MOVE_LEN) return { ok: false, skip: "bad_length" };
  const sub = b[35];
  if (!(sub in MOVE_SUBS)) return { ok: false, skip: "unknown_sub" };
  const asset = u16(b, 36);
  return { ok: true, event: { kind: "move", ixTag, market, sub, assetIndex: asset === NO_ASSET ? null : asset, a: u64(b, 38), b: u64(b, 46), c: u64(b, 54) } };
}

/** Decode one base64 token (the text after `Program data: `). */
export function decodeFillEventToken(token: string): { ok: true; event: DecodedFillEvent } | { ok: false; skip: SkipReason } {
  if (token.length === 0 || !B64.test(token)) return { ok: false, skip: "malformed" };
  return decodeFillEventBytes(new Uint8Array(Buffer.from(token, "base64")));
}

// ── Rows ───────────────────────────────────────────────────────────────────────────────────────────────────────────

/** Notional of an executed size in e6 quote units: |executed_q| x price_e6 / POS_SCALE. The ONLY volume figure; never requested size or a quote. */
export function fillNotionalE6(executedQ: bigint, priceE6: bigint): bigint {
  const abs = executedQ < 0n ? -executedQ : executedQ;
  return (abs * priceE6) / POS_SCALE;
}

const abs = (v: bigint): bigint => (v < 0n ? -v : v);

export interface EventGate {
  /** True when `slab` is a market in the indexer's registry. */
  isKnownMarket(slab: string): boolean;
  /** Last read wrapper VERSION of the market, or null. */
  versionOf(slab: string): number | null;
  /** Last read generation of the asset, or null. */
  generationOf(slab: string, assetIndex: number): bigint | null;
}

export interface LogEventContext {
  signature: string;
  slot?: number | null;
  blockTimeSec?: number | null;
}

export interface LogEventStats {
  /** Transactions whose logs were read (they held a wrapper instruction that emits events). */
  txsRead: number;
  rows: number;
  unknownByReason: Partial<Record<UnknownReason, number>>;
  skippedByReason: Partial<Record<SkipReason, number>>;
  /** Events whose market is not in the registry. */
  droppedUnknownMarket: number;
  /** Events whose market is known to be another wrapper VERSION. */
  droppedWrongVersion: number;
  /** Events whose asset_gen differs from the generation last read (kept, flagged). */
  generationMismatch: number;
}

const freshStats = (): LogEventStats => ({ txsRead: 0, rows: 0, unknownByReason: {}, skippedByReason: {}, droppedUnknownMarket: 0, droppedWrongVersion: 0, generationMismatch: 0 });
let stats = freshStats();
export function getLogEventStats(): LogEventStats {
  return JSON.parse(JSON.stringify(stats, (_k, v) => (typeof v === "bigint" ? v.toString() : v))) as LogEventStats;
}
export function resetLogEventStats(): void {
  stats = freshStats();
}
const bump = <K extends string>(m: Partial<Record<K, number>>, k: K): void => {
  m[k] = (m[k] ?? 0) + 1;
};

export type LogEventsResult =
  | { status: "not_applicable" | "failed"; rows: [] }
  | { status: "ok"; rows: V22EventRow[] }
  | { status: "unknown"; reason: UnknownReason; rows: V22EventRow[] };

/**
 * Decode a transaction's wrapper log events into `v22_events` rows.
 *
 * `wrapperInstructions` are the transaction's instructions of the pinned wrapper program ids (top-level and inner, as
 * `rawInstructionsFromParsedTx` returns them); a transaction without an event-emitting tag among them is `not_applicable`
 * and its logs are not read. A failed transaction yields nothing. Unknown events (truncated / null logs, inconsistent
 * frames) yield one `events_unknown` marker per known v2.2 market the transaction's wrapper instructions name, so the
 * positions can be reconciled from account state; they never yield "no fill".
 *
 * Every row carries `source: "log"`. Pure apart from the module's counters.
 */
export function decodeV22LogEvents(
  tx: { err: unknown; logMessages: unknown },
  wrapperInstructions: readonly RawInstruction[],
  wrapperIds: ReadonlySet<string>,
  gate: EventGate,
  ctx: LogEventContext,
): LogEventsResult {
  if (tx.err != null) return { status: "failed", rows: [] };
  if (!wrapperInstructions.some((i) => i.data.length > 0 && EVENT_EMITTING_TAGS.has(i.data[0]))) return { status: "not_applicable", rows: [] };
  stats.txsRead++;
  const base = {
    signature: ctx.signature,
    slot: ctx.slot ?? null,
    block_time: ctx.blockTimeSec != null ? new Date(ctx.blockTimeSec * 1000).toISOString() : null,
  };
  let ordinal = 0;
  const mk = (ixIndex: number, r: Omit<V22EventRow, "signature" | "ix_index" | "inner_index" | "slot" | "block_time">): V22EventRow => ({
    ...base,
    ix_index: ixIndex,
    inner_index: LOG_EVENT_INNER_BASE + ordinal++,
    ...r,
  });

  const att = attributeWrapperTokens(tx.logMessages, wrapperIds);
  if (!att.ok) {
    bump(stats.unknownByReason, att.reason);
    // One marker per known v2.2 market named by a wrapper instruction that should have emitted (VERSION-keyed: a market not known to be v2.2 gets none).
    const markets = new Set<string>();
    for (const ix of wrapperInstructions) {
      if (ix.data.length === 0 || !MARKER_TAGS.has(ix.data[0])) continue;
      for (const a of ix.accounts) if (a && gate.isKnownMarket(a) && gate.versionOf(a) === LAYOUT_V22.version) markets.add(a);
    }
    const rows = [...markets].sort().map((m) =>
      mk(-1, {
        kind: "events_unknown",
        slab_address: m,
        asset_index: null,
        actor: null,
        subject: null,
        amount: null,
        detail: { source: "log", reason: att.reason, wrapper_instructions: wrapperInstructions.length, reconcile: "from_account_state" },
      }),
    );
    stats.rows += rows.length;
    return { status: "unknown", reason: att.reason, rows };
  }

  const rows: V22EventRow[] = [];
  const accept = (market: string): boolean => {
    if (!gate.isKnownMarket(market)) {
      stats.droppedUnknownMarket++;
      return false;
    }
    const v = gate.versionOf(market);
    if (v !== null && v !== LAYOUT_V22.version) {
      stats.droppedWrongVersion++;
      return false;
    }
    return true;
  };
  const genCurrent = (market: string, asset: number | null, gen: bigint): boolean | null => {
    if (asset === null) return null;
    const cur = gate.generationOf(market, asset);
    if (cur === null) return null;
    if (cur !== gen) stats.generationMismatch++;
    return cur === gen;
  };

  for (const t of att.tokens) {
    const d = decodeFillEventToken(t.b64);
    if (!d.ok) {
      bump(stats.skippedByReason, d.skip);
      continue;
    }
    const e = d.event;
    if (!accept(e.market)) continue;
    if (e.kind === "fill") {
      e.recs.forEach((r, i) => {
        rows.push(
          mk(t.ixIndex, {
            kind: "fill_event",
            slab_address: e.market,
            asset_index: r.assetIndex,
            actor: e.taker,
            subject: e.lp,
            amount: abs(r.executedQ).toString(),
            detail: {
              source: "log",
              event_version: FILL_EVENT_VERSION,
              ix_tag: e.ixTag,
              asset_gen: r.assetGen.toString(),
              gen_current: genCurrent(e.market, r.assetIndex, r.assetGen),
              rec_index: i,
              recs_in_line: e.recs.length,
              flags: r.flags,
              clipped: (r.flags & FILL_FLAG.CLIPPED) !== 0,
              partial: (r.flags & FILL_FLAG.PARTIAL) !== 0,
              zero: (r.flags & FILL_FLAG.ZERO) !== 0,
              matcher: (r.flags & FILL_FLAG.MATCHER) !== 0,
              executed_q: r.executedQ.toString(),
              price_e6: r.priceE6.toString(),
              notional_e6: fillNotionalE6(r.executedQ, r.priceE6).toString(),
              fee_atoms_total: r.feeAtoms.toString(),
              backing_fee_atoms: r.backingFeeAtoms.toString(),
              requested_q_untrusted: r.requestedQ.toString(),
              quoted_price_e6_untrusted: r.quotedPriceE6.toString(),
            },
          }),
        );
      });
    } else if (e.kind === "reduce") {
      const why = REDUCE_REASONS[e.reason];
      rows.push(
        mk(t.ixIndex, {
          kind: "reduce_event",
          slab_address: e.market,
          asset_index: e.assetIndex,
          actor: e.portfolio,
          subject: e.counterparty,
          amount: abs(e.signedReducedQ).toString(),
          detail: {
            source: "log",
            event_version: FILL_EVENT_VERSION,
            ix_tag: e.ixTag,
            asset_gen: e.assetGen.toString(),
            gen_current: genCurrent(e.market, e.assetIndex, e.assetGen),
            reason: e.reason,
            reason_name: why.name,
            units: why.units,
            signed_reduced_q: e.signedReducedQ.toString(),
            price_e6: e.priceE6.toString(),
          },
        }),
      );
    } else {
      const sub = MOVE_SUBS[e.sub];
      const detail: V22EventRow["detail"] = { source: "log", event_version: FILL_EVENT_VERSION, ix_tag: e.ixTag, sub: e.sub, sub_name: sub };
      let amount: bigint | null = null;
      if (e.sub === 1) {
        // EARN_EXIT: a + b is the vault -> redeemer transfer
        Object.assign(detail, { principal_atoms: e.a.toString(), earnings_atoms: e.b.toString(), shares_burned: e.c.toString() });
        amount = e.a + e.b;
      } else if (e.sub === 2) {
        // G9: mode 0 draw (insurance -> vault LP capital) / 1 restore (capital only)
        Object.assign(detail, { amount_moved_atoms: e.a.toString(), receivable_after_atoms: e.b.toString(), mode: e.c.toString() });
        amount = e.a;
      } else if (e.sub === 3) {
        // RENT_ROUTED: an internal re-labelling, no token moves
        Object.assign(detail, { routed_atoms: e.a.toString() });
        amount = e.a;
      } else {
        // G9_RESTORE_PNL (mode 3): repaid from the vault LP's released profit first, capital for the remainder; a + b into insurance
        Object.assign(detail, { from_released_profit_atoms: e.a.toString(), from_capital_atoms: e.b.toString(), receivable_after_atoms: e.c.toString() });
        amount = e.a + e.b;
      }
      rows.push(
        mk(t.ixIndex, {
          kind: "move_event",
          slab_address: e.market,
          asset_index: e.assetIndex,
          actor: null,
          subject: null,
          amount: amount.toString(),
          detail,
        }),
      );
    }
  }
  stats.rows += rows.length;
  return { status: "ok", rows };
}
