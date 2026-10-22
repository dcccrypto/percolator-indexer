/**
 * v2.2 wrapper LOG events. Vectors: the worked examples of the wrapper's docs/v22-fill-events.md (release/v22-wrapper-rem
 * c6ee0b6e), and the attribution cases of its tests/v22_fill_events_attribution.rs, ported 1:1 (the forgeries must NOT be
 * attributed). Expected values are literals from the document, not re-derived from the decoder.
 */
import { readFileSync } from "node:fs";
import { beforeEach, describe, expect, it } from "vitest";
import { Keypair, PublicKey } from "@solana/web3.js";
import { LAYOUT_V22 } from "@percolatorct/sdk";
import {
  EVENT_EMITTING_TAGS, FILL_FLAG, LOG_EVENT_INNER_BASE, attributeWrapperTokens, decodeFillEventBytes, decodeFillEventToken,
  decodeV22LogEvents, fillNotionalE6, getLogEventStats, resetLogEventStats, type EventGate,
} from "../../src/parsers/v22FillEvents.js";
import type { RawInstruction } from "../../src/parsers/v22Events.js";

const key = (n: number): string => new PublicKey(new Uint8Array(32).fill(n)).toBase58();
const M = key(1), T = key(2), L = key(3);

// docs/v22-fill-events.md "Worked examples" (market 0x01 x32, taker 0x02 x32, LP 0x03 x32), extracted from the document itself
const DOC = JSON.parse(readFileSync(new URL("../fixtures/v22-fill-events-doc-examples.json", import.meta.url), "utf8")) as { examples: Record<string, { b64: string; bytes: number }> };
const EX1_CLIPPED = DOC.examples.fill_clipped_tradecpi.b64;
const EX2_ZERO = DOC.examples.fill_zero.b64;
const EX3_BATCH = DOC.examples.fill_batch_two_legs.b64;
const EX4_REDUCE = DOC.examples.reduce_rebalance.b64;
const EX5_EARN = DOC.examples.move_earn_exit.b64;

/** A MOVE event built from the document's table (kind 3, version 1, ix_tag, market, sub, asset u16, a, b, c). */
function moveBytes(o: { tag: number; sub: number; asset?: number; a?: bigint; b?: bigint; c?: bigint; market?: number }): Uint8Array {
  const b = Buffer.alloc(62);
  b[0] = 3; b[1] = 1; b[2] = o.tag;
  b.fill(o.market ?? 1, 3, 35);
  b[35] = o.sub;
  b.writeUInt16LE(o.asset ?? 0xffff, 36);
  b.writeBigUInt64LE(o.a ?? 0n, 38); b.writeBigUInt64LE(o.b ?? 0n, 46); b.writeBigUInt64LE(o.c ?? 0n, 54);
  return new Uint8Array(b);
}
const b64 = (u: Uint8Array): string => Buffer.from(u).toString("base64");

describe("token decoding: the document's worked examples", () => {
  it("1. TradeCpi clipped by LP headroom: asked 15, executed 10, fee 1,000, booked = quoted 100", () => {
    const d = decodeFillEventToken(EX1_CLIPPED);
    expect(d.ok).toBe(true);
    if (!d.ok || d.event.kind !== "fill") throw new Error("not a fill");
    expect([d.event.ixTag, d.event.market, d.event.taker, d.event.lp, d.event.recs.length]).toEqual([10, M, T, L, 1]);
    const r = d.event.recs[0];
    expect(r).toEqual({ assetIndex: 0, assetGen: 7n, flags: 0x09, requestedQ: 15_000_000n, executedQ: 10_000_000n, priceE6: 100_000_000n, quotedPriceE6: 100_000_000n, feeAtoms: 1_000n, backingFeeAtoms: 0n });
    expect(r.flags & FILL_FLAG.CLIPPED).toBeTruthy();
    expect(r.flags & FILL_FLAG.MATCHER).toBeTruthy();
  });

  it("2. zero fill (clipped to zero, no matcher call): executed 0, quoted 0", () => {
    const d = decodeFillEventToken(EX2_ZERO);
    if (!d.ok || d.event.kind !== "fill") throw new Error("not a fill");
    const r = d.event.recs[0];
    expect([r.flags, r.requestedQ, r.executedQ, r.quotedPriceE6, r.feeAtoms]).toEqual([0x0d, 3_000_000n, 0n, 0n, 0n]);
    expect(r.priceE6).toBe(100_000_000n); // the reference price: nothing was booked
  });

  it("3. BatchTradeCpi, two legs (ix_tag 67): leg 1 PARTIAL booked 250 quoted 249", () => {
    const d = decodeFillEventToken(EX3_BATCH);
    if (!d.ok || d.event.kind !== "fill") throw new Error("not a fill");
    expect([d.event.ixTag, d.event.recs.length]).toEqual([67, 2]);
    const [a, b] = d.event.recs;
    expect([a.assetIndex, a.flags, a.executedQ, a.priceE6]).toEqual([0, 0x08, 1_000_000n, 100_000_000n]); // leg 0: full fill through the matcher
    // leg 1: filled 0.5 of 2 by the matcher (PARTIAL|MATCHER), booked 250, quoted 249
    expect([b.assetIndex, b.flags, b.requestedQ, b.executedQ, b.priceE6, b.quotedPriceE6]).toEqual([1, 0x0a, 2_000_000n, 500_000n, 250_000_000n, 249_000_000n]);
  });

  it("4. RebalanceReduce (tag 44): a long reduced by 4 -> signed_reduced_q -4,000,000, no counterparty, reason 1", () => {
    const d = decodeFillEventToken(EX4_REDUCE);
    if (!d.ok || d.event.kind !== "reduce") throw new Error("not a reduce");
    expect(d.event).toEqual({ kind: "reduce", ixTag: 44, market: M, portfolio: T, counterparty: null, assetIndex: 0, assetGen: 7n, reason: 1, signedReducedQ: -4_000_000n, priceE6: 100_000_000n });
  });

  it("5. Earn exit (tag 77, MOVE sub 1): principal 9,000,000, earnings 1,250,000, shares burned 10,000,000, no asset", () => {
    const d = decodeFillEventToken(EX5_EARN);
    if (!d.ok || d.event.kind !== "move") throw new Error("not a move");
    expect(d.event).toEqual({ kind: "move", ixTag: 77, market: M, sub: 1, assetIndex: null, a: 9_000_000n, b: 1_250_000n, c: 10_000_000n });
  });

  it("MOVE sub 4 G9_RESTORE_PNL (tag 111): a = from released profit, b = from capital, c = receivable after", () => {
    const d = decodeFillEventBytes(moveBytes({ tag: 111, sub: 4, a: 700n, b: 0n, c: 12n }));
    if (!d.ok || d.event.kind !== "move") throw new Error("not a move");
    expect(d.event).toMatchObject({ sub: 4, ixTag: 111, a: 700n, b: 0n, c: 12n });
  });
});

describe("the decoder skips what it does not know and never throws", () => {
  const withBytes = (b: number[], pad = 0): Uint8Array => { const u = new Uint8Array(b.length + pad); u.set(b); return u; };
  it("unknown kind / version / sub / reason, wrong lengths, not base64: skipped with a reason", () => {
    expect(decodeFillEventBytes(withBytes([9, 1, 10], 200))).toEqual({ ok: false, skip: "unknown_kind" });
    expect(decodeFillEventBytes(withBytes([0, 1, 10, 0]))).toEqual({ ok: false, skip: "unknown_kind" });
    expect(decodeFillEventBytes(withBytes([1, 2, 10], 172))).toEqual({ ok: false, skip: "unknown_version" });
    expect(decodeFillEventBytes(withBytes([1, 1, 10, 0, 0]))).toEqual({ ok: false, skip: "bad_length" });
    const badN = withBytes([1, 1, 10], 97); badN[99] = 3;
    expect(decodeFillEventBytes(badN)).toEqual({ ok: false, skip: "bad_length" });
    expect(decodeFillEventBytes(withBytes([2, 1, 44], 100))).toEqual({ ok: false, skip: "bad_length" });
    expect(decodeFillEventBytes(withBytes([3, 1, 77], 80))).toEqual({ ok: false, skip: "bad_length" });
    expect(decodeFillEventBytes(new Uint8Array([1]))).toEqual({ ok: false, skip: "malformed" });
    expect(decodeFillEventToken("!!!not-base64!!!")).toEqual({ ok: false, skip: "malformed" });
    expect(decodeFillEventToken("")).toEqual({ ok: false, skip: "malformed" });
    expect(decodeFillEventBytes(moveBytes({ tag: 77, sub: 9 }))).toEqual({ ok: false, skip: "unknown_sub" });
    const reduce = Buffer.from(Buffer.from(EX4_REDUCE, "base64")); reduce[109] = 9;
    expect(decodeFillEventBytes(new Uint8Array(reduce))).toEqual({ ok: false, skip: "unknown_reason" });
  });

  it("every prefix of a real event is rejected without throwing; the whole event decodes", () => {
    const real = new Uint8Array(Buffer.from(EX1_CLIPPED, "base64"));
    expect(decodeFillEventBytes(real).ok).toBe(true);
    for (let n = 0; n < real.length; n++) expect(decodeFillEventBytes(real.subarray(0, n)).ok).toBe(false);
  });

  it("an unknown FLAG bit or flag combination is not rejected", () => {
    const real = Buffer.from(Buffer.from(EX1_CLIPPED, "base64"));
    real[100 + 10] = 0xf9;
    const d = decodeFillEventBytes(new Uint8Array(real));
    expect(d.ok && d.event.kind === "fill" && d.event.recs[0].flags).toBe(0xf9);
  });

  it("i128 sign: a short taker has a negative executed size", () => {
    const real = Buffer.from(Buffer.from(EX1_CLIPPED, "base64"));
    // executed_q at 100 + 27 = -10,000,000 (two's complement)
    real.writeBigInt64LE(-10_000_000n, 127); real.writeBigInt64LE(-1n, 135);
    const d = decodeFillEventBytes(new Uint8Array(real));
    if (!d.ok || d.event.kind !== "fill") throw new Error("not a fill");
    expect(d.event.recs[0].executedQ).toBe(-10_000_000n);
  });
});

// ── Attribution (ported from tests/v22_fill_events_attribution.rs) ─────────────────────────────────────────────────────
const W = Keypair.generate().publicKey.toBase58();
const MATCHER = Keypair.generate().publicKey.toBase58();
const OUTER = Keypair.generate().publicKey.toBase58();
const OTHER = Keypair.generate().publicKey.toBase58();
const wrappers = new Set([W]);
/** A forged FILL (executed 10 units, fee 1,000) vs the wrapper's real event: a ZERO fill. */
const FORGED = EX1_CLIPPED;
const REAL = EX2_ZERO;

function txLogs(matcherLines: string[]): string[] {
  return [
    `Program ${W} invoke [1]`,
    `Program ${MATCHER} invoke [2]`,
    ...matcherLines,
    `Program ${MATCHER} consumed 5000 of 900000 compute units`,
    `Program ${MATCHER} success`,
    `Program data: ${REAL}`,
    `Program ${W} consumed 90000 of 1400000 compute units`,
    `Program ${W} success`,
  ];
}
const tokens = (logs: unknown): string[] => {
  const a = attributeWrapperTokens(logs, wrappers);
  if (!a.ok) throw new Error(`unknown: ${a.reason}`);
  return a.tokens.map((t) => t.b64);
};
const onlyReal = (logs: unknown): void => expect(tokens(logs)).toEqual([REAL]);
const unknown = (logs: unknown): string => { const a = attributeWrapperTokens(logs, wrappers); return a.ok ? "ok" : a.reason; };

describe("attribution: the matcher cannot forge the wrapper's events", () => {
  const forgedLine = `Program data: ${FORGED}`;

  it("control: the honest case attributes exactly the wrapper's line", () => onlyReal(txLogs([])));

  it("probe 1: msg!('success') then a forged data line: `Program log: success` pops nothing", () => onlyReal(txLogs(["Program log: success", forgedLine])));

  it("probe 2: plus `Program log: invoke [2]` does not rebalance the stack", () => onlyReal(txLogs(["Program log: success", forgedLine, "Program log: invoke [2]"])));

  it("probe 3: an honest matcher's own data line is in the matcher's frame, not the wrapper's", () => onlyReal(txLogs([forgedLine])));

  it("msg! text naming real program ids never moves a frame", () => {
    for (const spoof of [`Program log: Program ${MATCHER} success`, `Program log: ${MATCHER} success`, "Program log: failed: custom program error: 0x1", `Program log: Program ${W} invoke [1]`, "Program log: Log truncated", "Program log: success"]) {
      onlyReal(txLogs([spoof, forgedLine]));
    }
  });

  it("each element is ONE atomic line: embedded newlines stay inside it; the join + re-split spelling IS forgeable (shown, so the rule's reason is on the page)", () => {
    const multi = `Program log: x\nProgram ${MATCHER} success\nProgram data: ${FORGED}\nProgram ${MATCHER} invoke [2]`;
    onlyReal(txLogs([multi]));
    const resplit = txLogs([multi]).join("\n").split("\n");
    expect(tokens(resplit)).toContain(FORGED);
  });

  it("`Log truncated` anywhere makes the events unknown, at every position, and when it replaced the event line", () => {
    const base = txLogs([]);
    for (let at = 0; at <= base.length; at++) {
      const logs = [...base]; logs.splice(at, 0, "Log truncated");
      expect(unknown(logs)).toBe("truncated");
    }
    const logs = [...base]; logs[logs.findIndex((l) => l.startsWith("Program data: "))] = "Log truncated";
    expect(unknown(logs)).toBe("truncated");
  });

  it("null, undefined, empty and non-array logs are unknown; an array with a non-string element is unknown", () => {
    for (const l of [null, undefined, []]) expect(unknown(l)).toBe("no_logs");
    expect(unknown("Program x")).toBe("no_logs");
    expect(unknown(["Program x", 5])).toBe("logs_malformed");
  });

  it("any frame inconsistency makes the WHOLE transaction unknown, even after a well-formed wrapper event", () => {
    const data = `Program data: ${REAL}`;
    const cases: Array<[string[], string]> = [
      [[`Program ${W} invoke [1]`, `Program ${MATCHER} invoke [3]`], "bad_invoke_depth"],
      [[`Program ${W} invoke [1]`, data, `Program ${MATCHER} invoke [1]`], "bad_invoke_depth"],
      [[`Program ${W} invoke [2]`], "bad_invoke_depth"],
      [[`Program ${W} invoke [1]`, `Program ${MATCHER} invoke [2]`, `Program ${W} success`], "bad_return"],
      [[`Program ${W} invoke [1]`, data, `Program ${OTHER} success`], "bad_return"],
      [[`Program ${W} invoke [1]`, `Program ${MATCHER} invoke [2]`, `Program ${W} failed: custom program error: 0x1`], "bad_return"],
      [[`Program ${W} success`], "bad_return"],
      [[data], "data_outside_frame"],
      [[`Program ${W} invoke [1]`, data], "unclosed_frame"],
    ];
    for (const [logs, why] of cases) expect(unknown(logs)).toBe(why);
  });

  it("near-miss runtime lines are not frames: none pops the matcher, none pushes", () => {
    const forgedLine = `Program data: ${FORGED}`;
    for (const notAPop of [`Program ${MATCHER} success `, `Program ${MATCHER} successful`, `Program ${MATCHER} failed`, `Program ${MATCHER} failedx: y`, ` Program ${MATCHER} success`, `program ${MATCHER} success`, `Program  ${MATCHER} success`]) {
      onlyReal(txLogs([notAPop, forgedLine]));
    }
    for (const notAPush of [`Program ${W} invoke [2] `, `Program ${W} invoke [x]`, `Program ${W} invoke [+3]`, `Program ${W} invoke []`, `Program ${W} invoke [3`, "Program notapubkey invoke [3]"]) {
      onlyReal(txLogs([notAPush, forgedLine]));
    }
    // a real `failed: ` line pops the top of the stack
    const logs = [`Program ${W} invoke [1]`, `Program ${MATCHER} invoke [2]`, `Program ${MATCHER} failed: custom program error: 0x7`, `Program data: ${REAL}`, `Program ${W} failed: custom program error: 0x7`];
    expect(tokens(logs)).toEqual([REAL]);
  });

  it("an id that is not a valid base58 32-byte key (a 0 / O / I / l character, or a wrong length) is not a frame", () => {
    for (const bad of [`${MATCHER.slice(0, -1)}0`, `1${MATCHER}`]) {
      onlyReal(txLogs([`Program ${bad} success`, `Program data: ${FORGED}`]));
    }
  });

  it("the wrapper entered by CPI keeps only its own lines; the outer program's copies of the forgery are ignored", () => {
    const logs = [`Program ${OUTER} invoke [1]`, `Program data: ${FORGED}`, `Program ${W} invoke [2]`, `Program data: ${REAL}`, `Program ${W} success`, `Program data: ${FORGED}`, `Program ${OUTER} success`];
    onlyReal(logs);
  });

  it("several tokens on one line are split on spaces; the top-level instruction index follows the depth-1 frames", () => {
    const logs = [
      `Program ${MATCHER} invoke [1]`, `Program ${MATCHER} success`,
      `Program ${W} invoke [1]`, `Program data: ${REAL} ${EX4_REDUCE}`, `Program ${W} success`,
      `Program ${W} invoke [1]`, `Program data: ${EX5_EARN}`, `Program ${W} success`,
    ];
    const a = attributeWrapperTokens(logs, wrappers);
    expect(a.ok && a.tokens.map((t) => [t.b64 === REAL ? "z" : t.b64 === EX4_REDUCE ? "r" : "m", t.ixIndex])).toEqual([["z", 1], ["r", 1], ["m", 2]]);
  });

  it("a pinned wrapper set may hold several ids; a program outside it is never attributed", () => {
    const logs = [`Program ${OTHER} invoke [1]`, `Program data: ${REAL}`, `Program ${OTHER} success`];
    expect(attributeWrapperTokens(logs, wrappers)).toEqual({ ok: true, tokens: [] });
    expect(attributeWrapperTokens(logs, new Set([W, OTHER]))).toMatchObject({ ok: true });
  });
});

// ── Rows ──────────────────────────────────────────────────────────────────────────────────────────────────────────────
const ix = (tag: number, accounts: string[] = [M]): RawInstruction => ({ programId: W, accounts, data: new Uint8Array([tag]), ixIndex: 0, innerIndex: -1 });
const gate = (o: { known?: string[]; version?: Record<string, number>; gen?: Record<string, bigint> } = {}): EventGate => ({
  isKnownMarket: (s) => (o.known ?? [M]).includes(s),
  versionOf: (s) => o.version?.[s] ?? null,
  generationOf: (s, a) => o.gen?.[`${s}:${a}`] ?? null,
});
const okLogs = (...toks: string[]): string[] => [`Program ${W} invoke [1]`, ...toks.map((t) => `Program data: ${t}`), `Program ${W} success`];
const ctx = { signature: "SIG", slot: 77, blockTimeSec: 1_790_000_000 };
const run = (logs: unknown, ixs: RawInstruction[], g: EventGate = gate(), err: unknown = null) => decodeV22LogEvents({ err, logMessages: logs }, ixs, wrappers, g, ctx);

beforeEach(() => resetLogEventStats());

describe("rows from the worked examples", () => {
  it("FILL: one row per record; executed size only; untrusted fields named so; unique inner_index", () => {
    const r = run(okLogs(EX1_CLIPPED, EX3_BATCH), [ix(10), ix(67)]);
    expect(r.status).toBe("ok");
    expect(r.rows.map((x) => x.kind)).toEqual(["fill_event", "fill_event", "fill_event"]);
    const [clip, leg0, leg1] = r.rows;
    expect([clip.slab_address, clip.actor, clip.subject, clip.asset_index, clip.amount]).toEqual([M, T, L, 0, "10000000"]);
    expect(clip.detail).toMatchObject({ source: "log", ix_tag: 10, asset_gen: "7", clipped: true, partial: false, zero: false, matcher: true, executed_q: "10000000", price_e6: "100000000", notional_e6: "1000000000", fee_atoms_total: "1000", requested_q_untrusted: "15000000", quoted_price_e6_untrusted: "100000000", rec_index: 0, recs_in_line: 1 });
    expect(leg0.detail).toMatchObject({ ix_tag: 67, rec_index: 0, recs_in_line: 2, partial: false });
    expect(leg1.detail).toMatchObject({ rec_index: 1, recs_in_line: 2, partial: true, executed_q: "500000", price_e6: "250000000", quoted_price_e6_untrusted: "249000000" });
    expect(r.rows.map((x) => x.inner_index)).toEqual([LOG_EVENT_INNER_BASE, LOG_EVENT_INNER_BASE + 1, LOG_EVENT_INNER_BASE + 2]);
    expect(new Set(r.rows.map((x) => `${x.signature}|${x.ix_index}|${x.inner_index}`)).size).toBe(3);
    expect(r.rows.every((x) => x.slot === 77 && x.block_time === "2026-09-21T14:13:20.000Z")).toBe(true);
  });

  it("REDUCE and MOVE rows (incl. sub 4)", () => {
    const sub4 = b64(moveBytes({ tag: 111, sub: 4, a: 700n, b: 300n, c: 12n }));
    const sub2 = b64(moveBytes({ tag: 111, sub: 2, a: 5_000n, b: 9n, c: 1n }));
    const sub3 = b64(moveBytes({ tag: 106, sub: 3, asset: 2, a: 42n }));
    const r = run(okLogs(EX4_REDUCE, EX5_EARN, sub4, sub2, sub3), [ix(44), ix(77), ix(111), ix(106)]);
    expect(r.rows.map((x) => x.kind)).toEqual(["reduce_event", "move_event", "move_event", "move_event", "move_event"]);
    const [red, earn, restore, g9, rent] = r.rows;
    expect([red.actor, red.subject, red.amount]).toEqual([T, null, "4000000"]);
    expect(red.detail).toMatchObject({ reason: 1, reason_name: "rebalance_reduce", units: "basis", signed_reduced_q: "-4000000", price_e6: "100000000" });
    expect([earn.amount, earn.asset_index]).toEqual(["10250000", null]);
    expect(earn.detail).toMatchObject({ sub: 1, sub_name: "earn_exit", principal_atoms: "9000000", earnings_atoms: "1250000", shares_burned: "10000000" });
    expect(restore.amount).toBe("1000");
    expect(restore.detail).toMatchObject({ sub: 4, sub_name: "g9_restore_pnl", from_released_profit_atoms: "700", from_capital_atoms: "300", receivable_after_atoms: "12" });
    expect(g9.detail).toMatchObject({ sub: 2, sub_name: "g9", amount_moved_atoms: "5000", receivable_after_atoms: "9", mode: "1" });
    expect([rent.asset_index, rent.amount]).toEqual([2, "42"]);
  });

  it("ix_index is the top-level instruction the event came from", () => {
    const logs = [`Program ${MATCHER} invoke [1]`, `Program ${MATCHER} success`, `Program ${W} invoke [1]`, `Program data: ${EX1_CLIPPED}`, `Program ${W} success`];
    expect(run(logs, [ix(10)]).rows[0].ix_index).toBe(1);
  });

  it("an event row is never produced for a skipped token; the known ones around it still decode (counted)", () => {
    const kind9 = b64(new Uint8Array([9, 1, 10, ...new Array(200).fill(7)]));
    const r = run(okLogs(kind9, EX1_CLIPPED, "!!!", b64(new Uint8Array([1]))), [ix(10)]);
    expect(r.rows).toHaveLength(1);
    expect(getLogEventStats().skippedByReason).toEqual({ unknown_kind: 1, malformed: 2 });
  });
});

describe("VOLUME is the executed size at the booked price, nothing else", () => {
  it("clipped: asked 15, executed 10 -> notional of 10; zero fill -> 0; quoted price never used", () => {
    expect(fillNotionalE6(10_000_000n, 100_000_000n)).toBe(1_000_000_000n); // 10 units x 100
    expect(fillNotionalE6(-10_000_000n, 100_000_000n)).toBe(1_000_000_000n);
    expect(fillNotionalE6(0n, 100_000_000n)).toBe(0n);
    const clipped = run(okLogs(EX1_CLIPPED), [ix(10)]).rows[0];
    expect(clipped.detail.notional_e6).toBe("1000000000");
    const zero = run(okLogs(EX2_ZERO), [ix(10)]).rows[0];
    expect([zero.amount, zero.detail.notional_e6, zero.detail.zero]).toEqual(["0", "0", true]);
    // NEGATIVE CONTROL: a volume taken from the requested size (3 units on the zero fill) or the quote would not be 0
    expect(fillNotionalE6(3_000_000n, 100_000_000n)).not.toBe(0n);
    const batch = run(okLogs(EX3_BATCH), [ix(67)]).rows[1]; // leg 1: PARTIAL
    expect(batch.detail.notional_e6).toBe(fillNotionalE6(500_000n, 250_000_000n).toString()); // booked 250, not the quoted 249 nor the requested 2
    expect(batch.detail.notional_e6).toBe("125000000");
  });
});

describe("trust rules around the rows", () => {
  it("a failed transaction yields nothing, whatever its logs say", () => {
    expect(run(okLogs(EX1_CLIPPED), [ix(10)], gate(), { InstructionError: [1, { Custom: 1 }] })).toEqual({ status: "failed", rows: [] });
    expect(getLogEventStats().txsRead).toBe(0);
  });

  it("a transaction without an event-emitting wrapper instruction is not read at all", () => {
    expect(run(okLogs(EX1_CLIPPED), [ix(108)])).toEqual({ status: "not_applicable", rows: [] });
    expect(run(null, [])).toEqual({ status: "not_applicable", rows: [] });
    // the wrapper's fill_events_v22.rs TAG_* constants (5 crank, 6, 10, 44, 66, 67, 77, 104, 106, 111, 118, 119), as literals
    expect([...EVENT_EMITTING_TAGS].sort((a, b) => a - b)).toEqual([5, 6, 10, 44, 66, 67, 77, 104, 106, 111, 118, 119]);
  });

  it("an event for a market outside the registry is dropped (counted); one for a market of another VERSION too", () => {
    expect(run(okLogs(EX1_CLIPPED), [ix(10)], gate({ known: [] })).rows).toHaveLength(0);
    expect(getLogEventStats().droppedUnknownMarket).toBe(1);
    expect(run(okLogs(EX1_CLIPPED), [ix(10)], gate({ version: { [M]: 18 } })).rows).toHaveLength(0);
    expect(getLogEventStats().droppedWrongVersion).toBe(1);
    // VERSION 19 or not-yet-read: kept
    expect(run(okLogs(EX1_CLIPPED), [ix(10)], gate({ version: { [M]: LAYOUT_V22.version } })).rows).toHaveLength(1);
    expect(run(okLogs(EX1_CLIPPED), [ix(10)], gate()).rows).toHaveLength(1);
  });

  it("the asset generation is checked and FLAGGED: a mismatch is kept (the cache can lag a fresh asset) and counted", () => {
    const same = run(okLogs(EX1_CLIPPED), [ix(10)], gate({ gen: { [`${M}:0`]: 7n } })).rows[0];
    expect(same.detail.gen_current).toBe(true);
    const diff = run(okLogs(EX1_CLIPPED), [ix(10)], gate({ gen: { [`${M}:0`]: 8n } })).rows[0];
    expect(diff.detail.gen_current).toBe(false);
    expect(getLogEventStats().generationMismatch).toBe(1);
    expect(run(okLogs(EX1_CLIPPED), [ix(10)], gate()).rows[0].detail.gen_current).toBeNull();
  });
});

describe("unknown events: a marker, never 'no fill'; VERSION-keyed", () => {
  const v22 = gate({ version: { [M]: LAYOUT_V22.version } });
  it("truncated / null / inconsistent logs record one events_unknown marker per known v2.2 market named by the wrapper instruction", () => {
    for (const [logs, why] of [[[...okLogs(EX1_CLIPPED), "Log truncated"], "truncated"], [null, "no_logs"], [[`Program ${W} invoke [1]`], "unclosed_frame"]] as const) {
      const r = run(logs, [ix(10, [T, M])], v22);
      expect(r.status).toBe("unknown");
      expect(r.rows).toHaveLength(1);
      expect(r.rows[0]).toMatchObject({ kind: "events_unknown", slab_address: M, ix_index: -1, amount: null });
      expect(r.rows[0].detail).toMatchObject({ source: "log", reason: why, reconcile: "from_account_state", wrapper_instructions: 1 });
      expect(r.rows[0].inner_index).toBeGreaterThanOrEqual(LOG_EVENT_INNER_BASE);
    }
    expect(getLogEventStats().unknownByReason).toEqual({ truncated: 1, no_logs: 1, unclosed_frame: 1 });
  });

  it("no marker for a market known to be v2.1, one not yet read, or one outside the registry; none for a bare crank", () => {
    expect(run(null, [ix(10)], gate({ version: { [M]: 18 } })).rows).toEqual([]);
    expect(run(null, [ix(10)], gate()).rows).toEqual([]);
    expect(run(null, [ix(10)], gate({ known: [], version: { [M]: 19 } })).rows).toEqual([]);
    expect(run(null, [ix(5)], v22).rows).toEqual([]);
    expect(run(null, [ix(5)], v22).status).toBe("unknown");
  });

  it("two markets in one transaction: one marker each, sorted, unique keys", () => {
    const M2 = key(9);
    const r = run(null, [ix(67, [M, M2]), ix(10, [M2])], gate({ known: [M, M2], version: { [M]: 19, [M2]: 19 } }));
    expect(r.rows.map((x) => x.slab_address)).toEqual([M, M2].sort());
    expect(new Set(r.rows.map((x) => x.inner_index)).size).toBe(2);
  });
});
