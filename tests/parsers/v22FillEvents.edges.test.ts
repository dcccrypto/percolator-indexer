/**
 * Edge cases for parsers/v22FillEvents.ts that the main suite does not reach: log-line framing oddities, the "unlisted tag" probe,
 * context fields that are absent, a REDUCE with a counterparty, and the generation gate on asset-less events.
 */
import { readFileSync } from "node:fs";
import { beforeEach, describe, expect, it } from "vitest";
import { Keypair, PublicKey } from "@solana/web3.js";
import {
  attributeWrapperTokens, decodeFillEventToken, decodeV22LogEvents, getLogEventStats, resetLogEventStats, type EventGate,
} from "../../src/parsers/v22FillEvents.js";
import type { RawInstruction } from "../../src/parsers/v22Events.js";

const key = (n: number): string => new PublicKey(new Uint8Array(32).fill(n)).toBase58();
const M = key(1);
const DOC = JSON.parse(readFileSync(new URL("../fixtures/v22-fill-events-doc-examples.json", import.meta.url), "utf8")) as { examples: Record<string, { b64: string }> };
const FILL = DOC.examples.fill_clipped_tradecpi.b64;
const REDUCE = DOC.examples.reduce_rebalance.b64;
const W = Keypair.generate().publicKey.toBase58();
const OTHER = Keypair.generate().publicKey.toBase58();
const wrappers = new Set([W]);

const ix = (tag: number, accounts: string[] = [M]): RawInstruction => ({ programId: W, accounts, data: new Uint8Array([tag]), ixIndex: 0, innerIndex: -1 });
const gate = (o: { gen?: Record<string, bigint> } = {}): EventGate => ({
  isKnownMarket: (s) => s === M,
  versionOf: () => null,
  generationOf: (s, a) => o.gen?.[`${s}:${a}`] ?? null,
});
const okLogs = (...lines: string[]): string[] => [`Program ${W} invoke [1]`, ...lines, `Program ${W} success`];

beforeEach(() => resetLogEventStats());

describe("attribution framing oddities", () => {
  it("an unsafe-integer invoke depth is not a frame push; the line is ignored like any other runtime line", () => {
    const logs = okLogs(`Program ${OTHER} invoke [99999999999999999999]`, `Program data: ${FILL}`);
    expect(attributeWrapperTokens(logs, wrappers)).toMatchObject({ ok: true, tokens: [{ b64: FILL, ixIndex: 0 }] });
  });

  it("runs of spaces in a data line yield no empty tokens", () => {
    const a = attributeWrapperTokens(okLogs(`Program data: ${FILL}  ${FILL} `), wrappers);
    expect(a).toMatchObject({ ok: true });
    if (a.ok) expect(a.tokens.map((t) => t.b64)).toEqual([FILL, FILL]);
  });

  it("an id that is not a 32-byte key is not a program line (a data line then has no frame)", () => {
    expect(attributeWrapperTokens(["Program notakey invoke [1]", `Program data: ${FILL}`], wrappers)).toEqual({ ok: false, reason: "data_outside_frame" });
  });
});

describe("REDUCE with a counterparty", () => {
  it("a non-zero counterparty key is kept; the all-zero key reads as null", () => {
    const raw = Buffer.from(REDUCE, "base64");
    const base = decodeFillEventToken(REDUCE);
    if (!base.ok || base.event.kind !== "reduce") throw new Error("not a reduce");
    expect(base.event.counterparty).toBeNull();
    raw.fill(9, 67, 99);
    const d = decodeFillEventToken(raw.toString("base64"));
    if (!d.ok || d.event.kind !== "reduce") throw new Error("not a reduce");
    expect(d.event.counterparty).toBe(key(9));
  });
});

describe("decodeV22LogEvents", () => {
  it("an unlisted emitting tag is detected only when the logs hold wrapper tokens", () => {
    // tag 200 is not an emitting tag: no data lines at all -> fast path
    expect(decodeV22LogEvents({ err: null, logMessages: okLogs() }, [ix(200)], wrappers, gate(), { signature: "S" }).status).toBe("not_applicable");
    // data lines exist but only in a foreign frame -> still not applicable, nothing counted
    const foreign = [`Program ${OTHER} invoke [1]`, `Program data: ${FILL}`, `Program ${OTHER} success`];
    expect(decodeV22LogEvents({ err: null, logMessages: foreign }, [ix(200)], wrappers, gate(), { signature: "S" })).toEqual({ status: "not_applicable", rows: [] });
    // unparsable logs (unclosed frame) with a data line -> probe not ok -> not applicable
    const broken = [`Program ${W} invoke [1]`, `Program data: ${FILL}`];
    expect(decodeV22LogEvents({ err: null, logMessages: broken }, [ix(200)], wrappers, gate(), { signature: "S" }).status).toBe("not_applicable");
    expect(getLogEventStats().txsRead).toBe(0);
  });

  it("a missing slot and block time become null on every row", () => {
    const r = decodeV22LogEvents({ err: null, logMessages: okLogs(`Program data: ${FILL}`) }, [ix(10)], wrappers, gate(), { signature: "S" });
    expect(r.status).toBe("ok");
    expect(r.rows.length).toBeGreaterThan(0);
    expect(r.rows.every((x) => x.slot === null && x.block_time === null)).toBe(true);
    const r2 = decodeV22LogEvents({ err: null, logMessages: okLogs(`Program data: ${FILL}`) }, [ix(10)], wrappers, gate(), { signature: "S", slot: 5, blockTimeSec: null });
    expect(r2.rows.every((x) => x.slot === 5 && x.block_time === null)).toBe(true);
  });

  it("gen_current: null without a known generation, true on match, false (and counted) on mismatch", () => {
    const run = (g: EventGate) => decodeV22LogEvents({ err: null, logMessages: okLogs(`Program data: ${FILL}`, `Program data: ${REDUCE}`) }, [ix(10), ix(44)], wrappers, g, { signature: "S" });
    const none = run(gate());
    expect(none.rows.map((x) => (x.detail as { gen_current: unknown }).gen_current)).toEqual([null, null]);
    const fillGen = decodeFillEventToken(FILL);
    if (!fillGen.ok || fillGen.event.kind !== "fill") throw new Error("not a fill");
    const match = run(gate({ gen: { [`${M}:0`]: fillGen.event.recs[0].assetGen } }));
    expect((match.rows[0].detail as { gen_current: unknown }).gen_current).toBe(true);
    resetLogEventStats();
    const miss = run(gate({ gen: { [`${M}:0`]: 999n } }));
    expect((miss.rows[0].detail as { gen_current: unknown }).gen_current).toBe(false);
    expect(getLogEventStats().generationMismatch).toBeGreaterThan(0);
  });
});
