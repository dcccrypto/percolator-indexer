/**
 * Mutation killers for the frame tracker and REDUCE decode in src/parsers/v22FillEvents.ts. Each test fails against at
 * least one mutant (F06, F16, F21, F25, R09, R10, R12) that survived v22FillEvents.edges.test.ts, v22FillEvents.test.ts and
 * `vitest related` at 9db52dd (IDX-REPORT-V22FILLEDGESTESTMUT). The base code is correct on every case here.
 */
import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { attributeWrapperTokens, decodeFillEventToken, decodeV22LogEvents, type EventGate } from "../../src/parsers/v22FillEvents.js";
import type { RawInstruction } from "../../src/parsers/v22Events.js";

const key = (n: number): string => new PublicKey(new Uint8Array(32).fill(n)).toBase58();
const M = key(1);
const DOC = JSON.parse(readFileSync(new URL("../fixtures/v22-fill-events-doc-examples.json", import.meta.url), "utf8")) as { examples: Record<string, { b64: string }> };
const FILL = DOC.examples.fill_clipped_tradecpi.b64;
const REDUCE = DOC.examples.reduce_rebalance.b64;
const W = key(2);
const MATCHER = key(3);
/** The System program: an all-zero key whose canonical base58 text is exactly 32 characters. */
const SYSTEM = "11111111111111111111111111111111";
const wrappers = new Set([W]);
const gate: EventGate = { isKnownMarket: (s) => s === M, versionOf: () => null, generationOf: () => null };
const ix = (tag: number, ixIndex: number): RawInstruction => ({ programId: W, accounts: [M], data: new Uint8Array([tag]), ixIndex, innerIndex: -1 });

const reduceWith = (edit: (raw: Buffer) => void) => {
  const raw = Buffer.from(REDUCE, "base64");
  edit(raw);
  const d = decodeFillEventToken(raw.toString("base64"));
  if (!d.ok || d.event.kind !== "reduce") throw new Error(`not a reduce: ${JSON.stringify(d)}`);
  return d.event;
};

describe("fill attribution: instruction index and frame visibility", () => {
  // F06 (`depth >= 1` bumps topLevel): every TradeCpi CPIs its matcher BEFORE emitting FILL, so the fill was booked to
  // the next top-level instruction.
  it("an inner CPI before the FILL line does not move the fill to another top-level instruction", () => {
    const logs = [
      `Program ${W} invoke [1]`,
      `Program ${MATCHER} invoke [2]`,
      `Program ${MATCHER} success`,
      `Program data: ${FILL}`,
      `Program ${W} success`,
    ];
    expect(attributeWrapperTokens(logs, wrappers)).toEqual({ ok: true, tokens: [{ b64: FILL, ixIndex: 0 }] });
    const r = decodeV22LogEvents({ err: null, logMessages: logs }, [ix(10, 0)], wrappers, gate, { signature: "S" });
    expect(r.status).toBe("ok");
    expect(r.rows.length).toBeGreaterThan(0);
    expect(r.rows.every((x) => x.ix_index === 0)).toBe(true);
  });

  // F21 (id length < 33): a 32-character id stops being a program line, so a top-level System instruction is not
  // counted (wrapper fill booked to ix 0 instead of 1) and data printed inside a System frame would read as the wrapper's.
  it("a 32-character program id (System) opens a frame: it counts as a top-level instruction", () => {
    const logs = [`Program ${SYSTEM} invoke [1]`, `Program ${SYSTEM} success`, `Program ${W} invoke [1]`, `Program data: ${FILL}`, `Program ${W} success`];
    expect(attributeWrapperTokens(logs, wrappers)).toEqual({ ok: true, tokens: [{ b64: FILL, ixIndex: 1 }] });
  });

  // Synthetic: the real System program never logs data; this only pins that data belongs to the frame on top of the stack.
  it("a data line inside a 32-character-id frame nested in the wrapper is not the wrapper's", () => {
    const logs = [`Program ${W} invoke [1]`, `Program ${SYSTEM} invoke [2]`, `Program data: ${FILL}`, `Program ${SYSTEM} success`, `Program ${W} success`];
    expect(attributeWrapperTokens(logs, wrappers)).toEqual({ ok: true, tokens: [] });
  });

  // F25 (startsWith -> includes): text that merely CONTAINS the prefix, e.g. a wrapper `msg!` echoing input, forged a FILL.
  it("a log line that only contains 'Program data: ' is not a data line", () => {
    const logs = [`Program ${W} invoke [1]`, `Program log: Program data: ${FILL}`, `Program ${W} success`];
    expect(attributeWrapperTokens(logs, wrappers)).toEqual({ ok: true, tokens: [] });
  });

  // F16 (split(" ") -> split(/\s+/)): the runtime separates tokens with single spaces only; a tab is not a separator,
  // so a tab-joined pair is one malformed token, never two events.
  it("only a space separates tokens: a tab-joined pair is one token", () => {
    const a = attributeWrapperTokens([`Program ${W} invoke [1]`, `Program data: ${FILL}\t${FILL}`, `Program ${W} success`], wrappers);
    expect(a).toEqual({ ok: true, tokens: [{ b64: `${FILL}\t${FILL}`, ixIndex: 0 }] });
  });
});

describe("REDUCE counterparty and length", () => {
  // R09 (zero test at 68): byte 99 is the asset index's low byte, so a counterparty-less REDUCE on asset 1 read the
  // all-zero key as a real counterparty (the System program as the row's subject).
  it("an all-zero counterparty is null whatever the asset index", () => {
    const e = reduceWith((raw) => raw.writeUInt16LE(1, 99));
    expect(e.assetIndex).toBe(1);
    expect(e.counterparty).toBeNull();
  });

  // R09 / R10: a counterparty that differs from zero only in its first or last byte is still a counterparty.
  it.each([67, 98])("a counterparty non-zero only at byte %i is kept", (at) => {
    const e = reduceWith((raw) => { raw[at] = 7; });
    const want = new Uint8Array(32);
    want[at - 67] = 7;
    expect(e.counterparty).toBe(new PublicKey(want).toBase58());
  });

  // R12 (length != -> <): a version-1 REDUCE is exactly 134 bytes; a longer one is skipped, not read.
  it("a REDUCE with trailing bytes is bad_length", () => {
    const raw = Buffer.concat([Buffer.from(REDUCE, "base64"), Buffer.alloc(1)]);
    expect(decodeFillEventToken(raw.toString("base64"))).toEqual({ ok: false, skip: "bad_length" });
  });
});
