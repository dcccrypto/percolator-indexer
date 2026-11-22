/** Shared builders: realistic v2.2 tx logs + wrapper instructions around the worked-example events (bench + differential test). */
import { readFileSync } from "node:fs";
import { Keypair, PublicKey } from "@solana/web3.js";
import { EVENT_EMITTING_TAGS, type EventGate } from "../../src/parsers/v22FillEvents.js";
import type { RawInstruction } from "../../src/parsers/v22Events.js";

const DOC = JSON.parse(readFileSync(new URL("../fixtures/v22-fill-events-doc-examples.json", import.meta.url), "utf8")) as {
  examples: Record<string, { b64: string; bytes: number }>;
};
export const DOC_TOKENS: string[] = Object.values(DOC.examples).map((e) => e.b64);
export const W = Keypair.generate().publicKey.toBase58();
export const MATCHER = Keypair.generate().publicKey.toBase58();
export const OTHER = Keypair.generate().publicKey.toBase58();
export const WRAPPERS: ReadonlySet<string> = new Set([W]);
export const key = (n: number): string => new PublicKey(new Uint8Array(32).fill(n)).toBase58();
export const MARKETS = [key(1), key(2), key(3)];
export const gate: EventGate = {
  isKnownMarket: (s) => MARKETS.includes(s),
  versionOf: () => 22,
  generationOf: (_s, a) => (a === 0 ? 7n : null),
};

export function txLogs(tokens: string[], extra: string[] = []): string[] {
  return [
    `Program ${W} invoke [1]`,
    "Program log: Instruction: TradeCpi",
    `Program ${MATCHER} invoke [2]`,
    "Program log: matcher fill",
    `Program ${MATCHER} consumed 5000 of 900000 compute units`,
    `Program ${MATCHER} success`,
    ...extra,
    ...tokens.map((t) => `Program data: ${t}`),
    `Program ${W} consumed 90000 of 1400000 compute units`,
    `Program ${W} success`,
    `Program ${OTHER} invoke [1]`,
    `Program ${OTHER} success`,
  ];
}

export function wrapperIxs(): RawInstruction[] {
  const tag = [...EVENT_EMITTING_TAGS][0];
  return [0, 1].map((i) => ({ programId: W, accounts: [key(9), ...MARKETS, key(10), key(11)], data: new Uint8Array([tag, 0, 0]), ixIndex: i, innerIndex: -1 }));
}

/** Fixture set: [logs, ixs] pairs covering every doc example singly and together. */
export function fixtureTxs(): Array<{ logs: string[]; ixs: RawInstruction[] }> {
  const ixs = wrapperIxs();
  const out = DOC_TOKENS.map((t) => ({ logs: txLogs([t]), ixs }));
  out.push({ logs: txLogs(DOC_TOKENS), ixs });
  out.push({ logs: txLogs([DOC_TOKENS.join(" ")]), ixs });
  return out;
}
