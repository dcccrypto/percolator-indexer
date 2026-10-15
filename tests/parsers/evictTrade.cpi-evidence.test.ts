/**
 * v2.2 EvictAndTradeCpi (119): the wrapped TradeCpi's matcher program / context sit at [4]/[5] of the
 * UNWRAPPED account list. cpiEvidenceFromParsed must read the unwrapped list, or it looks one account off
 * and silently degrades the fill to "unverified" (negative control below proves the raw list does).
 */
import { describe, it, expect } from "vitest";
import { cpiEvidenceFromParsed } from "../../src/parsers/percolatorTxParser.js";
import { unwrapEvictAndTradeIx } from "../../src/parsers/evictTrade.js";
import { encodeBase58 } from "../../src/lib/base58.js";
import { pk } from "../helpers/v22Fixtures.js";

function singleMatcherCall(): Uint8Array {
  const d = new Uint8Array(43);
  d[0] = 0; // MATCHER_TAG_SINGLE
  new DataView(d.buffer).setBigUint64(1, 7n, true);
  return d;
}

describe("cpiEvidenceFromParsed with the 119-unwrapped account list", () => {
  const victim = pk(), trader = pk(), slab = pk(), matcher = pk(), ctx = pk();
  // TradeCpi list: [0] trader, [1] slab, [2] .., [3] .., [4] matcher program, [5] matcher context
  const tradeCpi = [trader, slab, pk(), pk(), matcher, ctx];
  const evictRaw = [victim, ...tradeCpi];
  const inner = [{ index: 0, instructions: [{ programId: matcher, data: encodeBase58(singleMatcherCall()) }] }];
  const body = new Uint8Array(60); body[0] = 119;

  it("finds the matcher call and context at [4]/[5] after unwrapping", () => {
    const { accounts } = unwrapEvictAndTradeIx(body, evictRaw);
    const ev = cpiEvidenceFromParsed({ accounts: evictRaw }, 0, inner, null, false, accounts);
    expect(ev.kind).toBe("call");
    expect(ev.kind === "call" && ev.matcherContext).toBe(ctx.toBase58());
  });

  it("NEGATIVE CONTROL: the raw 119 list is one account off and finds no matcher call", () => {
    const ev = cpiEvidenceFromParsed({ accounts: evictRaw }, 0, inner, null, false);
    expect(ev.kind).not.toBe("call");
  });
});
