import { describe, it, expect, vi, afterEach } from "vitest";

vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
}));

import { resolveIdentitiesByCa } from "../../src/services/dexscreener.js";

const CA = "So11111111111111111111111111111111111111112";

const real = {
  chainId: "solana",
  baseToken: { address: CA, name: "Real Token", symbol: "REAL" },
  liquidity: { usd: 1_000_000 },
};
const spoof = (usd: unknown) => ({
  chainId: "solana",
  baseToken: { address: CA, name: "Spoof Token", symbol: "SPOOF" },
  liquidity: { usd },
});

async function pick(pairs: unknown[]) {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({ pairs }) }));
  return (await resolveIdentitiesByCa([CA])).get(CA)?.symbol;
}

const BAD: [string, unknown][] = [
  ["NaN", NaN],
  ["Infinity", Infinity],
  ["-Infinity", -Infinity],
  ["string (huge)", "999999999999"],
  ["null", null],
  ["undefined", undefined],
  ["object", {}],
];

describe("identityFromPairs liquidity ranking with bad values", () => {
  afterEach(() => vi.unstubAllGlobals());

  for (const [label, bad] of BAD) {
    for (const order of ["real-first", "spoof-first"] as const) {
      it(`deep real pair beats shallow spoof with liquidity=${label} (${order})`, async () => {
        const pairs = order === "real-first" ? [real, spoof(bad)] : [spoof(bad), real];
        expect(await pick(pairs)).toBe("REAL");
      });
    }
  }

  it("a spoof with several bad-liquidity pairs among real ones still loses", async () => {
    const pairs = [spoof(NaN), spoof("1e12"), real, spoof(Infinity), spoof(null)];
    expect(await pick(pairs)).toBe("REAL");
  });

  it("all-finite inputs: deepest wins, ties keep API order", async () => {
    const a = { ...spoof(10), baseToken: { address: CA, name: "A", symbol: "A" } };
    const b = { ...spoof(10), baseToken: { address: CA, name: "B", symbol: "B" } };
    expect(await pick([a, b, real])).toBe("REAL");
    expect(await pick([a, b])).toBe("A");
  });
});
