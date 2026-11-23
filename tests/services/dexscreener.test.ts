import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
}));

import {
  resolveIdentitiesByCa,
  chunkForDexScreener,
  DEXSCREENER_MAX_BATCH,
} from "../../src/services/dexscreener.js";
import { MAX_SYMBOL_LEN } from "../../src/services/tokenMetadata.js";

const API = "https://api.dexscreener.com/latest/dex/tokens";
const CA = "So11111111111111111111111111111111111111112";
const CA2 = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

function mockFetch(body: unknown, ok = true, status = 200) {
  const json = vi.fn(async () => body);
  const fn = vi.fn().mockResolvedValue({ ok, status, json });
  vi.stubGlobal("fetch", fn);
  return { fn, json };
}

const pair = (address: string, symbol: string, extra: Record<string, unknown> = {}) => ({
  chainId: "solana",
  baseToken: { address, symbol, name: symbol },
  ...extra,
});

describe("resolveIdentitiesByCa", () => {
  beforeEach(() => vi.unstubAllGlobals());
  afterEach(() => vi.unstubAllGlobals());

  it("returns empty and does not fetch when no address is valid base58", async () => {
    const { fn } = mockFetch({});
    const out = await resolveIdentitiesByCa(["not-an-address", "0OIl", ""]);
    expect(out.size).toBe(0);
    expect(fn).not.toHaveBeenCalled();
  });

  it("throws when more than the batch limit is requested", async () => {
    const { fn } = mockFetch({});
    const many = Array.from({ length: DEXSCREENER_MAX_BATCH + 1 }, () => CA);
    await expect(resolveIdentitiesByCa(many)).rejects.toThrow(/exceeds/);
    expect(fn).not.toHaveBeenCalled();
  });

  it("filters invalid addresses out of the request path", async () => {
    const { fn } = mockFetch({ pairs: [] });
    await resolveIdentitiesByCa([CA, "bad", CA2]);
    expect(fn.mock.calls[0][0]).toBe(`${API}/${CA},${CA2}`);
  });

  it("returns empty when fetch throws", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("boom")));
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
  });

  it("returns empty when a non-Error is thrown", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue("str"));
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
  });

  it("treats a missing or non-array pairs field as no data", async () => {
    mockFetch({ pairs: null });
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
    mockFetch(null);
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
  });

  it("ignores pairs for another base token or on another chain", async () => {
    mockFetch({
      pairs: [
        { chainId: "solana", baseToken: { address: "other", symbol: "X" }, info: {} },
        { chainId: "ethereum", baseToken: { address: CA, symbol: "ETHY" } },
        null,
      ],
    });
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
  });

  it("picks the deepest-liquidity base pair and sanitizes text", async () => {
    mockFetch({
      pairs: [
        { chainId: "solana", baseToken: { address: CA, symbol: "LOW", name: "Low" }, liquidity: { usd: 10 } },
        {
          chainId: "solana",
          baseToken: { address: CA, symbol: "<b>WSOL</b>", name: "Wrapped SOL" },
          liquidity: { usd: 1000 },
        },
        { baseToken: { address: CA, symbol: "NOLIQ", name: "n" } },
      ],
    });
    const out = await resolveIdentitiesByCa([CA]);
    expect(out.get(CA)).toEqual({ symbol: "bWSOL/b", name: "Wrapped SOL", logoUrl: null });
  });

  it("falls back to a shallower pair's logo when the best has none", async () => {
    mockFetch({
      pairs: [
        { chainId: "solana", baseToken: { address: CA, symbol: "A", name: "A" }, liquidity: { usd: 100 } },
        {
          chainId: "solana",
          baseToken: { address: CA, symbol: "A", name: "A" },
          liquidity: { usd: 5 },
          info: { imageUrl: "https://img.example/a.png" },
        },
        {
          chainId: "solana",
          baseToken: { address: CA, symbol: "A" },
          liquidity: { usd: 1 },
          info: { imageUrl: "javascript:alert(1)" },
        },
      ],
    });
    const out = await resolveIdentitiesByCa([CA]);
    expect(out.get(CA)?.logoUrl).toBe("https://img.example/a.png");
  });

  it("uses the best pair's own logo when present", async () => {
    mockFetch({
      pairs: [
        {
          chainId: "solana",
          baseToken: { address: CA, symbol: "A" },
          liquidity: { usd: 9 },
          info: { imageUrl: "https://img.example/best.png" },
        },
      ],
    });
    const out = await resolveIdentitiesByCa([CA]);
    expect(out.get(CA)).toEqual({ symbol: "A", name: null, logoUrl: "https://img.example/best.png" });
  });

  it("omits an address whose identity is entirely empty", async () => {
    mockFetch({ pairs: [{ chainId: "solana", baseToken: { address: CA, symbol: "<>", name: "  " } }] });
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
  });

  it("resolves several addresses from one flat pair list", async () => {
    mockFetch({
      pairs: [
        { chainId: "solana", baseToken: { address: CA, symbol: "A" } },
        { chainId: "solana", baseToken: { address: CA2, symbol: "B" } },
      ],
    });
    const out = await resolveIdentitiesByCa([CA, CA2]);
    expect(out.get(CA)?.symbol).toBe("A");
    expect(out.get(CA2)?.symbol).toBe("B");
  });
});

// The tests below each name the mutant(s) of resolveIdentitiesByCa they kill
// (mutation pass J-20261123-IDX-REPORT-DEXSCREENERIDENTITYMUT, M01..M34; M32,
// `Array.isArray(pairs)` -> `pairs ?? []`, is equivalent and survives).

describe("resolveIdentitiesByCa: request shape", () => {
  beforeEach(() => vi.unstubAllGlobals());
  afterEach(() => vi.unstubAllGlobals());

  // M01: `>=` would throw on a full chunk, and chunkForDexScreener emits exactly 30.
  it("accepts exactly the batch limit in one request", async () => {
    const { fn } = mockFetch({ pairs: [] });
    const full = Array.from({ length: DEXSCREENER_MAX_BATCH }, () => CA);
    await expect(resolveIdentitiesByCa(full)).resolves.toBeInstanceOf(Map);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  // M05 (unanchored), M06 (no $), M07 (0/O/I/l alphabet), M09 (max length 88).
  it.each([
    ["path segment after a valid CA", `${CA}/../../pairs/solana/x`],
    ["extra CA smuggled via comma", `${CA},${CA2}`],
    ["query string after a valid CA", `${CA}?chain=ethereum`],
    ["valid CA after a prefix", `x/${CA}`],
    ["non-base58 '0' in a CA-length string", `0${CA.slice(1)}`],
    ["two CAs concatenated (87 chars)", `${CA}${CA2}`],
  ])("keeps a malformed address out of the path: %s", async (_label, bad) => {
    const { fn } = mockFetch({ pairs: [] });
    await resolveIdentitiesByCa([bad, CA]);
    expect(fn).toHaveBeenCalledTimes(1);
    expect(fn.mock.calls[0][0]).toBe(`${API}/${CA}`);
  });

  // M10: a 32-character base58 address is valid (e.g. the system program id).
  it("accepts a 32-character address", async () => {
    const short = "11111111111111111111111111111111";
    const { fn } = mockFetch({ pairs: [] });
    await resolveIdentitiesByCa([short]);
    expect(fn.mock.calls[0][0]).toBe(`${API}/${short}`);
  });
});

describe("resolveIdentitiesByCa: which pair names the token", () => {
  beforeEach(() => vi.unstubAllGlobals());
  afterEach(() => vi.unstubAllGlobals());

  // M13: base58 is case-sensitive; a case-variant address is a different token.
  it("does not match a pair whose address differs only in case", async () => {
    mockFetch({ pairs: [pair(CA.toLowerCase(), "FAKE")] });
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
  });

  // M15: a different token that shares a long prefix must not match.
  it("does not match a pair whose address only shares a prefix", async () => {
    mockFetch({ pairs: [pair(`${CA.slice(0, 42)}9`, "FAKE")] });
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
  });

  // M14: real responses carry quoteToken; a token that only appears as the
  // quote side must not take the base token's symbol, name or logo.
  it("ignores a pair where the CA is the quote token, even with quoteToken present", async () => {
    mockFetch({
      pairs: [
        {
          chainId: "solana",
          baseToken: { address: CA2, symbol: "USDC", name: "USD Coin" },
          quoteToken: { address: CA, symbol: "SOL" },
          info: { imageUrl: "https://img.example/usdc.png" },
        },
      ],
    });
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
  });

  // M25: a pair with no chainId is treated as Solana (documented in identityFromPairs).
  it("accepts a best pair that has no chainId", async () => {
    mockFetch({ pairs: [{ baseToken: { address: CA, symbol: "WSOL" } }] });
    expect((await resolveIdentitiesByCa([CA])).get(CA)?.symbol).toBe("WSOL");
  });

  // M21: negative liquidity counts as 0, so the tie keeps API order.
  it("ranks a negative-liquidity pair like a no-liquidity pair (API order on tie)", async () => {
    mockFetch({
      pairs: [pair(CA, "FIRST", { liquidity: { usd: -5 } }), pair(CA, "SECOND")],
    });
    expect((await resolveIdentitiesByCa([CA])).get(CA)?.symbol).toBe("FIRST");
  });

  // M30: symbol is capped at the symbol column width, not the name width.
  it("caps the symbol at MAX_SYMBOL_LEN", async () => {
    mockFetch({ pairs: [pair(CA, "S".repeat(MAX_SYMBOL_LEN + 20))] });
    expect((await resolveIdentitiesByCa([CA])).get(CA)?.symbol).toHaveLength(MAX_SYMBOL_LEN);
  });
});

describe("resolveIdentitiesByCa: logo source", () => {
  beforeEach(() => vi.unstubAllGlobals());
  afterEach(() => vi.unstubAllGlobals());

  // M27: the fallback is the DEEPEST pair with a logo, not the shallowest.
  it("falls back to the next-deepest logo, not the last one", async () => {
    mockFetch({
      pairs: [
        pair(CA, "A", { liquidity: { usd: 1000 } }),
        pair(CA, "A", { liquidity: { usd: 1 }, info: { imageUrl: "https://img.example/spoof.png" } }),
        pair(CA, "A", { liquidity: { usd: 50 }, info: { imageUrl: "https://img.example/real.png" } }),
      ],
    });
    expect((await resolveIdentitiesByCa([CA])).get(CA)?.logoUrl).toBe("https://img.example/real.png");
  });

  // M28: the fallback logo is sanitized too.
  it("skips an unsafe fallback logo", async () => {
    mockFetch({
      pairs: [
        pair(CA, "A", { liquidity: { usd: 1000 } }),
        pair(CA, "A", { liquidity: { usd: 50 }, info: { imageUrl: "javascript:alert(1)" } }),
        pair(CA, "A", { liquidity: { usd: 5 }, info: { imageUrl: "data:image/svg+xml,<svg onload=alert(1)>" } }),
      ],
    });
    expect((await resolveIdentitiesByCa([CA])).get(CA)?.logoUrl).toBeNull();
  });

  // M29: another token's logo in the same batch must not attach to this one.
  it("never takes a logo from another token's pair in the same response", async () => {
    mockFetch({
      pairs: [
        pair(CA, "A", { liquidity: { usd: 100 } }),
        pair(CA2, "B", { liquidity: { usd: 100 }, info: { imageUrl: "https://img.example/b.png" } }),
      ],
    });
    const out = await resolveIdentitiesByCa([CA, CA2]);
    expect(out.get(CA)).toEqual({ symbol: "A", name: "A", logoUrl: null });
    expect(out.get(CA2)?.logoUrl).toBe("https://img.example/b.png");
  });
});

describe("resolveIdentitiesByCa: non-ok bodies are never data", () => {
  beforeEach(() => vi.unstubAllGlobals());
  afterEach(() => vi.unstubAllGlobals());

  // M22 (any status parsed), M23 (only 5xx rejected). The body carries real
  // pairs so a parsed error page would produce an identity.
  it.each([429, 404, 403, 500, 503])("ignores a %i body even when it carries pairs", async (status) => {
    const { json } = mockFetch(
      { pairs: [pair(CA, "STALE", { info: { imageUrl: "https://img.example/x.png" } })] },
      false,
      status,
    );
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
    expect(json).not.toHaveBeenCalled();
  });
});

describe("chunkForDexScreener", () => {
  it("chunks by the default batch size", () => {
    const items = Array.from({ length: 65 }, (_, i) => i);
    const chunks = chunkForDexScreener(items);
    expect(chunks.map((c) => c.length)).toEqual([30, 30, 5]);
  });
  it("honours a custom size and empty input", () => {
    expect(chunkForDexScreener([1, 2, 3], 2)).toEqual([[1, 2], [3]]);
    expect(chunkForDexScreener([])).toEqual([]);
  });
});
