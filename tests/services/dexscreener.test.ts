import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
}));

import {
  resolveIdentitiesByCa,
  chunkForDexScreener,
  DEXSCREENER_MAX_BATCH,
} from "../../src/services/dexscreener.js";

const CA = "So11111111111111111111111111111111111111112";
const CA2 = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

function mockFetch(body: unknown, ok = true, status = 200) {
  const fn = vi.fn().mockResolvedValue({ ok, status, json: async () => body });
  vi.stubGlobal("fetch", fn);
  return fn;
}

describe("resolveIdentitiesByCa", () => {
  beforeEach(() => vi.unstubAllGlobals());
  afterEach(() => vi.unstubAllGlobals());

  it("returns empty and does not fetch when no address is valid base58", async () => {
    const f = mockFetch({});
    const out = await resolveIdentitiesByCa(["not-an-address", "0OIl", ""]);
    expect(out.size).toBe(0);
    expect(f).not.toHaveBeenCalled();
  });

  it("throws when more than the batch limit is requested", async () => {
    const f = mockFetch({});
    const many = Array.from({ length: DEXSCREENER_MAX_BATCH + 1 }, () => CA);
    await expect(resolveIdentitiesByCa(many)).rejects.toThrow(/exceeds/);
    expect(f).not.toHaveBeenCalled();
  });

  it("filters invalid addresses out of the request path", async () => {
    const f = mockFetch({ pairs: [] });
    await resolveIdentitiesByCa([CA, "bad", CA2]);
    expect(f.mock.calls[0][0]).toBe(`https://api.dexscreener.com/latest/dex/tokens/${CA},${CA2}`);
  });

  it("returns empty on a non-ok response", async () => {
    mockFetch({}, false, 429);
    expect((await resolveIdentitiesByCa([CA])).size).toBe(0);
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

  it("ignores pairs where the token is only the quote side or on another chain", async () => {
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
