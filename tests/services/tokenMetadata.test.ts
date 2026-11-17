import { describe, it, expect, vi, afterEach } from "vitest";

vi.mock("@percolatorct/shared", () => ({
  createLogger: () => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() }),
}));

import {
  sanitizeText,
  sanitizeLogoUrl,
  fetchDasTokenMetadata,
  placeholderIdentity,
} from "../../src/services/tokenMetadata.js";

const HELIUS = "https://mainnet.helius-rpc.com/?api-key=x";
const MINT = "So11111111111111111111111111111111111111112";

function mockFetch(body: unknown, ok = true) {
  const fn = vi.fn().mockResolvedValue({ ok, json: async () => body });
  vi.stubGlobal("fetch", fn);
  return fn;
}

afterEach(() => vi.unstubAllGlobals());

describe("sanitizeText", () => {
  it("rejects non-strings and empty results", () => {
    expect(sanitizeText(5, 10)).toBeNull();
    expect(sanitizeText(undefined, 10)).toBeNull();
    expect(sanitizeText(" <>\u0000 ", 10)).toBeNull();
  });
  it("strips markup/control chars, trims and truncates", () => {
    expect(sanitizeText("  <b>Hi\n</b> ", 10)).toBe("bHi/b");
    expect(sanitizeText("abcdefgh", 3)).toBe("abc");
  });
});

describe("sanitizeLogoUrl", () => {
  it("rejects non-strings, empty, over-long and unparseable", () => {
    expect(sanitizeLogoUrl(null)).toBeNull();
    expect(sanitizeLogoUrl("")).toBeNull();
    expect(sanitizeLogoUrl("https://a.io/" + "x".repeat(600))).toBeNull();
    expect(sanitizeLogoUrl("not a url")).toBeNull();
  });
  it("rejects non-http protocols", () => {
    expect(sanitizeLogoUrl("data:image/svg+xml;base64,AAAA")).toBeNull();
    expect(sanitizeLogoUrl("ipfs://Qm123")).toBeNull();
  });
  it("accepts http and https", () => {
    expect(sanitizeLogoUrl("https://a.io/x.png")).toBe("https://a.io/x.png");
    expect(sanitizeLogoUrl("http://a.io")).toBe("http://a.io/");
  });
});

describe("fetchDasTokenMetadata", () => {
  it("returns null without fetching for non-Helius endpoints", async () => {
    const f = mockFetch({});
    expect(await fetchDasTokenMetadata("https://api.devnet.solana.com", MINT)).toBeNull();
    expect(f).not.toHaveBeenCalled();
  });

  it("returns null on a non-ok response", async () => {
    mockFetch({}, false);
    expect(await fetchDasTokenMetadata(HELIUS, MINT)).toBeNull();
  });

  it("returns null when fetch rejects (Error and non-Error)", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("net")));
    expect(await fetchDasTokenMetadata(HELIUS, MINT)).toBeNull();
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue("x"));
    expect(await fetchDasTokenMetadata(HELIUS, MINT)).toBeNull();
  });

  it("posts a getAsset request", async () => {
    const f = mockFetch({});
    await fetchDasTokenMetadata(HELIUS, MINT);
    const [url, init] = f.mock.calls[0];
    expect(url).toBe(HELIUS);
    expect(JSON.parse(init.body)).toMatchObject({ method: "getAsset", params: { id: MINT } });
  });

  it("returns all-null fields for an empty result", async () => {
    mockFetch({});
    expect(await fetchDasTokenMetadata(HELIUS, MINT)).toEqual({
      symbol: null,
      name: null,
      decimals: null,
      logoUrl: null,
    });
  });

  it("reads metadata, decimals and the direct link image", async () => {
    mockFetch({
      result: {
        content: {
          metadata: { symbol: "USDC", name: "USD Coin" },
          links: { image: "https://cdn.example/usdc.png" },
        },
        token_info: { decimals: 6 },
      },
    });
    expect(await fetchDasTokenMetadata(HELIUS, MINT)).toEqual({
      symbol: "USDC",
      name: "USD Coin",
      decimals: 6,
      logoUrl: "https://cdn.example/usdc.png",
    });
  });

  it("falls back to token_info.symbol and drops non-finite decimals", async () => {
    mockFetch({ result: { content: {}, token_info: { symbol: "TKN", decimals: "6" } } });
    const r = await fetchDasTokenMetadata(HELIUS, MINT);
    expect(r?.symbol).toBe("TKN");
    expect(r?.decimals).toBeNull();
    mockFetch({ result: { token_info: { decimals: Infinity } } });
    expect((await fetchDasTokenMetadata(HELIUS, MINT))?.decimals).toBeNull();
  });

  it("falls back to the files list: skips non-images, prefers cdn_uri, then uri", async () => {
    mockFetch({
      result: {
        content: {
          links: { image: "ipfs://nope" },
          files: [
            { mime: "video/mp4", uri: "https://v.example/v.mp4" },
            { mime: "image/png", cdn_uri: "ftp://bad", uri: "https://u.example/a.png" },
          ],
        },
      },
    });
    expect((await fetchDasTokenMetadata(HELIUS, MINT))?.logoUrl).toBe("https://u.example/a.png");

    mockFetch({
      result: {
        content: { files: [{ cdn_uri: "https://cdn.example/b.png", uri: "https://u.example/b.png" }] },
      },
    });
    expect((await fetchDasTokenMetadata(HELIUS, MINT))?.logoUrl).toBe("https://cdn.example/b.png");
  });

  it("returns a null logo when no file qualifies", async () => {
    mockFetch({ result: { content: { files: [{ mime: "image/png", uri: "ipfs://x" }, null] } } });
    expect((await fetchDasTokenMetadata(HELIUS, MINT))?.logoUrl).toBeNull();
    mockFetch({ result: { content: { files: "nope" } } });
    expect((await fetchDasTokenMetadata(HELIUS, MINT))?.logoUrl).toBeNull();
  });
});

describe("placeholderIdentity", () => {
  it("is visibly unnamed and never claims SOL", () => {
    expect(placeholderIdentity("ABCDEFGHIJKLMNOP")).toEqual({ symbol: "UNKNOWN", name: "Market ABCDEFGH" });
  });
});

describe("decimals and surrogate-safe truncation", () => {
  it("accepts only integer decimals in 0..255 (u8)", async () => {
    for (const bad of [-1, 6.5, 256, 300, 1e20]) {
      mockFetch({ result: { token_info: { decimals: bad } } });
      expect((await fetchDasTokenMetadata(HELIUS, MINT))?.decimals).toBeNull();
    }
    for (const good of [0, 9, 255]) {
      mockFetch({ result: { token_info: { decimals: good } } });
      expect((await fetchDasTokenMetadata(HELIUS, MINT))?.decimals).toBe(good);
    }
  });

  it("never cuts a surrogate pair in half", () => {
    const out = sanitizeText("a" + "😀".repeat(5), 4)!;
    expect(out).toBe("a😀😀😀");
    expect(Array.from(out)).toHaveLength(4);
    expect(/[\ud800-\udbff](?![\udc00-\udfff])/.test(sanitizeText("😀😀", 3)!)).toBe(false);
  });
});
