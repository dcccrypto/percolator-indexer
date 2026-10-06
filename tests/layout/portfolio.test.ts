import { describe, expect, it, vi } from "vitest";
import { Keypair, PublicKey } from "@solana/web3.js";
import { UnknownLayoutError, portfolioFilterForLayout } from "@percolatorct/sdk";
import { decodePortfolio, fetchPortfolios } from "../../src/layout/portfolio.js";
import { LAYOUT_V21, LAYOUT_V22, REAL_PORTFOLIO_V21, buildMarket, buildPortfolio, pk, stampHeader } from "../helpers/v22Fixtures.js";

const code = (f: () => unknown): string | undefined => { try { f(); } catch (e) { return e instanceof UnknownLayoutError ? e.code : `other:${String(e)}`; } return undefined; };

describe("decodePortfolio: v2.1 (VERSION 18) is intact", () => {
  it("REAL devnet capture: 9,563 B, stride 152, decodes with its active leg(s)", () => {
    expect(REAL_PORTFOLIO_V21.length).toBe(9563);
    const p = decodePortfolio(REAL_PORTFOLIO_V21)!;
    expect([p.version, p.accountLen, p.legStride]).toEqual([18, 9563, 152]);
    expect(p.positions.length).toBeGreaterThan(0);
    for (const pos of p.positions) { expect(pos.bandLiqPending).toBeUndefined(); expect(pos.rentSnap).toBeUndefined(); } // v2.2-only fields absent
  });
});

describe("decodePortfolio: v2.2 variant B (VERSION 19)", () => {
  it("10,603 B account, 217 B legs: a leg in slot 15 (deepest, most stride-sensitive) and slot 0 decode exactly", () => {
    const owner = pk();
    const d = buildPortfolio(LAYOUT_V22, [
      { index: 0, assetIndex: 0, marketId: 7n, side: 0, basisPosQ: 1_000n },
      { index: 15, assetIndex: 5, marketId: 4242n, side: 1, basisPosQ: -2_500n },
    ], owner);
    expect(d.length).toBe(10_603);
    // independent literal check of the stride (ledger: leg 217, legs start 356)
    expect(d[356 + 15 * 217]).toBe(1);
    const p = decodePortfolio(d)!;
    expect([p.version, p.accountLen, p.legStride, p.owner]).toEqual([19, 10_603, 217, owner.toBase58()]);
    expect(p.positions.map((x) => [x.assetIndex, x.marketId, x.side])).toEqual([[0, 7n, "long"], [5, 4242n, "short"]]);
    expect(p.positions[1].basisPosQ).toBe(-2_500n);
    expect(p.positions[0].bandLiqPending).toBe(false); // v2.2 fields present
  });

  it("NEGATIVE CONTROL: a v2.2 portfolio read with the v2.1 stride (152) would put leg 15 in the wrong place", () => {
    const d = buildPortfolio(LAYOUT_V22, [{ index: 15, assetIndex: 5, marketId: 4242n, side: 1, basisPosQ: -2_500n }]);
    expect(d[356 + 15 * 152]).not.toBe(1); // the v2.1 literal lands on zeros
    expect(decodePortfolio(d)!.positions).toHaveLength(1); // the VERSION-keyed decoder finds it
  });

  it("refuses: wrong length for the VERSION, engine-discriminator mismatch, unknown VERSION", () => {
    expect(code(() => decodePortfolio(new Uint8Array(buildPortfolio(LAYOUT_V22)).subarray(0, 10_091)))).toBe("BAD_LENGTH"); // the stage-A length
    const bad = buildPortfolio(LAYOUT_V22);
    new DataView(bad.buffer).setUint16(114, 18, true);
    expect(code(() => decodePortfolio(bad))).toBe("DISCRIMINATOR_MISMATCH");
    expect(code(() => decodePortfolio(stampHeader(new Uint8Array(10_603), 2, 20)))).toBe("UNKNOWN_VERSION");
  });

  it("returns null (not an error) for non-portfolio accounts and legacy v1 slabs", () => {
    expect(decodePortfolio(buildMarket(LAYOUT_V22))).toBeNull();
    expect(decodePortfolio(new Uint8Array(10_603))).toBeNull();
    expect(decodePortfolio(new TextEncoder().encode("PERCOLAT" + "x".repeat(600)))).toBeNull();
  });
});

describe("fetchPortfolios", () => {
  it("scans by VERSION: size filter + VERSION memcmp + kind memcmp, and a bad account is skipped, not thrown", async () => {
    const good = buildPortfolio(LAYOUT_V22, [{ index: 2, assetIndex: 1, marketId: 9n, side: 0, basisPosQ: 5n }]);
    const bad = buildPortfolio(LAYOUT_V22);
    new DataView(bad.buffer).setUint16(114, 18, true);
    const k1 = Keypair.generate().publicKey, k2 = Keypair.generate().publicKey;
    const getProgramAccounts = vi.fn(async () => [{ pubkey: k1, account: { data: Buffer.from(good) } }, { pubkey: k2, account: { data: Buffer.from(bad) } }]);
    const r = await fetchPortfolios({ getProgramAccounts } as never, new PublicKey("11111111111111111111111111111111"), LAYOUT_V22);
    expect(r.portfolios.map((x) => x.pubkey)).toEqual([k1.toBase58()]);
    expect(r.skipped.map((x) => x.pubkey)).toEqual([k2.toBase58()]);
    const filters = (getProgramAccounts.mock.calls[0] as unknown as [PublicKey, { filters: Array<Record<string, any>> }])[1].filters;
    const f22 = portfolioFilterForLayout(LAYOUT_V22);
    expect(filters[0]).toEqual({ dataSize: 10_603 });
    expect(filters[1].memcmp).toEqual({ offset: 8, bytes: f22.versionMemcmp.bytes });
    expect(filters[2].memcmp).toEqual({ offset: 10, bytes: "3" });
    expect(f22.versionMemcmp.bytes).not.toBe(portfolioFilterForLayout(LAYOUT_V21).versionMemcmp.bytes);
  });
});
