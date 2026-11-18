import { describe, expect, it, vi } from "vitest";
import { PublicKey } from "@solana/web3.js";
import { parsePortfolioV17, resolvePortfolioLayout } from "@percolatorct/sdk";
import { decodePortfolio, fetchPortfolios } from "../../src/layout/portfolio.js";
import { LAYOUT_V21, LAYOUT_V22, buildPortfolio, stampHeader } from "../helpers/v22Fixtures.js";

const legs = [
  { index: 0, assetIndex: 1, marketId: 7n, side: 0, basisPosQ: 1_000n },
  { index: 15, assetIndex: 5, marketId: 4242n, side: 1, basisPosQ: -2_500n },
];

describe("J-IDX-BUG-PORTFOLIODECODENULL leads", () => {
  it("lead 1: decodePortfolio throws (documented) for wrong-length / unknown VERSION; fetchPortfolios (only caller) catches", async () => {
    const odd = stampHeader(new Uint8Array(10_607), 2, 19);
    expect(() => decodePortfolio(odd)).toThrow();
    expect(() => decodePortfolio(stampHeader(new Uint8Array(10_603), 2, 99))).toThrow();
    const getProgramAccounts = vi.fn().mockResolvedValue([
      { pubkey: new PublicKey(new Uint8Array(32).fill(1)), account: { data: Buffer.from(odd) } },
    ]);
    const r = await fetchPortfolios({ getProgramAccounts } as never, new PublicKey("11111111111111111111111111111111"), LAYOUT_V22);
    expect(r.portfolios).toHaveLength(0);
    expect(r.skipped).toHaveLength(1);
  });

  it("lead 2: v2.2 10,603 B uses stride 217 and matches the SDK layout-aware parser; v2.1 uses 152", () => {
    for (const L of [LAYOUT_V22, LAYOUT_V21]) {
      const d = buildPortfolio(L, legs);
      const sdk = parsePortfolioV17(d);
      const p = decodePortfolio(d)!;
      expect(p.legStride).toBe(resolvePortfolioLayout(d, { parser: "t" }).portfolio.legStride);
      expect(p.legStride).toBe(L === LAYOUT_V22 ? 217 : 152);
      expect(p.capital).toBe(sdk.capital);
      expect(p.raw.legs).toEqual(sdk.legs);
      expect(p.positions.map((x) => x.basisPosQ)).toEqual([1_000n, -2_500n]);
    }
  });

  it("lead 3+4: scan filters carry kind memcmp '3' (= base58 of 0x02); no commitment is passed (connection default)", async () => {
    const k = new Uint8Array(32);
    k[31] = 2; // base58 of a 32-byte key ending in 0x02 is "1...13": leading zeros -> '1', then digit 2 -> '3'
    expect(new PublicKey(k).toBase58().slice(-1)).toBe("3");
    const getProgramAccounts = vi.fn().mockResolvedValue([]);
    await fetchPortfolios({ getProgramAccounts } as never, new PublicKey("11111111111111111111111111111111"), LAYOUT_V22);
    const [, cfg] = getProgramAccounts.mock.calls[0];
    expect(cfg.filters).toContainEqual({ memcmp: { offset: 10, bytes: "3" } });
    expect(cfg.commitment).toBeUndefined();
  });
});
