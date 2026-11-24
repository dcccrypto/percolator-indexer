/**
 * The mark-EWMA fallback must price asset N with asset N's own mark, not asset 0's.
 *
 * When an asset's effective_price can't be used (0 or out of range), the poll, webhook and stream paths fall back to
 * a mark EWMA. They read the wrapper-config `mark_ewma_e6` (account offset 248). That field is per market and holds
 * asset 0's value. On devnet slot 525623852 it equals asset 0's oracle-profile `mark_ewma_e6` (slot +168) in 71/71
 * markets, and differs from it on non-zero assets (2xhs#1: profile 9_694_483, cfg 78_685_884). So a fallback on
 * asset 1+ stored asset 0's price. Report IDX-DIFF-REPORT-SANEPRICEGUARDS, lead WRONG-ASSET-FALLBACK.
 *
 * Two sources of bytes:
 *   - slabs stamped from the SDK LAYOUT_V22 table (two assets with different profile marks),
 *   - real devnet captures of two 5-asset v2.2 markets (tests/fixtures/v22-multiasset-markets-devnet-20261124.fixture.json).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { readFileSync } from "node:fs";
import { LAYOUT_V22 } from "@percolatorct/sdk";

vi.mock("@percolatorct/shared", async (orig) => {
  const actual = await orig<typeof import("@percolatorct/shared")>();
  return { ...actual, config: { ...actual.config, allProgramIds: [] }, getConnection: vi.fn(), withRetry: vi.fn(async (fn: () => Promise<unknown>) => fn()) };
});
vi.mock("../../src/db/insertTradeRow.js", () => ({ insertTradeRow: vi.fn(), insertTradeRows: vi.fn(), tradeKey: vi.fn() }));
vi.mock("../../src/lib/skippedSignatures.js", () => ({ recordSkippedSignatures: vi.fn(), assertSkippedSignatureSinkReady: vi.fn() }));

import { TradeIndexerPolling } from "../../src/services/TradeIndexer.js";
import { readMarkPriceE6 } from "../../src/parsers/markPrice.js";
import { parseMarkPriceE6FromAccountBytes } from "../../src/routes/webhook.js";
import { resetLotExpCache } from "../../src/parsers/lotScale.js";
import { buildMarket } from "../helpers/v22Fixtures.js";

const L = LAYOUT_V22;
const slotBase = (a: number) => L.marketGroupOff + L.marketGroupLen + a * L.assetSlotStride;
const CFG_EWMA_OFF = 16 + 232; // WrapperConfigV17.mark_ewma_e6, account offset 248
const PROFILE_EWMA_OFF = 168; // AssetOracleProfileV17.mark_ewma_e6, offset in the slot's oracle profile
const profileEwmaOff = (a: number) => slotBase(a) + L.wrapperSlot.oracleProfile + PROFILE_EWMA_OFF;
const effOff = (a: number) => slotBase(a) + L.wrapperSlotLen + L.assetState.effectivePrice;
const SYN = "11111111111111111111111111111111";

/** All three fallback sites, in per-token dollars (lot 0 everywhere here, so e6 / 1e6). */
async function allPaths(data: Buffer, slab: string, asset: number): Promise<{ poll: number; stream: number | null; webhook: number }> {
  const conn = { getAccountInfo: async () => ({ data }) };
  resetLotExpCache();
  const poll = await (new TradeIndexerPolling() as any).readMarkPriceFromSlab(conn, slab, asset);
  resetLotExpCache();
  const e6 = await readMarkPriceE6(conn as never, slab, asset);
  resetLotExpCache();
  const webhook = parseMarkPriceE6FromAccountBytes(new Uint8Array(data), asset, slab);
  return { poll, stream: e6 === null ? null : e6 / 1_000_000, webhook };
}

/** v2.2 market from LAYOUT_V22: cfg mark EWMA (= asset 0's, as on chain) and each asset's own profile mark EWMA; effective_price 0. */
function market(profileEwmaE6: bigint[], cfgEwmaE6: bigint): Buffer {
  const d = Buffer.from(buildMarket(L, { slots: profileEwmaE6.length }));
  d.writeBigUInt64LE(cfgEwmaE6, CFG_EWMA_OFF);
  profileEwmaE6.forEach((v, a) => d.writeBigUInt64LE(v, profileEwmaOff(a)));
  return d;
}

describe("mark fallback on a LAYOUT_V22 two-asset market", () => {
  beforeEach(() => resetLotExpCache());

  it("asset 1 falls back to its own profile mark_ewma, not asset 0's cfg mark", async () => {
    const d = market([1_500_000n, 9_000_000n], 1_500_000n);
    expect(await allPaths(d, SYN, 1)).toEqual({ poll: 9, stream: 9, webhook: 9 });
  });

  it("asset 0 still falls back to its mark (cfg @248 == profile 0 on chain)", async () => {
    const d = market([1_500_000n, 9_000_000n], 1_500_000n);
    expect(await allPaths(d, SYN, 0)).toEqual({ poll: 1.5, stream: 1.5, webhook: 1.5 });
  });

  it("asset 1 with no profile mark yields no price rather than asset 0's", async () => {
    const d = market([1_500_000n, 0n], 1_500_000n);
    expect(await allPaths(d, SYN, 1)).toEqual({ poll: 0, stream: null, webhook: 0 });
  });

  it("an asset index past the market's slots yields no price rather than asset 0's", async () => {
    const d = market([1_500_000n, 9_000_000n], 1_500_000n);
    expect(await allPaths(d, SYN, 2)).toEqual({ poll: 0, stream: null, webhook: 0 });
  });

  it("a profile mark at the 1e12 cap is rejected like the cfg one", async () => {
    const d = market([1_500_000n, 1_000_000_000_000n], 1_500_000n);
    expect(await allPaths(d, SYN, 1)).toEqual({ poll: 0, stream: null, webhook: 0 });
  });

  it("an unknown VERSION still yields no price", async () => {
    const d = market([1_500_000n, 9_000_000n], 1_500_000n);
    d.writeUInt16LE(99, 8);
    expect(await allPaths(d, SYN, 1)).toEqual({ poll: 0, stream: null, webhook: 0 });
  });

  it("without an asset index the stream reader keeps the market-wide cfg mark", async () => {
    const d = market([1_500_000n, 9_000_000n], 1_500_000n);
    expect(await readMarkPriceE6({ getAccountInfo: async () => ({ data: d }) } as never, SYN)).toBe(1_500_000);
  });
});

interface FixtureAsset { asset: number; lot_exp: number; effective_price_e6: string; profile_mark_ewma_e6: string; cfg_mark_ewma_248_e6: string }
const fx = JSON.parse(readFileSync(new URL("../fixtures/v22-multiasset-markets-devnet-20261124.fixture.json", import.meta.url), "utf8")) as {
  slot: number; program: string; markets: { slab: string; data: string; assets: FixtureAsset[] }[];
};

describe("mark fallback on real devnet 5-asset v2.2 markets (slot 525623852)", () => {
  it("the capture is the one the report measured", () => {
    expect(fx.slot).toBe(525_623_852);
    expect(fx.program).toBe("6kpg2wi7vwkn7E9rvodXSktYRhna8TWjiZrC1dBek6NM");
    expect(fx.markets.map((m) => m.slab).sort()).toEqual([
      "2xhsLP9NLkUMMsCsyzkHM9R1zsVU2V2vDo6v9WuPBzVw",
      "869FCGZEEvqH56RN5KMBeGRL3QmerJ7d9Wniq1bstacd",
    ]);
    for (const m of fx.markets) {
      const d = Buffer.from(m.data, "base64");
      expect(d.readUInt16LE(8)).toBe(19);
      for (const a of m.assets) {
        expect(d.readBigUInt64LE(effOff(a.asset)).toString()).toBe(a.effective_price_e6);
        expect(d.readBigUInt64LE(profileEwmaOff(a.asset)).toString()).toBe(a.profile_mark_ewma_e6);
        expect(d.readBigUInt64LE(CFG_EWMA_OFF).toString()).toBe(a.cfg_mark_ewma_248_e6);
        expect(d[slotBase(a.asset) + L.wrapperSlot.profileLotExp]).toBe(a.lot_exp);
      }
      // On chain the cfg mark is asset 0's profile mark.
      expect(m.assets[0].cfg_mark_ewma_248_e6).toBe(m.assets[0].profile_mark_ewma_e6);
    }
    // ...and the case that matters: non-zero assets whose own mark differs from it (7 of the 8 non-zero assets here).
    const differ = fx.markets.flatMap((m) => m.assets.filter((a) => a.asset > 0 && a.profile_mark_ewma_e6 !== a.cfg_mark_ewma_248_e6));
    expect(differ).toHaveLength(7);
  });

  const cases = fx.markets.flatMap((m) => m.assets.map((a) => ({ slab: m.slab, data: m.data, ...a, label: `${m.slab.slice(0, 6)}#${a.asset}` })));
  it.each(cases)("$label: with effective_price zeroed, every path falls back to the asset's own profile mark", async (c) => {
    const d = Buffer.from(c.data, "base64");
    d.writeBigUInt64LE(0n, effOff(c.asset));
    const want = Number(c.profile_mark_ewma_e6) / 1_000_000;
    expect(await allPaths(d, c.slab, c.asset)).toEqual({ poll: want, stream: want, webhook: want });
  });
});
