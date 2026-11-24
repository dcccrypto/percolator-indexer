/**
 * readMarkPriceFromSlab over real v2.2 slab bytes, with no parser mocks.
 *
 * The sibling TradeIndexer.readMarkPrice.test.ts mocks every parser and feeds 64 zero bytes. So lot_exp is always 0,
 * the mock lets wrapper magic and the effective price vary independently (a real v2.2 slab always has magic), and any
 * absolute-offset guess throws RangeError, which the outer catch turns into 0. Four wrong-price mutants hid behind that
 * (report IDX-REPORT-READMARKPRICETESTMUT):
 *   M04 lot_exp ignored on the booked effective price (stored 10^lot_exp too high),
 *   M03 asset 0's lot_exp used for asset N,
 *   M06 mark EWMA preferred over the booked effective price on wrapper-magic accounts,
 *   M18 unknown VERSION guesses a price from cfg offset 248.
 *
 * Two sources of bytes:
 *   - real devnet captures (tests/fixtures/*.fixture.json, VERSION 19, program 6kpg2wi7...),
 *   - slabs stamped from the SDK LAYOUT_V22 table via buildMarket, for shapes devnet does not have
 *     (two assets with different lot_exp, effective price != EWMA).
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
import { resetLotExpCache } from "../../src/parsers/lotScale.js";
import { buildMarket } from "../helpers/v22Fixtures.js";

const L = LAYOUT_V22;
const slotBase = (a: number) => L.marketGroupOff + L.marketGroupLen + a * L.assetSlotStride;
const EWMA_OFF = 16 + 232; // WrapperConfigV17.mark_ewma_e6 (account offset 248)

/** A real devnet market account from a checked-in capture, as a fresh copy. */
function captured(file: string, slab: string): Buffer {
  const j = JSON.parse(readFileSync(new URL(`../fixtures/${file}`, import.meta.url), "utf8"));
  const m = j.markets.find((x: { slab: string }) => x.slab === slab);
  if (!m) throw new Error(`${slab} not in ${file}`);
  return Buffer.from(m.data, "base64");
}
// 2026-11-20 devnet capture: asset 0 lot_exp 4, booked effective_price 16_290_000 e6 per lot (= cfg mark EWMA).
const DZF = "DZfeLdFFz3DMCKcBCtfpDSAq1Fnz4RHDoDtiNBFLc3o";
const realLot4 = () => captured("v22-markets-20261120.fixture.json", DZF);
// Slot 525470260 devnet capture: asset 0 lot_exp 0, booked effective_price 113_300_000 e6.
const S25 = "25xGmJg5ZaAckSXCnnvhSwsvzJqi82UqpR82UFtsZiNC";
const realLot0 = () => captured("rs1-resolved-market-devnet-20261123.fixture.json", S25);

/** v2.2 market stamped from LAYOUT_V22: per-asset lot_exp and booked effective_price (e6 per lot), optional EWMA. */
function market(assets: { lot: number; effE6?: bigint }[], markEwmaE6?: bigint, version?: number): Buffer {
  const d = buildMarket(L, { slots: assets.length, ...(markEwmaE6 !== undefined ? { markEwmaE6 } : {}) }, version ?? L.version);
  const dv = new DataView(d.buffer, d.byteOffset, d.byteLength);
  assets.forEach((a, i) => {
    d[slotBase(i) + L.wrapperSlot.profileLotExp] = a.lot;
    if (a.effE6 !== undefined) dv.setBigUint64(slotBase(i) + L.wrapperSlotLen + L.assetState.effectivePrice, a.effE6, true);
  });
  return Buffer.from(d);
}
const read = (data: Buffer, slab: string, asset: number): Promise<number> =>
  (new TradeIndexerPolling() as any).readMarkPriceFromSlab({ getAccountInfo: async () => ({ data }) }, slab, asset);
const SYN = "11111111111111111111111111111111";

describe("readMarkPriceFromSlab over real devnet v2.2 captures", () => {
  beforeEach(() => resetLotExpCache());

  // M04: a lot_exp 4 market is priced per token (16.29 / 10^4), not per lot (16.29).
  it("scales a captured lot_exp 4 market's booked effective_price to per-token", async () => {
    const d = realLot4();
    expect(d.readUInt16LE(8)).toBe(19);
    expect(d[slotBase(0) + L.wrapperSlot.profileLotExp]).toBe(4);
    expect(await read(d, DZF, 0)).toBeCloseTo(0.001629, 15);
  });

  it("reads a captured lot_exp 0 market's booked effective_price unchanged (slot 525470260)", async () => {
    expect(await read(realLot0(), S25, 0)).toBe(113.3);
  });

  // M06: on a real wrapper-magic slab the booked effective_price wins over a different cfg mark EWMA.
  it("prefers the booked effective_price over the cfg mark EWMA on a captured slab", async () => {
    const d = realLot0();
    d.writeBigUInt64LE(1_500_000n, EWMA_OFF);
    expect(await read(d, S25, 0)).toBe(113.3);
  });

  // M18: the same real bytes with an unknown VERSION yield 0, never the plausible u64 still sitting at offset 248.
  it("yields 0 for a captured slab restamped with an unknown VERSION", async () => {
    const d = realLot0();
    d.writeUInt16LE(99, 8);
    expect(d.readBigUInt64LE(EWMA_OFF)).toBe(113_300_000n);
    expect(await read(d, S25, 0)).toBe(0);
  });
});

describe("readMarkPriceFromSlab over LAYOUT_V22-stamped slabs", () => {
  beforeEach(() => resetLotExpCache());

  // M04 (lot ignored) and M03 (asset 0's lot_exp used for asset 1): asset 1 would be stored 10^6 too high.
  it("prices each asset's effective_price per token with THAT asset's lot_exp", async () => {
    const d = market([{ lot: 0, effE6: 2_000_000n }, { lot: 6, effE6: 84_250_000n }]);
    expect(await read(d, SYN, 0)).toBe(2);
    expect(await read(d, SYN, 1)).toBeCloseTo(84.25e-6, 18);
  });

  // M06: a v2.2 slab has wrapper magic; the booked effective price must still win over the cfg mark EWMA.
  it("prefers the booked effective_price over the mark EWMA on a wrapper-magic slab", async () => {
    const d = market([{ lot: 0, effE6: 84_250_000n }, { lot: 0 }], 1_500_000n);
    expect(await read(d, SYN, 0)).toBe(84.25);
  });

  // The fallback leg the cases above rely on: no effective price -> EWMA (lot 0).
  it("falls back to the mark EWMA when the asset has no booked effective_price", async () => {
    const d = market([{ lot: 0 }, { lot: 0 }], 1_500_000n);
    expect(await read(d, SYN, 0)).toBe(1.5);
  });

  // M18: an unknown VERSION with a plausible mark at cfg offset 248 must yield 0, never that guessed value.
  it("an unknown market VERSION yields 0 even when bytes at the v17 EWMA offset look like a price", async () => {
    const d = market([{ lot: 0 }, { lot: 0 }], 1_500_000n, 99);
    expect(await read(d, SYN, 0)).toBe(0);
  });
});
