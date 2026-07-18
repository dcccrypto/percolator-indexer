/**
 * Regression tests for #162 — price poisoning via CPI log injection in the
 * price=0 backfill script.
 *
 * The script derived a trade's price by scraping `Program log:` lines and
 * accepting the first integer in [1_000, 1e12] as price_e6. A Percolator
 * transaction can contain inner CPIs to arbitrary third-party programs, any of
 * which can emit a log line, so an attacker could set the recorded price of
 * their own trades. The same scraper was already neutralised in the live paths
 * (webhook.ts, TradeIndexer.ts) by #150; this script was missed.
 *
 * These tests pin the property that log content can never influence a price.
 */

import { describe, it, expect, vi } from "vitest";

vi.mock("@percolatorct/shared", () => ({
  getSupabase: () => ({}),
  getNetwork: () => "devnet",
  config: {},
}));

vi.mock("@percolatorct/sdk", () => ({
  // Length-keyed layout stub: only the length we build below is "known".
  detectSlabLayout: (len: number) =>
    len === 2048 ? { engineOff: 640, engineMarkPriceOff: 400, configMarkEwmaOff: null } : null,
}));

const { extractPrice, extractPriceFromLogs } = await import(
  "../../src/scripts/backfill-price-zero-trades.js"
);

const SLAB = "SlabAddress1111111111111111111111111111111";

/** A log line an attacker's inner CPI could emit — shape the old scraper accepted. */
const POISON_LOGS = [
  "Program log: 123456789, 0",
  "Program log: 0x75bcd15, 0x0",
  "Program log: 999999999999, 1",
];

/** Build a slab account blob carrying a real mark_price_e6 at ENGINE_OFF+400. */
function slabTx(markPriceE6: bigint, logs: string[] = []) {
  const raw = new Uint8Array(2048);
  const dv = new DataView(raw.buffer);
  dv.setBigUint64(640 + 400, markPriceE6, true);
  return {
    accountData: [{ account: SLAB, data: Buffer.from(raw).toString("base64") }],
    logs,
  };
}

describe("#162 backfill price extraction ignores program logs", () => {
  it("returns 0 for attacker-shaped log lines with no slab post-state", () => {
    // Previously each of these yielded an attacker-chosen price.
    for (const log of POISON_LOGS) {
      expect(extractPrice({ accountData: [], logs: [log] }, SLAB)).toBe(0);
    }
  });

  it("extractPriceFromLogs is inert regardless of log content", () => {
    expect(extractPriceFromLogs({ logs: POISON_LOGS })).toBe(0);
    expect(extractPriceFromLogs({ logMessages: POISON_LOGS })).toBe(0);
    expect(extractPriceFromLogs({})).toBe(0);
  });

  it("does not let logs override a real slab-derived price", () => {
    // Slab says $84.00; attacker logs claim $123.456789.
    const tx = slabTx(84_000_000n, POISON_LOGS);
    expect(extractPrice(tx, SLAB)).toBe(84);
  });

  it("still recovers the price from slab post-state (the trusted path)", () => {
    expect(extractPrice(slabTx(1_500_000n), SLAB)).toBe(1.5);
    expect(extractPrice(slabTx(84_120_000n), SLAB)).toBe(84.12);
  });

  it("leaves the row at 0 when the slab post-state is absent", () => {
    // The correct outcome: unfilled, rather than filled from an untrusted source.
    expect(extractPrice({ accountData: [], logs: POISON_LOGS }, SLAB)).toBe(0);
  });

  it("ignores account data for a different slab", () => {
    const tx = slabTx(84_000_000n, POISON_LOGS);
    expect(extractPrice(tx, "OtherSlab111111111111111111111111111111111")).toBe(0);
  });
});
