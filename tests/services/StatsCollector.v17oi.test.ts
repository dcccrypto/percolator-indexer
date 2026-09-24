import { describe, it, expect, vi } from 'vitest';

// This suite deliberately does NOT mock '@percolatorct/sdk' — it exercises the
// REAL parseMarketGroupV17OI so the OI wiring is tested end-to-end against the
// actual v18 layout (offset 592 / stride 2325). Only '@percolatorct/shared' and
// the DB writer are stubbed, so importing StatsCollector needs no env / network.
vi.mock('@percolatorct/shared', () => ({
  createLogger: vi.fn(() => ({ info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() })),
  getConnection: vi.fn(() => ({})),
  getSupabase: vi.fn(() => ({})),
  upsertMarketStats: vi.fn(),
  withRetry: vi.fn(async (fn: any) => fn()),
  addBreadcrumb: vi.fn(),
  captureException: vi.fn(),
}));
vi.mock('../../src/db/insertMarketRow.js', () => ({ insertMarketRow: vi.fn() }));

import {
  V17_MAGIC,
  V17_EXPECTED_VERSION,
  V17_KIND_MARKET,
  V17_KIND_OFF,
  V17_MARKET_GROUP_OFF,
  V17_MARKET_GROUP_LEN,
  V17_MARKET_ASSET_SLOT_LEN,
  V17_ASSET_ORACLE_WRAPPER_LEN,
  isV17MarketAccount,
  parseMarketGroupV17OI,
} from '@percolatorct/sdk';
import { parseV17AccountStats } from '../../src/services/StatsCollector.js';

// oi_eff_{long,short}_q offsets within AssetStateV16Account (the first sub-struct
// of each EngineAssetSlotV16Account, which follows the wrapper T in every slot).
// These are internal to the SDK and not exported; they are quoted here only to
// PLANT a known value at the exact byte the SDK reads back, so the assertion that
// `parseV17AccountStats` == `parseMarketGroupV17OI` is meaningful. Verified against
// the SDK's own parseMarketGroupV17OI (V17_ASSET_STATE_OI_LONG_REL = 289).
const OI_LONG_REL = 289;
const OI_SHORT_REL = 305;

/** Build a minimal, well-formed v18 market account buffer with `capacity` slots. */
function makeV18Market(capacity: number): Uint8Array {
  const len = V17_MARKET_GROUP_OFF + V17_MARKET_GROUP_LEN + capacity * V17_MARKET_ASSET_SLOT_LEN;
  const buf = new Uint8Array(len);
  const dv = new DataView(buf.buffer);
  dv.setBigUint64(0, V17_MAGIC, true);          // magic
  dv.setUint16(8, V17_EXPECTED_VERSION, true);  // version = 18
  buf[V17_KIND_OFF] = V17_KIND_MARKET;          // kind = 1 (market)
  return buf;
}

function setU128LE(buf: Uint8Array, off: number, value: bigint): void {
  const dv = new DataView(buf.buffer);
  dv.setBigUint64(off, value & 0xffff_ffff_ffff_ffffn, true);
  dv.setBigUint64(off + 8, value >> 64n, true);
}

function oiEffOffset(slot: number, rel: number): number {
  const slotBase = V17_MARKET_GROUP_OFF + V17_MARKET_GROUP_LEN + slot * V17_MARKET_ASSET_SLOT_LEN;
  return slotBase + V17_ASSET_ORACLE_WRAPPER_LEN + rel;
}

describe('parseV17AccountStats — open interest', () => {
  it('sums per-asset oi_eff across slots (not hard-coded to 0)', () => {
    const buf = makeV18Market(2);
    expect(isV17MarketAccount(buf)).toBe(true);

    // Plant OI: long on slot 0, short on slot 1 — exercises summing across slots.
    const longQ = 12_345_000n;
    const shortQ = 6_780_000n;
    setU128LE(buf, oiEffOffset(0, OI_LONG_REL), longQ);
    setU128LE(buf, oiEffOffset(1, OI_SHORT_REL), shortQ);

    // Precondition: the SDK reads back exactly what we planted (guards against a
    // wrong OI_*_REL making the buffer all-zero and the test vacuous).
    const sdk = parseMarketGroupV17OI(buf);
    expect(sdk.totalLongOiQ).toBe(longQ);
    expect(sdk.totalShortOiQ).toBe(shortQ);

    const { engine } = parseV17AccountStats(buf);
    expect(engine.longOi).toBe(longQ);
    expect(engine.shortOi).toBe(shortQ);
    expect(engine.totalOpenInterest).toBe(longQ + shortQ);
    // The load-bearing assertion: a non-zero total. Before the fix this was 0n
    // regardless of on-chain state.
    expect(engine.totalOpenInterest).toBeGreaterThan(0n);
  });

  it('reports 0 OI for a market with no open positions (all-zero slots)', () => {
    const buf = makeV18Market(3);
    const { engine } = parseV17AccountStats(buf);
    expect(engine.totalOpenInterest).toBe(0n);
    expect(engine.longOi).toBe(0n);
    expect(engine.shortOi).toBe(0n);
  });
});
