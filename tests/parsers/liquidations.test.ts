/**
 * parseLiquidation — v18 has NO instruction-level liquidation signal.
 *
 * v18 PermissionlessCrank (tag 5) wire is `tag + nowSlot(u64) + n_obs(u8) + obs[]`
 * with NO action byte, tags 7/101 are removed, and the programs emit no liquidation
 * log. So parseLiquidation always returns null. These tests pin that — especially
 * the regression case: a crank whose nowSlot low byte is 0x01 (which the old v17
 * decoder mistook for `action==Liquidate`) must NOT be flagged.
 */
import { describe, it, expect, vi } from 'vitest';

vi.mock('@percolatorct/sdk', () => ({ IX_TAG: { PermissionlessCrank: 5 } }));

import { parseLiquidation } from '../../src/parsers/liquidations.js';

const OWNER = 'Owner1111111111111111111111111111111111111';
const MARKET = 'Market111111111111111111111111111111111111';
const PORTFOLIO = 'Portfo111111111111111111111111111111111111';
const accts = [OWNER, MARKET, PORTFOLIO];

// v18 crank: tag(5) + nowSlot(u64 LE) + n_obs(u8) + [assetIndex(u16)+oracleAccounts(u8)]*n
const crankV18 = (nowSlot: bigint, obs: Array<[number, number]> = []) => {
  const buf = [5];
  for (let i = 0; i < 8; i++) buf.push(Number((nowSlot >> BigInt(8 * i)) & 0xffn));
  buf.push(obs.length);
  for (const [ai, oa] of obs) buf.push(ai & 0xff, (ai >> 8) & 0xff, oa & 0xff);
  return new Uint8Array(buf);
};

describe('parseLiquidation (v18: no instruction-level liquidation)', () => {
  it('returns null for a normal v18 crank', () => {
    expect(parseLiquidation(5, crankV18(490_000_000n, [[3, 1]]), accts)).toBeNull();
  });

  it('REGRESSION: a crank whose nowSlot low byte is 0x01 is NOT a liquidation', () => {
    // nowSlot = ...0x01 — data[1] === 1, exactly what the v17 decoder false-flagged.
    expect(parseLiquidation(5, crankV18(0x1201n /* low byte 0x01 */), accts)).toBeNull();
    expect(parseLiquidation(5, crankV18(1n), accts)).toBeNull();
  });

  it('returns null for non-crank tags (a Trade tag)', () => {
    expect(parseLiquidation(6, crankV18(1n), accts)).toBeNull();
  });

  it('returns null regardless of accounts present', () => {
    expect(parseLiquidation(5, crankV18(1n), [OWNER])).toBeNull();
    expect(parseLiquidation(5, crankV18(1n), accts)).toBeNull();
  });

  it('returns null on a truncated crank', () => {
    expect(parseLiquidation(5, new Uint8Array([5, 1]), accts)).toBeNull();
  });
});
