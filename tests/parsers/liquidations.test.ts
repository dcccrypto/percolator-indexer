/**
 * parseLiquidation — v17 liquidations run through PermissionlessCrank (tag 5) with
 * action byte 1. Data: tag(1)+action(1)+asset_index(u16)+... ; accounts [1]=market
 * [2]=portfolio. Only action==1 is a liquidation.
 */
import { describe, it, expect, vi } from 'vitest';

vi.mock('@percolatorct/sdk', () => ({ IX_TAG: { PermissionlessCrank: 5 } }));

import { parseLiquidation } from '../../src/parsers/liquidations.js';

const OWNER = 'Owner1111111111111111111111111111111111111';
const MARKET = 'Market111111111111111111111111111111111111';
const PORTFOLIO = 'Portfo111111111111111111111111111111111111';
const accts = [OWNER, MARKET, PORTFOLIO];

// tag(5) action(a) asset_index(u16 LE = idx)
const crank = (action: number, idx = 3) => new Uint8Array([5, action, idx & 0xff, (idx >> 8) & 0xff, 0, 0, 0, 0]);

describe('parseLiquidation', () => {
  it('returns a marker for a crank with action 1 (Liquidate)', () => {
    const m = parseLiquidation(5, crank(1, 3), accts);
    expect(m).toEqual({ slabAddress: MARKET, portfolio: PORTFOLIO, assetIndex: 3 });
  });

  it('reads a multi-byte asset_index (u16 LE)', () => {
    const m = parseLiquidation(5, crank(1, 258), accts); // 258 = 0x0102
    expect(m?.assetIndex).toBe(258);
  });

  it('returns null for action 0 (Refresh) and 2 (SettleB)', () => {
    expect(parseLiquidation(5, crank(0), accts)).toBeNull();
    expect(parseLiquidation(5, crank(2), accts)).toBeNull();
  });

  it('returns null for non-crank tags (a Trade tag)', () => {
    expect(parseLiquidation(6, crank(1), accts)).toBeNull();
  });

  it('returns null when market/portfolio accounts are missing', () => {
    expect(parseLiquidation(5, crank(1), [OWNER])).toBeNull();
    expect(parseLiquidation(5, crank(1), [OWNER, MARKET])).toBeNull();
  });

  it('returns null on a truncated crank (no asset_index bytes)', () => {
    expect(parseLiquidation(5, new Uint8Array([5, 1]), accts)).toBeNull();
  });
});
