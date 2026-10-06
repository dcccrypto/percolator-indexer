import { describe, it, expect, vi, beforeEach } from 'vitest';

const pages: Array<{ data: unknown[] | null; error: { message: string } | null }> = [];
const calls: Array<[string, unknown[]]> = [];

function chain() {
  const q: any = {};
  for (const m of ['from', 'select', 'eq', 'neq', 'order']) {
    q[m] = (...args: unknown[]) => { calls.push([m, args]); return q; };
  }
  q.range = (...args: unknown[]) => { calls.push(['range', args]); return Promise.resolve(pages.shift() ?? { data: [], error: null }); };
  return q;
}

vi.mock('@percolatorct/shared', () => ({
  getSupabase: vi.fn(() => chain()),
  getNetwork: vi.fn(() => 'devnet'),
}));

import { fetchTraderNetPositionQ } from '../../src/db/traderNetPosition.js';

describe('fetchTraderNetPositionQ', () => {
  beforeEach(() => { pages.length = 0; calls.length = 0; });

  it('sums long as + and short as -, filtered to the trader/slab/asset/network, excluding the tx itself', async () => {
    pages.push({ data: [
      { side: 'short', size: 1658210 },      // NUMERIC as a JSON number
      { side: 'long', size: '400000' },     // or as a string
      { side: 'short', size: '100000.000' },
      { side: null, size: null },            // never counted
    ], error: null });
    expect(await fetchTraderNetPositionQ('T', 'S', 0, 'SIG')).toBe(-1358210n);
    const eqs = calls.filter(([m]) => m === 'eq').map(([, a]) => a);
    expect(eqs).toEqual(expect.arrayContaining([
      ['trader', 'T'], ['slab_address', 'S'], ['asset_index', 0], ['network', 'devnet'], ['is_liquidation', false],
    ]));
    expect(calls).toContainEqual(['neq', ['tx_signature', 'SIG']]);
  });

  it('pages past 1000 rows', async () => {
    pages.push({ data: Array.from({ length: 1000 }, () => ({ side: 'long', size: 1 })), error: null });
    pages.push({ data: [{ side: 'short', size: 5 }], error: null });
    expect(await fetchTraderNetPositionQ('T', 'S', 0, 'SIG')).toBe(995n);
  });

  it('throws on a DB error', async () => {
    pages.push({ data: null, error: { message: 'boom' } });
    await expect(fetchTraderNetPositionQ('T', 'S', 0, 'SIG')).rejects.toThrow(/boom/);
  });
});
