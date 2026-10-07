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

import { fetchTraderPositionEvidence, resolveRebalanceReduce } from '../../src/db/traderNetPosition.js';

const T0 = 1_700_000_000; // tx block time (s)
const EARLY = '2023-11-01T00:00:00Z';
const LATE = '2023-11-20T00:00:00Z';
const row = (side: string | null, size: unknown, extra: Record<string, unknown> = {}) =>
  ({ side, size, is_liquidation: false, created_at: EARLY, ...extra });

describe('fetchTraderPositionEvidence', () => {
  beforeEach(() => { pages.length = 0; calls.length = 0; });

  it('sums long as + and short as -, filtered to the trader/slab/asset/network, excluding the tx itself', async () => {
    pages.push({ data: [
      row('short', 1658210),       // NUMERIC as a JSON number
      row('long', '400000'),       // or as a string
      row('short', '100000.000'),
      row(null, null),             // never counted
    ], error: null });
    expect(await fetchTraderPositionEvidence('T', 'S', 0, 'SIG', T0)).toEqual({ netQ: -1358210n, uncertain: null });
    const eqs = calls.filter(([m]) => m === 'eq').map(([, a]) => a);
    expect(eqs).toEqual(expect.arrayContaining([['trader', 'T'], ['slab_address', 'S'], ['asset_index', 0], ['network', 'devnet']]));
    expect(calls).toContainEqual(['neq', ['tx_signature', 'SIG']]);
  });

  it('pages past 1000 rows', async () => {
    pages.push({ data: Array.from({ length: 1000 }, () => row('long', 1)), error: null });
    pages.push({ data: [row('short', 5)], error: null });
    expect((await fetchTraderPositionEvidence('T', 'S', 0, 'SIG', T0)).netQ).toBe(995n);
  });

  it('throws on a DB error', async () => {
    pages.push({ data: null, error: { message: 'boom' } });
    await expect(fetchTraderPositionEvidence('T', 'S', 0, 'SIG', T0)).rejects.toThrow(/boom/);
  });

  it('is uncertain when the block time is unknown', async () => {
    pages.push({ data: [row('short', 5)], error: null });
    expect((await fetchTraderPositionEvidence('T', 'S', 0, 'SIG', null)).uncertain).toMatch(/block time/);
  });

  it('is uncertain when a liquidation marker exists (a forced close has no recorded size)', async () => {
    pages.push({ data: [row('short', 5), row(null, null, { is_liquidation: true })], error: null });
    expect((await fetchTraderPositionEvidence('T', 'S', 0, 'SIG', T0)).uncertain).toMatch(/liquidation/);
  });

  it('is uncertain when a fill was indexed at/after the tx block time (could be a later fill)', async () => {
    pages.push({ data: [row('short', 5), row('long', 2, { created_at: LATE })], error: null });
    expect((await fetchTraderPositionEvidence('T', 'S', 0, 'SIG', T0)).uncertain).toMatch(/not provably earlier/);
  });
});

describe('resolveRebalanceReduce', () => {
  beforeEach(() => { pages.length = 0; calls.length = 0; });
  const args = { trader: 'T', slabAddress: 'S', assetIndex: 0, reduceQ: 10n, signature: 'SIG', txTimeSec: T0, repeatInTx: false };

  it('resolves side and size from a certain position', async () => {
    pages.push({ data: [row('short', 30)], error: null });
    expect(await resolveRebalanceReduce(args)).toEqual({ ok: true, side: 'long', sizeValue: 10n });
  });

  it('a DB failure is reported as unresolved, not thrown', async () => {
    pages.push({ data: null, error: { message: 'boom' } });
    expect(await resolveRebalanceReduce(args)).toEqual({ ok: false, reason: expect.stringContaining('boom') });
  });

  it('a repeat on the same position in one tx is unresolved without a lookup', async () => {
    expect((await resolveRebalanceReduce({ ...args, repeatInTx: true })).ok).toBe(false);
    expect(calls).toHaveLength(0);
  });
});
