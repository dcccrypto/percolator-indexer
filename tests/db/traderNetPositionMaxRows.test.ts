import { describe, it, expect, vi, beforeEach } from 'vitest';

// In-memory trades table; .range() honours a PostgREST-style max_rows cap.
let table: Array<{ side: string; size: number; is_liquidation: boolean; created_at: string }> = [];
let cap = Infinity;
const ranges: Array<[number, number]> = [];

function chain() {
  const q: any = {};
  for (const m of ['from', 'select', 'eq', 'neq', 'order']) q[m] = () => q;
  q.range = (from: number, to: number) => {
    ranges.push([from, to]);
    const end = Math.min(to + 1, from + cap);
    return Promise.resolve({ data: table.slice(from, end), error: null });
  };
  return q;
}

vi.mock('@percolatorct/shared', () => ({
  getSupabase: vi.fn(() => chain()),
  getNetwork: vi.fn(() => 'devnet'),
}));

import { fetchTraderPositionEvidence } from '../../src/db/traderNetPosition.js';

const T0 = 1_700_000_000;
const mk = (n: number) =>
  Array.from({ length: n }, (_, i) => ({
    side: i % 3 === 0 ? 'short' : 'long',
    size: i + 1,
    is_liquidation: false,
    created_at: '2023-11-01T00:00:00Z',
  }));
const expected = (rows: ReturnType<typeof mk>) =>
  rows.reduce((a, r) => a + (r.side === 'long' ? BigInt(r.size) : -BigInt(r.size)), 0n);

describe('fetchTraderPositionEvidence with a PostgREST max_rows cap below PAGE', () => {
  beforeEach(() => { ranges.length = 0; cap = Infinity; });

  it('sums every fill when the server caps pages at 400 rows (3 capped pages)', async () => {
    table = mk(1000);
    cap = 400;
    const r = await fetchTraderPositionEvidence('T', 'S', 0, 'SIG', T0);
    expect(r.netQ).toBe(expected(table));
    expect(r.uncertain).toBeNull();
  });

  it('is unchanged when the cap is >= PAGE (same sum; pages 0-999, 1000-1999, then an empty probe)', async () => {
    table = mk(1500);
    cap = 1000;
    const r = await fetchTraderPositionEvidence('T', 'S', 0, 'SIG', T0);
    expect(r.netQ).toBe(expected(table));
    expect(ranges).toEqual([[0, 999], [1000, 1999], [1500, 2499]]);
  });
});
