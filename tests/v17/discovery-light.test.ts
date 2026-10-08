import { describe, it, expect, vi } from 'vitest';
import { PublicKey } from '@solana/web3.js';
import { discoverV17Markets, V17_REGISTRATION_SLICE_LEN } from '../../src/v17/discovery.js';

describe('discoverV17Markets light option (#223)', () => {
  const pk = new PublicKey('11111111111111111111111111111111');
  const conn = () => ({ getProgramAccounts: vi.fn().mockResolvedValue([]) });

  it('light: one KIND_MARKET-filtered scan with a header-only dataSlice', async () => {
    const c = conn();
    await discoverV17Markets(c as never, pk, undefined, { light: true });
    expect(c.getProgramAccounts).toHaveBeenCalledTimes(1);
    const opts = c.getProgramAccounts.mock.calls[0][1];
    expect(opts.dataSlice).toEqual({ offset: 0, length: V17_REGISTRATION_SLICE_LEN });
    expect(opts.filters.map((f: any) => f.memcmp.offset)).toEqual([0, 10]);
    expect(V17_REGISTRATION_SLICE_LEN).toBeLessThan(4096);
  });

  it('default (full) scan stays unsliced', async () => {
    const c = conn();
    await discoverV17Markets(c as never, pk);
    expect(c.getProgramAccounts.mock.calls[0][1].dataSlice).toBeUndefined();
  });
});
