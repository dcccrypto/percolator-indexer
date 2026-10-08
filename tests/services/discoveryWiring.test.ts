import { describe, it, expect, vi } from 'vitest';
import { readFileSync } from 'node:fs';
import { resolveDiscoveryIntervals, startDiscovery } from '../../src/services/discoveryWiring.js';

describe('discovery wiring (#223)', () => {
  it('defaults: full pass every 5 min, light pass every minute', () => {
    expect(resolveDiscoveryIntervals({})).toEqual({ fullMs: 300_000, lightMs: 60_000 });
  });

  it('DISCOVERY_INTERVAL_MS overrides only the full pass; DISCOVERY_LIGHT_INTERVAL_MS only the light one', () => {
    expect(resolveDiscoveryIntervals({ DISCOVERY_INTERVAL_MS: '120000' })).toEqual({ fullMs: 120_000, lightMs: 60_000 });
    expect(resolveDiscoveryIntervals({ DISCOVERY_LIGHT_INTERVAL_MS: '30000' })).toEqual({ fullMs: 300_000, lightMs: 30_000 });
    expect(resolveDiscoveryIntervals({ DISCOVERY_LIGHT_INTERVAL_MS: '0' }).lightMs).toBe(0); // disabled
  });

  it('garbage values fall back to the defaults', () => {
    expect(resolveDiscoveryIntervals({ DISCOVERY_INTERVAL_MS: 'abc', DISCOVERY_LIGHT_INTERVAL_MS: '-5' }))
      .toEqual({ fullMs: 300_000, lightMs: 60_000 });
    expect(resolveDiscoveryIntervals({ DISCOVERY_INTERVAL_MS: '0' }).fullMs).toBe(300_000);
  });

  it('hooks registration to onDiscovered and starts discovery with both intervals', async () => {
    let hook: (() => unknown) | undefined;
    const order: string[] = [];
    const discovery = {
      onDiscovered: vi.fn((fn: () => unknown) => { hook = fn; order.push('hook'); return () => {}; }),
      start: vi.fn(async () => { order.push('start'); }),
    };
    const registrar = { registerNewMarkets: vi.fn(async () => {}) };
    await startDiscovery(discovery as never, registrar, {});
    expect(order).toEqual(['hook', 'start']); // hook is in place before the first discovery pass
    expect(discovery.start).toHaveBeenCalledWith(300_000, 60_000);
    hook!();
    expect(registrar.registerNewMarkets).toHaveBeenCalledTimes(1);
  });

  it('src/index.ts starts discovery through startDiscovery (no direct discovery.start left)', () => {
    const src = readFileSync(new URL('../../src/index.ts', import.meta.url), 'utf8');
    expect(src).toMatch(/startDiscovery\(discovery, statsCollector, process\.env\)/);
    expect(src).not.toMatch(/discovery\.start\(/);
  });
});
