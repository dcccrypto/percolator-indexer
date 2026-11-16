import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Branch-coverage companion to MarketDiscovery.test.ts. Own mock factory so sendCriticalAlert
// and captureException are observable and getPrimaryConnection can be made to throw.
vi.mock('../../src/v17/discovery.js', () => ({
  discoverV17Markets: vi.fn(async () => []),
}));

vi.mock('@percolatorct/sdk', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@percolatorct/sdk')>()),
  discoverMarkets: vi.fn(),
  getMarketsByAddress: vi.fn(),
  isV17Account: vi.fn(() => false),
}));

const { primary, fallback, loggerMock } = vi.hoisted(() => ({
  primary: { name: 'primary' },
  fallback: { name: 'fallback' },
  loggerMock: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() },
}));

vi.mock('@percolatorct/shared', () => ({
  config: {
    allProgramIds: ['11111111111111111111111111111111', 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'],
  },
  createLogger: vi.fn(() => loggerMock),
  captureException: vi.fn(),
  sendCriticalAlert: vi.fn(async () => {}),
  getPrimaryConnection: vi.fn(() => primary),
  getFallbackConnection: vi.fn(() => fallback),
}));

import { MarketDiscovery } from '../../src/services/MarketDiscovery.js';
import * as core from '@percolatorct/sdk';
import * as shared from '@percolatorct/shared';
import * as v17disc from '../../src/v17/discovery.js';

const P1 = '11111111111111111111111111111111';
const P2 = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const mk = (addr: string, pid = P1) => ({
  slabAddress: { toBase58: () => addr, equals: () => false },
  programId: { toBase58: () => pid },
  config: {}, params: {}, header: {},
});
const A = mk('MarketAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA');
const B = mk('MarketBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB', P2);

async function drive<T>(p: Promise<T>): Promise<T> {
  let done = false;
  const w = p.finally(() => { done = true; });
  w.catch(() => {});
  for (let i = 0; !done; i++) {
    if (i > 900) throw new Error('drive(): still pending');
    await vi.advanceTimersByTimeAsync(1_000);
  }
  return w;
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
  delete process.env.MARKETS_FILTER;
  vi.mocked(v17disc.discoverV17Markets).mockReset();
  vi.mocked(v17disc.discoverV17Markets).mockResolvedValue([] as any);
  vi.mocked(core.discoverMarkets).mockReset();
  vi.mocked(core.getMarketsByAddress).mockReset();
  vi.mocked(shared.getPrimaryConnection).mockReturnValue(primary as any);
  vi.mocked(shared.sendCriticalAlert).mockResolvedValue(undefined as any);
});
afterEach(() => {
  delete process.env.MARKETS_FILTER;
  vi.useRealTimers();
});

describe('overlap guards', () => {
  it('discover() returns [] while another pass is in flight', async () => {
    vi.mocked(core.discoverMarkets).mockResolvedValue([A] as any);
    const d = new MarketDiscovery();
    const first = d.discover();
    expect(await d.discover()).toEqual([]);
    await drive(first);
    expect(d.getMarkets().size).toBe(1);
  });

  it('discoverLight() returns [] while a full pass is in flight', async () => {
    vi.mocked(core.discoverMarkets).mockResolvedValue([A] as any);
    const d = new MarketDiscovery();
    const first = d.discover();
    expect(await d.discoverLight()).toEqual([]);
    await drive(first);
  });
});

describe('total failure and critical alert', () => {
  it('counts consecutive failures, alerts from the 3rd, and keeps stale markets', async () => {
    vi.mocked(core.discoverMarkets).mockResolvedValue([A] as any);
    const d = new MarketDiscovery();
    await drive(d.discover());
    expect(d.getMarkets().size).toBe(1);

    vi.mocked(core.discoverMarkets).mockRejectedValue(new Error('rpc down'));
    expect(await drive(d.discover())).toEqual([]);
    expect(await drive(d.discover())).toEqual([]);
    expect(shared.sendCriticalAlert).not.toHaveBeenCalled();
    expect(await drive(d.discover())).toEqual([]);
    expect(shared.sendCriticalAlert).toHaveBeenCalledTimes(1);
    expect(shared.captureException).toHaveBeenCalledTimes(3);
    expect(d.getMarkets().size).toBe(1);
  });

  it('a rejecting critical alert is logged, not thrown', async () => {
    vi.mocked(core.discoverMarkets).mockRejectedValue(new Error('rpc down'));
    vi.mocked(shared.sendCriticalAlert).mockRejectedValue(new Error('discord down'));
    const d = new MarketDiscovery();
    for (let i = 0; i < 3; i++) await drive(d.discover());
    await vi.advanceTimersByTimeAsync(0);
    expect(loggerMock.error).toHaveBeenCalledWith('Failed to send discovery alert', expect.anything());
  });

  it('a success resets the consecutive-failure counter', async () => {
    const d = new MarketDiscovery();
    vi.mocked(core.discoverMarkets).mockRejectedValue(new Error('x'));
    await drive(d.discover());
    await drive(d.discover());
    vi.mocked(core.discoverMarkets).mockResolvedValue([A] as any);
    await drive(d.discover());
    vi.mocked(core.discoverMarkets).mockRejectedValue(new Error('x'));
    await drive(d.discover());
    await drive(d.discover());
    expect(shared.sendCriticalAlert).not.toHaveBeenCalled();
  });
});

describe('rate-limit classification and retry', () => {
  it.each([
    ['rate limit text', new Error('Rate Limit exceeded')],
    ['too many requests text', new Error('Too Many Requests')],
    ['429 in a string', '429 from upstream'],
  ])('retries on primary for %s', async (_n, err) => {
    vi.mocked(core.discoverMarkets)
      .mockRejectedValueOnce(err as any)
      .mockResolvedValue([A] as any);
    const d = new MarketDiscovery();
    const res = await drive(d.discover());
    expect(res.length).toBeGreaterThan(0);
    expect(loggerMock.warn).toHaveBeenCalledWith(
      expect.stringContaining('429'),
      expect.objectContaining({ conn: 'primary', attempt: 1 }),
    );
  });

  it('does not retry a falsy rejection (isRateLimitError(undefined) is false)', async () => {
    vi.mocked(core.discoverMarkets).mockRejectedValue(undefined);
    const d = new MarketDiscovery();
    await drive(d.discover());
    expect(core.discoverMarkets).toHaveBeenCalledTimes(2); // one attempt per program
  });

  it('succeeding on the fallback RPC after primary 429s logs the fallback success', async () => {
    vi.mocked(core.discoverMarkets).mockImplementation(async (conn: any) => {
      if (conn === fallback) return [A] as any;
      throw new Error('429 Too Many Requests');
    });
    const d = new MarketDiscovery();
    const res = await drive(d.discover());
    expect(res.length).toBe(2);
    expect(loggerMock.warn).toHaveBeenCalledWith(
      'discoverMarkets succeeded on fallback RPC after primary 429s',
      expect.anything(),
    );
  });

  it('gives up when the fallback also 429s', async () => {
    vi.mocked(core.discoverMarkets).mockRejectedValue(new Error('429'));
    const d = new MarketDiscovery();
    expect(await drive(d.discover())).toEqual([]);
    expect(loggerMock.warn).toHaveBeenCalledWith(
      'Failed to discover on program',
      expect.objectContaining({ conn: 'fallback' }),
    );
  });
});

describe('0 markets despite successful scans', () => {
  it('warns and leaves the map untouched', async () => {
    vi.mocked(core.discoverMarkets).mockResolvedValueOnce([A] as any).mockResolvedValueOnce([] as any);
    const d = new MarketDiscovery();
    await drive(d.discover());
    vi.mocked(core.discoverMarkets).mockResolvedValue([] as any);
    expect(await drive(d.discover())).toEqual([]);
    expect(loggerMock.warn).toHaveBeenCalledWith('Discovery succeeded but found 0 markets', expect.anything());
    expect(d.getMarkets().size).toBe(1);
  });
});

describe('MARKETS_FILTER full pass', () => {
  beforeEach(() => { process.env.MARKETS_FILTER = ` ${P1}, ,${P2} `; });

  it('merges v17 and v12 results and swaps the map', async () => {
    vi.mocked(v17disc.discoverV17Markets).mockResolvedValue([A] as any);
    vi.mocked(core.getMarketsByAddress).mockResolvedValue([B] as any);
    const d = new MarketDiscovery();
    const res = await drive(d.discover());
    expect(res.length).toBe(4);
    expect(d.getMarkets().size).toBe(2);
    expect(v17disc.discoverV17Markets).toHaveBeenCalledWith(primary, expect.anything(), expect.any(Array));
  });

  it('a failing program is counted and its known markets are carried over', async () => {
    vi.mocked(core.getMarketsByAddress).mockImplementation(async (_c: any, pid: any) => (pid.toBase58() === P1 ? [A] : [B]) as any);
    const d = new MarketDiscovery();
    await drive(d.discover());
    expect(d.getMarkets().size).toBe(2);

    vi.mocked(core.getMarketsByAddress).mockImplementation(async (_c: any, pid: any) => {
      if (pid.toBase58() === P2) throw new Error('boom');
      return [A] as any;
    });
    await drive(d.discover());
    expect(d.getMarkets().size).toBe(2);
    expect(loggerMock.warn).toHaveBeenCalledWith('MARKETS_FILTER discovery failed on program', expect.objectContaining({ error: 'boom' }));
    expect(loggerMock.warn).toHaveBeenCalledWith('Kept known markets of programs whose scan failed this pass', expect.objectContaining({ kept: 1 }));
    expect(loggerMock.info).toHaveBeenCalledWith('Market discovery complete', expect.objectContaining({ failedPrograms: 1 }));
  });

  it('records a non-Error failure by string and keeps the map when nothing is found', async () => {
    vi.mocked(core.getMarketsByAddress).mockRejectedValue('weird');
    const d = new MarketDiscovery();
    expect(await drive(d.discover())).toEqual([]);
    expect(loggerMock.warn).toHaveBeenCalledWith('MARKETS_FILTER discovery failed on program', expect.objectContaining({ error: 'weird' }));
    expect(loggerMock.warn).toHaveBeenCalledWith('MARKETS_FILTER discovery found 0 markets', expect.objectContaining({ failedPrograms: 2 }));
  });
});

describe('discoverLight', () => {
  it('honours MARKETS_FILTER and de-duplicates a market returned by two programs', async () => {
    process.env.MARKETS_FILTER = `${P1}, ${P2}`;
    vi.mocked(v17disc.discoverV17Markets).mockResolvedValue([A] as any);
    const d = new MarketDiscovery();
    const fresh = await d.discoverLight();
    expect(fresh.length).toBe(1);
    expect(d.getMarkets().size).toBe(1);
    expect(v17disc.discoverV17Markets).toHaveBeenCalledWith(primary, expect.anything(), expect.any(Array), { light: true });
  });

  it('logs a non-Error scan failure as a string and continues', async () => {
    vi.mocked(v17disc.discoverV17Markets).mockRejectedValueOnce('nope').mockResolvedValueOnce([B] as any);
    const d = new MarketDiscovery();
    const fresh = await d.discoverLight();
    expect(fresh.length).toBe(1);
    expect(loggerMock.warn).toHaveBeenCalledWith('Light discovery failed on program', expect.objectContaining({ error: 'nope' }));
  });

  it('releases the in-flight flag when the connection lookup throws', async () => {
    vi.mocked(shared.getPrimaryConnection).mockImplementationOnce(() => { throw new Error('no conn'); });
    const d = new MarketDiscovery();
    await expect(d.discoverLight()).rejects.toThrow('no conn');
    vi.mocked(v17disc.discoverV17Markets).mockResolvedValue([A] as any);
    expect((await d.discoverLight()).length).toBe(1);
  });
});

describe('listener failures', () => {
  it('logs a non-Error listener rejection as a string', async () => {
    vi.mocked(core.discoverMarkets).mockResolvedValue([A] as any);
    const d = new MarketDiscovery();
    d.onDiscovered(() => Promise.reject('listener-str'));
    await drive(d.discover());
    await vi.advanceTimersByTimeAsync(0);
    expect(loggerMock.warn).toHaveBeenCalledWith('onDiscovered listener failed', { error: 'listener-str' });
  });
});

describe('start() retry and timers', () => {
  it('retries a throwing initial discovery, reports it, then succeeds', async () => {
    vi.mocked(core.discoverMarkets).mockResolvedValue([A] as any);
    vi.mocked(shared.getPrimaryConnection).mockImplementationOnce(() => { throw new Error('conn boom'); });
    const d = new MarketDiscovery();
    await drive(d.start(300_000, 0));
    expect(shared.captureException).toHaveBeenCalledWith(
      expect.any(Error),
      { tags: { context: 'market-discovery-initial', attempt: '1' } },
    );
    expect(d.getMarkets().size).toBe(1);
    expect(loggerMock.error).not.toHaveBeenCalledWith('Initial market discovery exhausted all retries — will continue with periodic polling');
    d.stop();
  });

  it('exhausts every initial retry on persistent exceptions and still arms the timer', async () => {
    vi.mocked(shared.getPrimaryConnection).mockImplementation(() => { throw new Error('conn boom'); });
    const d = new MarketDiscovery();
    await drive(d.start(300_000, 0));
    expect(shared.captureException).toHaveBeenCalledTimes(5);
    expect(loggerMock.error).toHaveBeenCalledWith('Initial market discovery exhausted all retries — will continue with periodic polling');

    // Periodic timer: a throwing pass is reported, not unhandled.
    await vi.advanceTimersByTimeAsync(300_000);
    expect(shared.captureException).toHaveBeenCalledWith(expect.any(Error), { tags: { context: 'market-discovery-periodic' } });
    d.stop();
  });

  it('exhausts every initial retry when each pass finds 0 markets', async () => {
    vi.mocked(core.discoverMarkets).mockResolvedValue([] as any);
    const d = new MarketDiscovery();
    await drive(d.start(300_000, 0));
    expect(loggerMock.error).toHaveBeenCalledWith('Initial market discovery exhausted all retries — will continue with periodic polling');
    d.stop();
  });

  it('light timer: a throwing light pass is logged as a warning; lightInterval >= interval arms nothing', async () => {
    vi.mocked(core.discoverMarkets).mockResolvedValue([A] as any);
    const d = new MarketDiscovery();
    await drive(d.start(300_000, 60_000));
    vi.mocked(shared.getPrimaryConnection).mockImplementation(() => { throw new Error('light boom'); });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(loggerMock.warn).toHaveBeenCalledWith('Light discovery failed', { error: 'light boom' });
    d.stop();

    const d2 = new MarketDiscovery();
    vi.mocked(shared.getPrimaryConnection).mockReturnValue(primary as any);
    await drive(d2.start(60_000, 60_000));
    const calls = vi.mocked(v17disc.discoverV17Markets).mock.calls.filter((c) => (c as any)[3]?.light).length;
    await vi.advanceTimersByTimeAsync(61_000);
    expect(vi.mocked(v17disc.discoverV17Markets).mock.calls.filter((c) => (c as any)[3]?.light).length).toBe(calls);
    d2.stop();
  });
});
