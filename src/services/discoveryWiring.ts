import type { MarketDiscovery } from "./MarketDiscovery.js";
import {
  DEFAULT_FULL_DISCOVERY_INTERVAL_MS,
  DEFAULT_LIGHT_DISCOVERY_INTERVAL_MS,
} from "./MarketDiscovery.js";

export interface DiscoveryEnv {
  DISCOVERY_INTERVAL_MS?: string;
  DISCOVERY_LIGHT_INTERVAL_MS?: string;
}

const MIN_INTERVAL_MS = 1_000;
const MAX_INTERVAL_MS = 2 ** 31 - 1; // setInterval overflows (fires after 1 ms) above this

/** Decimal digits only; sub-minimum values floor to 1 s, huge ones cap at 2^31-1. */
function positiveOr(raw: string | undefined, fallback: number, allowZero = false): number {
  const t = raw?.trim();
  if (t === undefined || !/^\d+$/.test(t)) return fallback;
  const n = Number(t);
  if (n === 0) return allowZero ? 0 : fallback;
  return Math.min(Math.max(n, MIN_INTERVAL_MS), MAX_INTERVAL_MS);
}

/**
 * #223: the discovery cadences. The full multi-tier pass keeps its 5-minute default
 * (DISCOVERY_INTERVAL_MS overrides it); the light pass runs every minute
 * (DISCOVERY_LIGHT_INTERVAL_MS overrides it, 0 disables it).
 */
export function resolveDiscoveryIntervals(env: DiscoveryEnv): { fullMs: number; lightMs: number } {
  return {
    fullMs: positiveOr(env.DISCOVERY_INTERVAL_MS, DEFAULT_FULL_DISCOVERY_INTERVAL_MS),
    lightMs: positiveOr(env.DISCOVERY_LIGHT_INTERVAL_MS, DEFAULT_LIGHT_DISCOVERY_INTERVAL_MS, true),
  };
}

/** #223: register newly discovered markets right away, then start both discovery timers. */
export async function startDiscovery(
  discovery: Pick<MarketDiscovery, "onDiscovered" | "start">,
  registrar: { registerNewMarkets(): Promise<void> },
  env: DiscoveryEnv,
): Promise<void> {
  discovery.onDiscovered(() => registrar.registerNewMarkets());
  const { fullMs, lightMs } = resolveDiscoveryIntervals(env);
  await discovery.start(fullMs, lightMs);
}
