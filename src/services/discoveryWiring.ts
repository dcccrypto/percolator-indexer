import type { MarketDiscovery } from "./MarketDiscovery.js";
import {
  DEFAULT_FULL_DISCOVERY_INTERVAL_MS,
  DEFAULT_LIGHT_DISCOVERY_INTERVAL_MS,
} from "./MarketDiscovery.js";

export interface DiscoveryEnv {
  DISCOVERY_INTERVAL_MS?: string;
  DISCOVERY_LIGHT_INTERVAL_MS?: string;
}

function positiveOr(raw: string | undefined, fallback: number, allowZero = false): number {
  if (raw === undefined || raw.trim() === "") return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0 || (n === 0 && !allowZero)) return fallback;
  return n;
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
