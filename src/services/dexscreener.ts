import { createLogger } from "@percolatorct/shared";
import { sanitizeText, sanitizeLogoUrl, MAX_SYMBOL_LEN, MAX_NAME_LEN } from "./tokenMetadata.js";

const logger = createLogger("indexer:dexscreener");

/**
 * Market identity resolved from a mainnet contract address via DexScreener.
 *
 * Why DexScreener rather than DAS: a playground market tracks a MAINNET token
 * while the indexer talks to a devnet RPC, so DAS cannot see the asset at all.
 * DexScreener is chain-wide and keyed by contract address, and it carries a
 * curated image that on-chain metadata often lacks.
 */
export interface DexScreenerIdentity {
  symbol: string | null;
  name: string | null;
  logoUrl: string | null;
}

/** DexScreener accepts up to 30 comma-separated addresses per request. */
export const DEXSCREENER_MAX_BATCH = 30;

const API_BASE = "https://api.dexscreener.com/latest/dex/tokens";

/** Solana base58 — validated before interpolating into the request path. */
const BASE58_ADDRESS = /^[1-9A-HJ-NP-Za-km-z]{32,44}$/;

interface DexPair {
  chainId?: string;
  baseToken?: { address?: string; name?: string; symbol?: string };
  info?: { imageUrl?: string };
  liquidity?: { usd?: number };
}

/**
 * Pick the pair that best identifies `ca` and read the identity off it.
 *
 * Two things matter here:
 *
 *  - The queried token can appear as either side of a pair. Only a pair where it
 *    is the BASE token describes it; taking the first pair blindly would name a
 *    token after whatever it happens to be quoted against.
 *  - A token typically has many pairs of wildly varying quality. The deepest one
 *    is the most likely to carry a real image and a non-spoofed name, so pairs
 *    are ranked by USD liquidity rather than taking whatever the API listed first.
 */
function identityFromPairs(ca: string, pairs: DexPair[]): DexScreenerIdentity | null {
  const asBase = pairs.filter(
    (p) => p?.baseToken?.address === ca && (p.chainId === undefined || p.chainId === "solana"),
  );
  if (asBase.length === 0) return null;

  asBase.sort((a, b) => (b.liquidity?.usd ?? 0) - (a.liquidity?.usd ?? 0));

  // The deepest pair may still lack an image while a shallower one has it, so
  // fall back through the ranked list for the logo specifically.
  const best = asBase[0];
  const logoUrl =
    sanitizeLogoUrl(best.info?.imageUrl) ??
    asBase.map((p) => sanitizeLogoUrl(p.info?.imageUrl)).find((u) => u != null) ??
    null;

  return {
    symbol: sanitizeText(best.baseToken?.symbol, MAX_SYMBOL_LEN),
    name: sanitizeText(best.baseToken?.name, MAX_NAME_LEN),
    logoUrl,
  };
}

/**
 * Resolve identities for up to DEXSCREENER_MAX_BATCH contract addresses in ONE
 * request, returned as a map keyed by the address that resolved.
 *
 * Addresses that DexScreener doesn't know (or that only appear as a quote token)
 * are simply absent from the map — callers treat that as "learned nothing" and
 * keep whatever they already had. Never throws: a resolution failure must not
 * take down the cycle that called it.
 */
export async function resolveIdentitiesByCa(
  contractAddresses: string[],
  timeoutMs = 10_000,
): Promise<Map<string, DexScreenerIdentity>> {
  const out = new Map<string, DexScreenerIdentity>();

  const valid = contractAddresses.filter((ca) => BASE58_ADDRESS.test(ca));
  if (valid.length === 0) return out;
  if (valid.length > DEXSCREENER_MAX_BATCH) {
    // Caller error rather than silent truncation — chunk before calling.
    throw new Error(
      `resolveIdentitiesByCa: ${valid.length} addresses exceeds the ${DEXSCREENER_MAX_BATCH} per-request limit`,
    );
  }

  try {
    const res = await fetch(`${API_BASE}/${valid.join(",")}`, {
      headers: { Accept: "application/json" },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) {
      logger.warn("DexScreener request failed", { status: res.status, count: valid.length });
      return out;
    }

    const json = (await res.json()) as { pairs?: DexPair[] };
    const pairs = Array.isArray(json?.pairs) ? json.pairs : [];

    // One flat pair list covers every requested address, so group by base token.
    for (const ca of valid) {
      const identity = identityFromPairs(ca, pairs);
      if (identity && (identity.symbol || identity.name || identity.logoUrl)) {
        out.set(ca, identity);
      }
    }
  } catch (err) {
    logger.warn("DexScreener resolution error", {
      count: valid.length,
      error: err instanceof Error ? err.message : String(err),
    });
  }

  return out;
}

/** Split a list into DEXSCREENER_MAX_BATCH-sized chunks for resolveIdentitiesByCa. */
export function chunkForDexScreener<T>(items: T[], size = DEXSCREENER_MAX_BATCH): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) chunks.push(items.slice(i, i + size));
  return chunks;
}
