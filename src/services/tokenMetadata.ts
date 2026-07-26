import { createLogger } from "@percolatorct/shared";

const logger = createLogger("indexer:token-metadata");

/**
 * Token identity resolved from Helius DAS.
 *
 * Every field is independently optional: DAS routinely returns a symbol with no
 * image, or an image with no name. Callers merge whatever came back over their
 * existing values rather than replacing wholesale.
 */
export interface DasTokenMetadata {
  symbol: string | null;
  name: string | null;
  decimals: number | null;
  logoUrl: string | null;
}

/** Max lengths mirror the markets table columns (symbol TEXT/32, name TEXT/128). */
export const MAX_SYMBOL_LEN = 32;
export const MAX_NAME_LEN = 128;
const MAX_LOGO_URL_LEN = 512;

/**
 * Strip control characters and angle brackets from external metadata.
 *
 * Defense in depth: this lands in the DB and is rendered by the frontend, so a
 * token whose on-chain name contains markup must not survive the trip. The
 * frontend escapes too — this stops the value being stored in the first place.
 */
export function sanitizeText(value: unknown, maxLen: number): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.replace(/[\x00-\x1f\x7f<>]/g, "").trim().slice(0, maxLen);
  return cleaned.length > 0 ? cleaned : null;
}

/**
 * Accept only http(s) image URLs.
 *
 * Rejects `data:` (can carry inline SVG with script), `javascript:`, IPFS/AR
 * protocol URLs the browser cannot load directly, and anything unparseable.
 * Mirrors the frontend's sanitizeLogoUrl allowlist so a URL that passes here
 * will also pass there.
 */
export function sanitizeLogoUrl(value: unknown): string | null {
  if (typeof value !== "string" || value.length === 0 || value.length > MAX_LOGO_URL_LEN) {
    return null;
  }
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    return null;
  }
  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
  return parsed.toString().slice(0, MAX_LOGO_URL_LEN);
}

/**
 * Pull the best available image URL out of a DAS asset.
 *
 * DAS exposes images in two places and neither is guaranteed:
 *   content.links.image        — the canonical CDN link, present for most tokens
 *   content.files[].uri/cdn_uri — the raw file list; cdn_uri is preferred when
 *                                 present because the plain uri is often IPFS
 *
 * Tries them in that order and returns the first that survives sanitization.
 */
function extractLogoUrl(content: any): string | null {
  const direct = sanitizeLogoUrl(content?.links?.image);
  if (direct) return direct;

  const files = Array.isArray(content?.files) ? content.files : [];
  for (const file of files) {
    const mime = typeof file?.mime === "string" ? file.mime : "";
    // Skip non-images (some tokens attach json/video); accept entries with no
    // mime at all, since older assets often omit it.
    if (mime && !mime.startsWith("image/")) continue;
    const candidate = sanitizeLogoUrl(file?.cdn_uri) ?? sanitizeLogoUrl(file?.uri);
    if (candidate) return candidate;
  }
  return null;
}

/**
 * Resolve a mint's identity via the Helius DAS `getAsset` method.
 *
 * Returns null when the endpoint isn't DAS-capable (only Helius RPCs implement
 * getAsset), when the asset is unknown, or on any transport failure — callers
 * treat null as "learned nothing" and keep whatever they already had.
 */
export async function fetchDasTokenMetadata(
  rpcEndpoint: string,
  mintAddress: string,
  timeoutMs = 5000,
): Promise<DasTokenMetadata | null> {
  if (!rpcEndpoint.includes("helius-rpc.com")) return null;

  try {
    const res = await fetch(rpcEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: `das-${mintAddress}`,
        method: "getAsset",
        params: { id: mintAddress, options: { showFungible: true } },
      }),
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) return null;

    const json = await res.json();
    const content = json?.result?.content;
    const metadata = content?.metadata;
    const tokenInfo = json?.result?.token_info;

    const decimalsRaw = tokenInfo?.decimals;
    return {
      symbol: sanitizeText(metadata?.symbol ?? tokenInfo?.symbol, MAX_SYMBOL_LEN),
      name: sanitizeText(metadata?.name, MAX_NAME_LEN),
      decimals: typeof decimalsRaw === "number" && Number.isFinite(decimalsRaw) ? decimalsRaw : null,
      logoUrl: extractLogoUrl(content),
    };
  } catch (err) {
    logger.debug("DAS metadata lookup failed", {
      mintAddress: mintAddress.slice(0, 8),
      error: err instanceof Error ? err.message : String(err),
    });
    return null;
  }
}

/**
 * Neutral identity for a market whose base asset cannot be resolved on-chain.
 *
 * v17 admin-oracle markets carry no pointer to what they track — no Pyth
 * indexFeedId, no DEX pool, no mainnet CA. The indexer previously guessed
 * "SOL"/"SOL/USDC Perpetual" here, which made every unresolvable market claim to
 * be SOL. A visibly-unnamed market is honest and is trivially queryable for
 * follow-up (`WHERE metadata_source = 'auto' AND symbol = 'UNKNOWN'`); a market
 * mislabelled SOL is neither.
 */
export function placeholderIdentity(slabAddress: string): { symbol: string; name: string } {
  return {
    symbol: "UNKNOWN",
    name: `Market ${slabAddress.slice(0, 8)}`,
  };
}
