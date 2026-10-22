/**
 * S3-C11: the client IP the webhook rate limiter keys on, behind a known number of proxies.
 *
 * The limiter used `x-forwarded-for.split(",")[0]`, the LEFTMOST entry, which is whatever the
 * caller wrote: any client could rotate it per request and never hit the per-IP limit. Proxies
 * APPEND to X-Forwarded-For, so the trustworthy entries are on the right. This peels
 * TRUSTED_PROXY_DEPTH hops from the right, the same parser and peel as the app's
 * lib/trusted-proxy-depth.ts + lib/get-client-ip.ts (percolator-launch #3483/#3486).
 *
 *   TRUSTED_PROXY_DEPTH unset / blank     -> 1 (Railway's edge alone; rightmost hop is the client)
 *   integer 0..MAX (trimmed)              -> that integer
 *   anything else                         -> 1, logged once per distinct value
 *
 * Depth 0 means no proxy in front, so no forwarded header is trusted at all and the socket's
 * remote address is used. The depth must equal the real number of proxies: a larger one lands
 * on a left-hand entry the caller wrote.
 */
import { isIPv4, isIPv6 } from "node:net";

export const DEFAULT_TRUSTED_PROXY_DEPTH = 1;
export const MAX_TRUSTED_PROXY_DEPTH = 8;
export const UNKNOWN_IP = "unknown-ip";

const reported = new Set<string>();

export function trustedProxyDepth(
  raw: string | undefined = process.env.TRUSTED_PROXY_DEPTH,
  onInvalid: (msg: string) => void = (msg) => console.error(msg),
): number {
  const v = raw?.trim() ?? "";
  if (v === "") return DEFAULT_TRUSTED_PROXY_DEPTH;
  if (/^\d+$/.test(v)) {
    const n = Number(v);
    if (n <= MAX_TRUSTED_PROXY_DEPTH) return n;
  }
  if (!reported.has(v)) {
    reported.add(v);
    onInvalid(
      `[trusted-proxy-depth] TRUSTED_PROXY_DEPTH=${JSON.stringify(raw)} is not an integer 0..${MAX_TRUSTED_PROXY_DEPTH}; ` +
        `using ${DEFAULT_TRUSTED_PROXY_DEPTH}.`,
    );
  }
  return DEFAULT_TRUSTED_PROXY_DEPTH;
}

function isValidIp(s: string | undefined): s is string {
  return !!s && (isIPv4(s) || isIPv6(s));
}

export interface ClientIpInput {
  /** Header lookup (case-insensitive, as Hono's c.req.header). */
  header: (name: string) => string | undefined;
  /** The TCP peer address, if known. */
  remoteAddress?: string;
  depth: number;
}

export function clientIp({ header, remoteAddress, depth }: ClientIpInput): string {
  if (depth > 0) {
    const forwarded = header("x-forwarded-for");
    if (forwarded) {
      const ips = forwarded.split(",").map((s) => s.trim()).filter(Boolean);
      if (ips.length > 0) {
        const candidate = ips[Math.max(0, ips.length - depth)];
        if (isValidIp(candidate)) return candidate;
      }
    }
    const realIp = header("x-real-ip")?.trim();
    if (isValidIp(realIp)) return realIp;
  }
  return isValidIp(remoteAddress) ? remoteAddress : UNKNOWN_IP;
}
