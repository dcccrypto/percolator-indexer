/**
 * Strip API keys and webhook tokens out of everything the indexer sends to Sentry.
 *
 * `@percolatorct/shared` initSentry() keeps @sentry/node's default http/fetch integrations, which
 * record every outgoing request as a breadcrumb. Those breadcrumbs carry
 * `data["http.query"] = "?api-key=<Helius key>"` (every web3.js RPC call and every
 * HeliusWebhookManager call) and `data.url = ".../api/webhooks/<id>/<token>"` (every Discord alert),
 * and each captured exception ships the last 100 of them (report J-20261120-IDX-REPORT-SECRETURLINLOGS).
 *
 * shared cannot be changed from this repo, so the indexer hooks the client shared created:
 *  - `beforeAddBreadcrumb` scrubs each breadcrumb before it enters the buffer;
 *  - a global-scope event processor scrubs the whole event (breadcrumbs, request, spans, extra)
 *    before it is sent, covering anything added another way.
 * Both need the same @sentry/node instance as shared, so the dependency follows the base's
 * `@sentry/node` override (~10.78.0) and resolves to the single copy shared also uses.
 * Since 10.78 Sentry masks `?api-key=` in `http.query` itself; the Discord token in the URL path,
 * configured secret values and anything outside `http.query` are still only caught here.
 */
import * as Sentry from "@sentry/node";

export const FILTERED = "[Filtered]";

/** Keys whose whole value is a query string; dropped, since that is where the api-key lives. */
const DROP_KEYS = new Set(["http.query", "url.query"]);

const API_KEY_PARAM = /\b(api[-_]?key)=[^&#\s"'<>]*/gi;
const DISCORD_WEBHOOK_TOKEN = /(\/api(?:\/v\d+)?\/webhooks\/\d+\/)[^/?#\s"'<>]+/gi;

/** Values of configured secrets, matched literally wherever they appear (e.g. a key in a URL path). */
export function configuredSecrets(env: NodeJS.ProcessEnv): string[] {
  const out: string[] = [];
  for (const name of ["HELIUS_API_KEY", "HELIUS_MAINNET_API_KEY", "HELIUS_DEVNET_API_KEY", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY", "HELIUS_WEBHOOK_SECRET"]) {
    const v = env[name];
    if (v) out.push(v);
  }
  for (const name of ["RPC_URL", "FALLBACK_RPC_URL"]) {
    const raw = env[name];
    if (!raw) continue;
    try {
      const k = new URL(raw).searchParams.get("api-key");
      if (k) out.push(k);
    } catch {
      /* not a URL: nothing to extract */
    }
  }
  const hook = env.DISCORD_ALERT_WEBHOOK;
  if (hook) {
    const m = /\/webhooks\/\d+\/([^/?#]+)/.exec(hook);
    if (m) out.push(m[1]);
  }
  // Very short values would redact unrelated text; real keys/tokens are far longer.
  return [...new Set(out)].filter((s) => s.length >= 8);
}

export function scrubString(s: string, secrets: readonly string[] = []): string {
  let r = s.replace(API_KEY_PARAM, `$1=${FILTERED}`).replace(DISCORD_WEBHOOK_TOKEN, `$1${FILTERED}`);
  for (const secret of secrets) if (r.includes(secret)) r = r.split(secret).join(FILTERED);
  return r;
}

/** Scrub every string in `value` in place (objects/arrays); returns the scrubbed value. */
export function scrubDeep<T>(value: T, secrets: readonly string[] = [], seen = new WeakSet<object>()): T {
  if (typeof value === "string") return scrubString(value, secrets) as T;
  if (value === null || typeof value !== "object") return value;
  // Sentry serialises these in `extra`/contexts (href; name/message/stack), so scrub a plain copy.
  if (value instanceof URL) return scrubString(value.href, secrets) as T;
  if (value instanceof Error) return scrubDeep({ name: value.name, message: value.message, stack: value.stack }, secrets, seen) as T;
  // Bytes go out as their numbers, so a secret inside them survives string scrubbing.
  if (ArrayBuffer.isView(value)) {
    const text = Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("utf8");
    return (scrubString(text, secrets) === text ? value : FILTERED) as T;
  }
  // Anything else Sentry sends as toJSON(), or for class instances (a web3 Connection, a client or
  // response class, Map/Headers) as their own enumerable props. Scrub a plain copy of that: the live
  // object, which may be the indexer's own Connection or an SDK scope, is never mutated.
  const proto = Object.getPrototypeOf(value);
  const toJSON = (value as { toJSON?: unknown }).toJSON;
  if (typeof toJSON === "function" || (!Array.isArray(value) && proto !== Object.prototype && proto !== null)) {
    if (seen.has(value)) return FILTERED as T; // a copy cannot point back at itself; drop the cycle
    seen.add(value);
    let copy: unknown;
    try {
      copy = typeof toJSON === "function" ? toJSON.call(value) : Object.fromEntries(Object.keys(value).map((k) => [k, (value as Record<string, unknown>)[k]]));
    } catch {
      return FILTERED as T; // a throwing getter/toJSON: Sentry could not serialise it either
    }
    return scrubDeep(copy, secrets, seen) as T;
  }
  if (seen.has(value)) return value;
  seen.add(value);
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) value[i] = scrubDeep(value[i], secrets, seen);
    return value;
  }
  const obj = value as Record<string, unknown>;
  for (const key of Object.keys(obj)) {
    if (key === "sdkProcessingMetadata") continue; // SDK-internal, stripped before sending
    if (DROP_KEYS.has(key)) {
      delete obj[key];
      continue;
    }
    obj[key] = scrubDeep(obj[key], secrets, seen);
  }
  return value;
}

let installed = false;

/**
 * Install the scrubber on the client created by initSentry(). Call right after initSentry().
 * No-op when Sentry is disabled (no SENTRY_DSN) or when already installed.
 */
export function installSentryScrubber(env: NodeJS.ProcessEnv = process.env): boolean {
  const client = Sentry.getClient();
  if (!client || installed) return installed;
  installed = true;
  const secrets = configuredSecrets(env);
  client.on("beforeAddBreadcrumb", (breadcrumb) => {
    scrubDeep(breadcrumb, secrets);
  });
  Sentry.getGlobalScope().addEventProcessor((event) => scrubDeep(event, secrets));
  return true;
}
