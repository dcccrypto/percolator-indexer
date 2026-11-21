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
 * Both need the same @sentry/node instance as shared, so the dependency is pinned to shared's version.
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
  for (const name of ["HELIUS_API_KEY", "SUPABASE_KEY", "SUPABASE_SERVICE_ROLE_KEY", "HELIUS_WEBHOOK_SECRET"]) {
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
  if (seen.has(value)) return value;
  seen.add(value);
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) value[i] = scrubDeep(value[i], secrets, seen);
    return value;
  }
  const obj = value as Record<string, unknown>;
  for (const key of Object.keys(obj)) {
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
