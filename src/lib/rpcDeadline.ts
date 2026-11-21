/**
 * Per-call RPC deadline for the indexer's polling loops (hung-RPC liveness, J-20261121).
 *
 * web3.js 1.99 on Node uses the global (undici) fetch, so the agentkeepalive socket timeout it configures is ignored
 * and the only cut-off is undici's 300 s headers/body timeout. A server that trickles bytes is never cut off at all.
 * Every loop (trade poll, stats, discovery, LP vault) has an in-flight guard, so ONE held call parks the whole loop
 * until that call settles. This module bounds every call two ways:
 *
 *  1. `deadlineFetch`: a `fetch` for web3's `Connection` that adds `AbortSignal.timeout(ms)`. The abort covers the
 *     whole request including the body, so it also frees the socket of a trickling server. The shared package builds
 *     its Connection without a `fetch` option, so `deadlineConnection` rebuilds a real `Connection` from the shared
 *     one's endpoint and commitment (plus shared's Origin header) with this fetch.
 *     Upstream: once @percolatorct/shared's `connectionConfig()` passes such a fetch itself, the rebuild can go.
 *  2. `withCallDeadlines`: a Proxy that races every promise-returning method against the same deadline, so a call
 *     that never settles (any transport, a mocked connection, a chain adapter) rejects with `RpcDeadlineError` and the
 *     caller's existing error handling runs.
 *
 * A healthy RPC answers in well under a second, so the default (60 s) changes nothing for it. `RPC_CALL_TIMEOUT_MS`
 * overrides it; "0" turns both off.
 */
import { Connection } from "@solana/web3.js";
import { config } from "@percolatorct/shared";

export const DEFAULT_RPC_CALL_TIMEOUT_MS = 60_000;
export const MIN_RPC_CALL_TIMEOUT_MS = 1_000;
/** undici's own cut-off; a deadline above it would never fire before undici does. */
export const MAX_RPC_CALL_TIMEOUT_MS = 300_000;

export class RpcDeadlineError extends Error {
  constructor(label: string, ms: number) {
    super(`${label} timed out after ${ms} ms`);
    this.name = "RpcDeadlineError";
  }
}

/**
 * Decimal-only. Unset/blank -> default; "0" -> 0 (deadline off); out of [1 s, 300 s] or not a plain integer -> default
 * with a warning.
 */
export function resolveRpcCallTimeoutMs(raw: string | undefined, warn?: (msg: string, ctx?: Record<string, unknown>) => void): number {
  const v = raw?.trim() ?? "";
  if (v === "") return DEFAULT_RPC_CALL_TIMEOUT_MS;
  if (/^\d{1,15}$/.test(v)) {
    const n = Number(v);
    if (n === 0) return 0;
    if (n >= MIN_RPC_CALL_TIMEOUT_MS && n <= MAX_RPC_CALL_TIMEOUT_MS) return n;
  }
  warn?.("Invalid RPC_CALL_TIMEOUT_MS, falling back to default", {
    raw, fallback: DEFAULT_RPC_CALL_TIMEOUT_MS, min: MIN_RPC_CALL_TIMEOUT_MS, max: MAX_RPC_CALL_TIMEOUT_MS,
  });
  return DEFAULT_RPC_CALL_TIMEOUT_MS;
}

let defaultMs: number | null = null;
/** The process-wide deadline, read once from RPC_CALL_TIMEOUT_MS. */
export function rpcCallTimeoutMs(): number {
  if (defaultMs === null) {
    defaultMs = resolveRpcCallTimeoutMs(process.env.RPC_CALL_TIMEOUT_MS, (m, c) => console.warn(m, c));
  }
  return defaultMs;
}

/** `p`, or a rejection with RpcDeadlineError after `ms`. The timer is cleared as soon as `p` settles. ms <= 0: `p`. */
export function withDeadline<T>(p: PromiseLike<T>, ms: number, label: string): Promise<T> {
  if (!(ms > 0)) return Promise.resolve(p);
  return new Promise<T>((resolve, reject) => {
    const t = setTimeout(() => reject(new RpcDeadlineError(label, ms)), ms);
    (t as { unref?: () => void }).unref?.();
    Promise.resolve(p).then(
      (v) => { clearTimeout(t); resolve(v); },
      (e) => { clearTimeout(t); reject(e); },
    );
  });
}

function isThenable(v: unknown): v is PromiseLike<unknown> {
  return (typeof v === "object" || typeof v === "function") && v !== null && typeof (v as { then?: unknown }).then === "function";
}

const proxies = new WeakMap<object, Map<number, object>>();

/**
 * `target` with every method that returns a promise raced against `ms` (label `<label>.<method>`). Synchronous
 * members pass through unchanged. One proxy per (target, ms), so repeated wrapping is cheap and stable.
 */
export function withCallDeadlines<T extends object>(target: T, ms: number = rpcCallTimeoutMs(), label = "rpc"): T {
  if (!(ms > 0)) return target;
  let byMs = proxies.get(target);
  if (!byMs) { byMs = new Map(); proxies.set(target, byMs); }
  const hit = byMs.get(ms);
  if (hit) return hit as T;
  const proxy = new Proxy(target, {
    get(t, prop, receiver) {
      const v = Reflect.get(t, prop, receiver);
      if (typeof v !== "function") return v;
      return (...args: unknown[]) => {
        const out = (v as (...a: unknown[]) => unknown).apply(t, args);
        return isThenable(out) ? withDeadline(out, ms, `${label}.${String(prop)}`) : out;
      };
    },
  });
  byMs.set(ms, proxy);
  return proxy;
}

type FetchFn = (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => Promise<Response>;

/** A fetch that aborts the whole request (headers AND body) after `ms`, combined with any caller signal. */
export function deadlineFetch(ms: number, baseFetch: FetchFn = (i, n) => fetch(i, n)): FetchFn {
  return (input, init) => {
    const timeout = AbortSignal.timeout(ms);
    const signal = init?.signal ? AbortSignal.any([init.signal, timeout]) : timeout;
    return baseFetch(input, { ...init, signal });
  };
}

const rebuilt = new WeakMap<Connection, Map<number, Connection>>();

/** A real web3 Connection like `base` (endpoint, commitment, shared's Origin header) whose fetch has the deadline. */
function withDeadlineFetch(base: Connection, ms: number): Connection {
  let byMs = rebuilt.get(base);
  if (!byMs) { byMs = new Map(); rebuilt.set(base, byMs); }
  let conn = byMs.get(ms);
  if (!conn) {
    const origin = (config as { rpcOrigin?: string } | undefined)?.rpcOrigin;
    conn = new Connection(base.rpcEndpoint, {
      commitment: base.commitment ?? "confirmed",
      ...(origin ? { httpHeaders: { Origin: origin } } : {}),
      fetch: deadlineFetch(ms) as never,
    });
    byMs.set(ms, conn);
  }
  return conn;
}

/**
 * The connection the polling loops use: `base` (normally shared's `getConnection()`) with a per-call deadline.
 * A real web3 Connection is rebuilt once with `deadlineFetch` so the held socket is actually aborted; anything else
 * (test doubles) gets only the promise race. Both are skipped when the deadline is 0.
 */
export function deadlineConnection<C extends object>(base: C, ms: number = rpcCallTimeoutMs()): C {
  if (!(ms > 0)) return base;
  const conn = base instanceof Connection ? (withDeadlineFetch(base, ms) as unknown as C) : base;
  return withCallDeadlines(conn, ms, "rpc");
}
