/**
 * Strict integer env parsing for index.ts (which cannot be imported from a test).
 *
 * `Number(process.env.X ?? d)` is wrong for operator typos: '' -> 0 and 'abc'/'30s' -> NaN, and
 * Node treats a NaN/0 timer delay as 1 ms (tight RPC loop) while a NaN TTL silently disables a
 * cache. Accepts only plain decimal integers (no sign, exponent, hex, separators) inside
 * [min, max]; anything else warns and returns the fallback. Unset/blank is silent.
 */
export interface EnvIntOptions {
  name: string;
  fallback: number;
  min: number;
  max: number;
  warn?: (msg: string, ctx?: Record<string, unknown>) => void;
}

export function envInt(raw: string | undefined, o: EnvIntOptions): number {
  const v = raw?.trim() ?? "";
  if (v === "") return o.fallback;
  if (/^\d{1,15}$/.test(v)) {
    const n = Number(v);
    if (n >= o.min && n <= o.max) return n;
  }
  o.warn?.(`Invalid ${o.name}, falling back to default`, { raw, fallback: o.fallback, min: o.min, max: o.max });
  return o.fallback;
}
