/**
 * A small in-memory stand-in for the supabase-js query builder, enough for the real writers (insertTradeRows,
 * insertV22Events, SupabaseLpVaultStore, StatsCollector's volume rollup) and readers to run against one set of tables.
 *
 * Supported: select (column list, `col::text`, `*`), insert, upsert (onConflict, ignoreDuplicates), update, delete,
 * eq / neq / in / gt / gte / lt / lte / is / not(col,'is',null) / or("a.gt.0,b.eq.x"), order, range, limit, maybeSingle,
 * single, `.select()` after a write (returns the written / deleted rows), and rpc("volume_24h_by_slab").
 * Any other builder method is accepted and ignored (recorded in `ignored`).
 */
type Row = Record<string, unknown>;

const UNIQUE: Record<string, string[]> = {
  trades: ["tx_signature", "asset_index", "leg_index"],
  v22_events: ["signature", "ix_index", "inner_index", "network"],
  lp_vault_events: ["signature", "ix_index"],
  lp_vault_positions: ["network", "registry", "user_wallet"],
  market_stats: ["slab_address"],
  skipped_signatures: ["signature", "source"],
  markets: ["slab_address"],
};

export interface FakeSupabase {
  tables: Map<string, Row[]>;
  clock: { t: number };
  ignored: string[];
  rows(table: string): Row[];
  from(table: string): any;
  rpc(name: string, args: Record<string, unknown>): Promise<{ data: unknown; error: null | { message: string } }>;
}

function cmp(a: unknown, b: unknown): number {
  if (typeof a === "number" && typeof b === "number") return a - b;
  const sa = String(a), sb = String(b);
  if (/^-?\d+(\.\d+)?$/.test(sa) && /^-?\d+(\.\d+)?$/.test(sb)) return Number(sa) - Number(sb);
  return sa < sb ? -1 : sa > sb ? 1 : 0;
}

export function createFakeSupabase(startMs = Date.parse("2026-11-20T12:00:00.000Z")): FakeSupabase {
  const tables = new Map<string, Row[]>();
  const clock = { t: startMs };
  const ignored: string[] = [];
  const rows = (t: string) => {
    if (!tables.has(t)) tables.set(t, []);
    return tables.get(t)!;
  };
  const defaults = (table: string, r: Row): Row => {
    const now = new Date(clock.t).toISOString();
    if (table === "trades") return { asset_index: 0, leg_index: 0, is_liquidation: false, fee: 0, created_at: now, ...r };
    if (table === "v22_events") return { inner_index: -1, indexed_at: now, ...r };
    if (table === "lp_vault_events") return { created_at: now, ...r };
    return r;
  };

  function query(table: string) {
    const st = {
      op: "select" as "select" | "insert" | "upsert" | "update" | "delete",
      payload: [] as Row[],
      upsertOpts: {} as { onConflict?: string; ignoreDuplicates?: boolean },
      filters: [] as Array<(r: Row) => boolean>,
      orders: [] as Array<{ col: string; asc: boolean }>,
      from: 0,
      to: Infinity,
      cols: undefined as string | undefined,
      returning: false,
      single: null as null | "maybe" | "one",
      count: false,
    };
    const project = (r: Row): Row => {
      if (!st.cols || st.cols.trim() === "*") return { ...r };
      const out: Row = {};
      for (const raw of st.cols.split(",").map((c) => c.trim()).filter(Boolean)) {
        const [col, cast] = raw.split("::");
        const v = r[col!];
        out[col!] = cast === "text" && v !== null && v !== undefined ? String(v) : v ?? null;
      }
      return out;
    };
    const matches = (r: Row) => st.filters.every((f) => f(r));
    function exec(): { data: unknown; error: null | { message: string; code?: string }; count?: number } {
      const t = rows(table);
      if (st.op === "insert" || st.op === "upsert") {
        const key = st.op === "upsert" && st.upsertOpts.onConflict ? st.upsertOpts.onConflict.split(",").map((s) => s.trim()) : UNIQUE[table];
        const written: Row[] = [];
        for (const p of st.payload) {
          const r = defaults(table, p);
          const hit = key ? t.findIndex((x) => key.every((k) => String(x[k] ?? "") === String(r[k] ?? ""))) : -1;
          if (hit >= 0) {
            if (st.op === "insert") return { data: null, error: { message: `duplicate key value violates unique constraint on ${table}`, code: "23505" } };
            if (st.upsertOpts.ignoreDuplicates) continue;
            t[hit] = { ...t[hit], ...p };
            written.push(t[hit]!);
          } else {
            t.push(r);
            written.push(r);
          }
        }
        return { data: st.returning ? written.map(project) : null, error: null };
      }
      if (st.op === "delete") {
        const keep: Row[] = [], gone: Row[] = [];
        for (const r of t) (matches(r) ? gone : keep).push(r);
        tables.set(table, keep);
        return { data: st.returning ? gone.map(project) : null, error: null };
      }
      if (st.op === "update") {
        const changed: Row[] = [];
        for (const r of t) if (matches(r)) { Object.assign(r, st.payload[0]); changed.push(r); }
        return { data: st.returning ? changed.map(project) : null, error: null };
      }
      let out = t.filter(matches);
      if (st.orders.length) {
        out = [...out].sort((a, b) => {
          for (const o of st.orders) {
            const c = cmp(a[o.col], b[o.col]);
            if (c !== 0) return o.asc ? c : -c;
          }
          return 0;
        });
      }
      const total = out.length;
      out = out.slice(st.from, st.to === Infinity ? undefined : st.to + 1);
      const data = out.map(project);
      if (st.single === "maybe") return { data: data[0] ?? null, error: null };
      if (st.single === "one") return data.length === 1 ? { data: data[0], error: null } : { data: null, error: { message: "not exactly one row", code: "PGRST116" } };
      return { data, error: null, ...(st.count ? { count: total } : {}) };
    }
    const api: Record<string, unknown> = {
      select(cols?: string, opts?: { count?: string }) {
        if (st.op === "select") st.cols = cols;
        else { st.returning = true; st.cols = cols; }
        if (opts?.count) st.count = true;
        return proxy;
      },
      insert(p: Row | Row[]) { st.op = "insert"; st.payload = Array.isArray(p) ? p : [p]; return proxy; },
      upsert(p: Row | Row[], o: { onConflict?: string; ignoreDuplicates?: boolean } = {}) { st.op = "upsert"; st.payload = Array.isArray(p) ? p : [p]; st.upsertOpts = o; return proxy; },
      update(p: Row) { st.op = "update"; st.payload = [p]; return proxy; },
      delete() { st.op = "delete"; return proxy; },
      eq(c: string, v: unknown) { st.filters.push((r) => String(r[c]) === String(v)); return proxy; },
      neq(c: string, v: unknown) { st.filters.push((r) => String(r[c]) !== String(v)); return proxy; },
      in(c: string, vs: unknown[]) { const s = new Set(vs.map(String)); st.filters.push((r) => s.has(String(r[c]))); return proxy; },
      gt(c: string, v: unknown) { st.filters.push((r) => r[c] != null && cmp(r[c], v) > 0); return proxy; },
      gte(c: string, v: unknown) { st.filters.push((r) => r[c] != null && cmp(r[c], v) >= 0); return proxy; },
      lt(c: string, v: unknown) { st.filters.push((r) => r[c] != null && cmp(r[c], v) < 0); return proxy; },
      lte(c: string, v: unknown) { st.filters.push((r) => r[c] != null && cmp(r[c], v) <= 0); return proxy; },
      is(c: string, v: unknown) { st.filters.push((r) => (v === null ? r[c] == null : r[c] === v)); return proxy; },
      not(c: string, op: string, v: unknown) {
        if (op === "is" && (v === null || v === "null")) st.filters.push((r) => r[c] != null);
        else ignored.push(`not.${op}`);
        return proxy;
      },
      or(expr: string) {
        const parts = expr.split(",").map((p) => p.split("."));
        st.filters.push((r) => parts.some(([c, op, v]) => {
          const x = r[c!];
          if (x == null) return false;
          const d = cmp(x, v);
          return op === "gt" ? d > 0 : op === "gte" ? d >= 0 : op === "lt" ? d < 0 : op === "lte" ? d <= 0 : op === "eq" ? d === 0 : false;
        }));
        return proxy;
      },
      order(c: string, o: { ascending?: boolean } = {}) { st.orders.push({ col: c, asc: o.ascending !== false }); return proxy; },
      range(a: number, b: number) { st.from = a; st.to = b; return proxy; },
      limit(n: number) { st.to = st.from + n - 1; return proxy; },
      maybeSingle() { st.single = "maybe"; return proxy; },
      single() { st.single = "one"; return proxy; },
      then(res: (v: unknown) => unknown, rej?: (e: unknown) => unknown) {
        try { return Promise.resolve(exec()).then(res, rej); } catch (e) { return Promise.reject(e).then(res, rej); }
      },
    };
    const proxy: any = new Proxy(api, {
      get(target, prop: string) {
        if (prop in target) return target[prop];
        return () => { ignored.push(String(prop)); return proxy; };
      },
    });
    return proxy;
  }

  return {
    tables,
    clock,
    ignored,
    rows,
    from: (t: string) => query(t),
    async rpc(name: string, args: Record<string, unknown>) {
      if (name !== "volume_24h_by_slab") return { data: null, error: { message: `rpc ${name} not faked` } };
      const since = clock.t - 24 * 3600_000;
      const agg = new Map<string, { volume: bigint; n: number }>();
      for (const r of rows("trades")) {
        if (r.network !== args.p_network || r.is_liquidation || r.size == null) continue;
        if (Date.parse(String(r.created_at)) < since) continue;
        const a = agg.get(String(r.slab_address)) ?? { volume: 0n, n: 0 };
        const sz = BigInt(String(r.size).replace(/^-/, "").split(".")[0]!);
        a.volume += sz;
        a.n++;
        agg.set(String(r.slab_address), a);
      }
      return { data: [...agg].map(([slab_address, a]) => ({ slab_address, volume_24h: a.volume.toString(), trade_count_24h: a.n })), error: null };
    },
  };
}
