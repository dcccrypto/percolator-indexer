import { getSupabase, getNetwork } from "@percolatorct/shared";

/**
 * The only `markets` columns the indexer reads back from the DB. The shared
 * `getMarkets()` helper does `select *` (~1-2 KB a row) on two timers; these three
 * columns are ~20x smaller, and every caller only ever used them.
 */
export interface MarketRowLite {
  slab_address: string;
  indexer_excluded: boolean | null;
  keeper_status: string | null;
}

const COLUMNS = "slab_address,indexer_excluded,keeper_status";

/** PostgREST caps a response at `max_rows` (1000 on Supabase). Page below it. */
export const MARKET_ROWS_PAGE = 1000;

/** How long a read is reused by `getMarketRowsCached` (the trade poller's 5-min backstop). */
export const MARKET_ROWS_TTL_MS = 60_000;

let cache: { at: number; rows: MarketRowLite[] } | null = null;
let inFlight: Promise<MarketRowLite[]> | null = null;
/** Bumped by resetMarketRowsCache so a read that started before the reset cannot write the cache or clear a newer inFlight. */
let generation = 0;

/**
 * Every `markets` row for this network, ALL statuses (registration must see closed
 * rows too, or it re-registers them), keyset-paged on slab_address so the list is never
 * silently truncated at 1000 rows. Concurrent callers share one read.
 */
export function getMarketRows(): Promise<MarketRowLite[]> {
  if (inFlight) return inFlight;
  const gen = generation;
  const run = readAll()
    .then((rows) => {
      if (gen === generation) cache = { at: Date.now(), rows };
      return rows;
    })
    .finally(() => {
      if (gen === generation) inFlight = null;
    });
  inFlight = run;
  return run;
}

/** Same rows, reusing a read younger than MARKET_ROWS_TTL_MS. For callers that tolerate a minute of staleness. */
export function getMarketRowsCached(): Promise<MarketRowLite[]> {
  if (cache) {
    // A negative age (wall clock stepped back) is expired, not fresh.
    const age = Date.now() - cache.at;
    if (age >= 0 && age < MARKET_ROWS_TTL_MS) return Promise.resolve(cache.rows);
  }
  return getMarketRows();
}

/** Test hook. */
export function resetMarketRowsCache(): void {
  generation++;
  cache = null;
  inFlight = null;
}

async function readAll(): Promise<MarketRowLite[]> {
  // Keyset paging on `slab_address` (UNIQUE in `markets`), not offset paging: with .range(),
  // a row inserted or deleted below the cursor between two page reads shifts every later row
  // by one, so a market at the page boundary is read twice or skipped. A skipped row would be
  // re-registered (harmless, 23505) but also missed by the retired blocklist and the poller
  // for that cycle. `slab_address > last` is immune to writes elsewhere in the table.
  const out: MarketRowLite[] = [];
  let after: string | null = null;
  for (;;) {
    let q = getSupabase()
      .from("markets")
      .select(COLUMNS)
      .eq("network", getNetwork());
    if (after !== null) q = q.gt("slab_address", after);
    const { data, error } = await q
      .order("slab_address", { ascending: true })
      .limit(MARKET_ROWS_PAGE);
    if (error) throw error;
    const rows = (data ?? []) as MarketRowLite[];
    out.push(...rows);
    // Only an empty page ends the read. A short page is NOT the last one: if the project's
    // PostgREST max_rows is below MARKET_ROWS_PAGE every response is capped short, and stopping
    // there would silently drop every later market. Costs one extra (empty) read per call.
    if (rows.length === 0) return out;
    const last = rows[rows.length - 1].slab_address;
    // slab_address is UNIQUE and `gt` is applied server-side: a page that does not advance means a broken query, not data.
    if (last === after) throw new Error("marketRows: keyset cursor did not advance");
    after = last;
  }
}
