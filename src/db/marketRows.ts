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

/**
 * Every `markets` row for this network, ALL statuses (registration must see closed
 * rows too, or it re-registers them), paged with `.range()` so the list is never
 * silently truncated at 1000 rows. Concurrent callers share one read.
 */
export function getMarketRows(): Promise<MarketRowLite[]> {
  if (inFlight) return inFlight;
  const run = readAll()
    .then((rows) => {
      cache = { at: Date.now(), rows };
      return rows;
    })
    .finally(() => {
      inFlight = null;
    });
  inFlight = run;
  return run;
}

/** Same rows, reusing a read younger than MARKET_ROWS_TTL_MS. For callers that tolerate a minute of staleness. */
export function getMarketRowsCached(): Promise<MarketRowLite[]> {
  if (cache && Date.now() - cache.at < MARKET_ROWS_TTL_MS) return Promise.resolve(cache.rows);
  return getMarketRows();
}

/** Test hook. */
export function resetMarketRowsCache(): void {
  cache = null;
  inFlight = null;
}

async function readAll(): Promise<MarketRowLite[]> {
  const out: MarketRowLite[] = [];
  for (let from = 0; ; from += MARKET_ROWS_PAGE) {
    const { data, error } = await getSupabase()
      .from("markets")
      .select(COLUMNS)
      .eq("network", getNetwork())
      .order("slab_address", { ascending: true })
      .range(from, from + MARKET_ROWS_PAGE - 1);
    if (error) throw error;
    const rows = (data ?? []) as MarketRowLite[];
    out.push(...rows);
    if (rows.length < MARKET_ROWS_PAGE) return out;
  }
}
