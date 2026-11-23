Local-only bench for the indexer's recurring trade reads. Needs a throwaway Postgres (built with 18; 16 works).

    export LC_ALL=en_US.UTF-8
    initdb -D /tmp/pg && pg_ctl -D /tmp/pg -o "-p 55433 -k /tmp -c listen_addresses=''" start && createdb -h /tmp -p 55433 bench
    psql -h /tmp -p 55433 bench -c "create role anon; create role authenticated; create role service_role; create schema auth; create schema extensions;"
    for f in supabase/migrations/*.sql; do [ "$f" = supabase/migrations/20261115120000_trades_covering_read_indexes.sql ] || psql -h /tmp -p 55433 bench -q -f $f; done   # skip the new file for the "before"; 1 unrelated error is expected (realtime publication)
    psql -h /tmp -p 55433 bench -f seed.sql      # 500k trades / 30 days, 12 markets, 8000 lp events; slow on a loaded machine
    psql -h /tmp -p 55433 bench -c "VACUUM (ANALYZE) trades"
    python3 -I best.py "psql -h /tmp -p 55433 bench" 15    # min/median of 15 EXPLAIN ANALYZE runs + buffers
    ./run.sh                                               # pgbench p50/p95 (very noisy on a busy host)

Apply `db/manual/20261115120000_trades_covering_read_indexes.sql` between runs for the "after" (VACUUM ANALYZE again). The `supabase/migrations` stand-in of the same name only builds on an empty trades table.
Queries: Q1 `volume_24h_by_slab`, Q2 `fetchStoredLegsMany` (100 signatures), Q3 `fetchTraderPositionEvidence`.

## Results (500k trades, 16.6k in the last 24h, Postgres 18.3, Mac mini load average ~25)

| Query | Before: min / median, buffers | After: min / median, buffers | Verdict |
|---|---|---|---|
| Q1 volume_24h_by_slab (bitmap heap scan -> index-only scan on idx_trades_volume_24h) | 3.97 / 7.43 ms, 486 | 2.24 / 4.10 ms, 53 | kept: ~1.8x, 9x fewer buffers |
| Q3 position evidence (bitmap heap + sort -> index-only scan on idx_trades_position_lookup_cover) | 3.74 / 6.15 ms, 1239 | 0.26 / 0.39 ms, 19 | kept: ~14x, 65x fewer buffers |
| Q2 stored legs, 100 sigs (already an index scan on uq_trades_sig_asset_leg) | 1.44 / 2.54 ms, 400 | 1.52 / 2.86 ms, 400 (a covering sig index gave 301 buffers but was not faster) | no gain: not shipped |

Caveat: the table was freshly vacuumed, so every page is all-visible. In production the newest pages (what Q1 reads) lag autovacuum, so expect Heap Fetches > 0 and a smaller Q1 gain than shown; Q3 reads old pages and is less affected. Write cost: see the next section.

Response rows are byte-identical before and after (md5 of Q1 output and Q3 rows). pgbench p50 numbers were
too noisy under load (Q1 went 45 ms -> 27-40 ms across runs, Q2/Q3 swung 2x between identical runs), so the
table above is from the EXPLAIN runs; the buffer counts are the deterministic part.

## Write and size cost of the cover index (re-measured with the old index dropped)

Same throwaway Postgres 18 bench, ~800k trades after earlier insert runs (so absolute sizes are larger than the 500k table above). Each state: VACUUM ANALYZE, CHECKPOINT, then WAL bytes of a 100k-row INSERT into trades (rolled back), plus total size of all trades indexes.

| State | Trades index size | WAL for 100k inserts | vs before |
|---|---|---|---|
| A. before the PR (idx_trades_position_lookup only) | 259 MB | 178 MB | baseline |
| B. PR as first built (old + cover) | 379 MB | 262 MB | +46% size (+120 MB), +47% WAL |
| C. migration with the DROP (cover only) | 342 MB | 186 MB | +32% size (+83 MB), +4.5% WAL |

Dropping the old index removes about 37 MB of index and ~76 MB of WAL per 100k inserts versus keeping both. After the full migration (DROP included) EXPLAIN still shows Q3 as an Index Only Scan on idx_trades_position_lookup_cover, Q1 on idx_trades_volume_24h, and Q2 unchanged. WAL numbers move a little between runs (full-page writes after checkpoints); the earlier PR note quoted +69% / +77 MB for B on a 500k table.
