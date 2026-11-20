Local-only bench for the three app-read covering indexes in `db/manual/20261120120000_trades_app_read_cover_indexes.sql`
(follow-up to the app query-plan report J-20261120-IDX-REPORT-APPQUERYPLANSb). Scratch Postgres 16 only; nothing here touches a hosted DB.

## How to run

    export COV_DIR=/tmp/cov-bench COV_PORT=55477      # any free socket dir / port
    ./start.sh                     # initdb + start (fsync=off, full_page_writes=on, autovacuum=off so the templates stay fixed)
    ./fresh.sh                     # fresh DB from supabase/migrations, applied twice, per-file transaction and autocommit
    ./base.sh                      # t0: every migration except 20261115 + 20261120, then the report's data set (setseed(0.42))
    ./states.sh                    # t1 = t0 + db/manual/20261115 ; t2 = t1 + db/manual/20261120 (CONCURRENTLY), VACUUM ANALYZE
    python3 -I reads.py q.sql 5 $COV_DIR/reads.json t0 t1 t2     # 27 reads, EXPLAIN (ANALYZE, BUFFERS), 5 runs each, states interleaved
    python3 -I md5.py q.sql t1 t2                                 # same rows before/after
    ./writes.sh 5                  # pgbench: 5,000 single-row upserts + 200 x 50-row upserts, fresh template copy per run
    ./writes-explain.sh 5          # EXPLAIN (ANALYZE, BUFFERS, WAL) of one seeded 10,000-row upsert, rolled back
    python3 -I table.py $COV_DIR/reads.json $COV_DIR/writes.tsv

`q.sql` is the report's query file (app `percolator-launch` `feat/v22-app:app/lib/indexer-db.ts`, `app/lib/chart/trade-feed.ts`,
indexer `src/`, keeper). The inserts use the indexer's own statement shape (`insertTradeRow` / `insertTradeRows`:
`INSERT ... ON CONFLICT (tx_signature, asset_index, leg_index) DO NOTHING RETURNING ...`).

## Data and states

Same generator and seed as the report: 300 markets, 250,000 trades over 30 days (slab = floor(300·r²), trader = floor(5000·r²),
1% liquidation markers), 50,000 v22_events, 20,000 lp_vault_events; 8,228 trades in the last 24 h. One load, then copied with
`CREATE DATABASE ... TEMPLATE`, so all three states hold byte-identical rows:

- **S0** (t0): shipped indexes, i.e. what the report called "before".
- **S1** (t1): S0 + `db/manual/20261115120000` (idx_trades_volume_24h, idx_trades_position_lookup_cover, drops idx_trades_position_lookup).
- **S2** (t2): S1 + this PR's three indexes. **S1 -> S2 is this PR.**

Index sizes (250k rows, table heap 33 MB): S0 all trades indexes 63 MB; S1 87 MB; S2 151 MB (+64 MB:
idx_trades_net_created_cover 19 MB, idx_trades_slab_net_created_cover 21 MB, idx_trades_trader_net_created_cover 23 MB).
The CONCURRENTLY build of the three took 50 s on this host.

## Reads (min ms of 5 / shared buffers of the plan)

Host load average 30-50 throughout. Every read is warm (`shared hit`), so the buffer counts are the deterministic part; the
min-of-5 times still swing 2-4x between identical states (e.g. A2 S0 vs S1 has the same plan and buffers but 117 vs 403 ms).
`md5.py`: all 27 reads return the same rows on S1 and S2 (sorted-output compare).

{{READS}}

Notes:
- Reads that switch plans (A2, A2b, A2c, A3, A7a, A7b, A8, A9, A10a) all become Index Only Scans with **0 heap fetches** on the new
  indexes, and buffers drop 3423 -> 39 (A2c), 3432 -> 48 (A2), 4023 -> 160 (A2b), 4070 -> 83 (A7a), 4070 -> 552 (A7b), 2429 -> 44/41 (A8/A9).
- Sub-millisecond rows with unchanged plan and buffers (A1, A3, A4, A10b, A10c, I2 ...) moving 0.3-3x either way are load noise.
- I1 `volume_24h_by_slab`: the function's inner plan is the same on S1 and S2 (auto_explain: Index Only Scan on
  idx_trades_volume_24h, 55 buffers); the outer 647 -> 738 buffers is planning over the extra index catalog entries.
- Not fixed by an index (left for separate work, not done here):
  - **A5 queryStatsAggregate** (21-25 s): two all-time `COUNT(DISTINCT)` over every trade. Needs a rollup table
    (per-network distinct slabs/traders, maintained on insert or by the stats cycle) or a short-TTL cache in the app.
  - **A7c all-time leaderboard** (1.5-1.7 s): whole-table aggregate (Seq Scan). Needs a per-trader rollup or a cache.
  - **A6 queryKnownSlabs** (60-140 ms): a recursive skip scan over idx_trades_network measured 20 ms in the report, or read `markets`.
  - A7b (7 d leaderboard) still aggregates ~55k index rows (690 ms); a rollup would also cover it.

## Ingest write cost

Fixed batches on a fresh template copy per run (5 runs per state, states interleaved), CHECKPOINT right before each batch, so
the WAL includes the full-page image of every page first touched (the worst case right after a checkpoint).
WAL bytes come from `pg_current_wal_insert_lsn()` and `pg_walinspect.pg_get_wal_stats`; they repeat to within 0.5% across runs.

**pgbench** (`writes.sh`; median of 5):

{{PGBENCH}}

The wall-clock columns are not usable here: the same state's 5,000-row run took 4.3 s to 56 s across reps as the host load
moved, so the S1/S2 difference is inside that noise.

**EXPLAIN (ANALYZE, BUFFERS, WAL) of one seeded 10,000-row upsert** (`writes-explain.sh`; same rows in every state, rolled back;
"after checkpoint" = CHECKPOINT first, "steady" = pages already logged since the last checkpoint, so no full-page images):

{{EXPLAIN}}

## Is the write cost worth it?

{{VERDICT}}
