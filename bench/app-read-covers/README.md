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
    ./writes-cpu.sh 7              # backend CPU seconds (log_executor_stats) for the same upsert
    python3 -I table.py $COV_DIR/reads.json $COV_DIR/writes.tsv $COV_DIR/writes-explain.tsv $COV_DIR/writes-cpu.tsv

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

| Read | rows | S0 no covers: min ms / buf | S1 +20261115: min ms / buf | S2 +this PR: min ms / buf | S1->S2 | S2 plan (trades scan) |
|---|---|---|---|---|---|---|
| A1 queryTrades indexer-db.ts:111 (hot slab0, limit 100) | 100 | 0.41 / 103 | 0.43 / 103 | 0.74 / 103 | 0.6x | Index Scan using idx_trades_network on trades |
| A1b queryTrades cold slab299 | 100 | 1.02 / 104 | 1.78 / 104 | 0.55 / 104 | 3.2x | Index Scan using idx_trades_network on trades |
| A2 CANDLE_AGG_SQL indexer-db.ts:203 (slab0, 5m bars, 7d) | 1632 | 117.17 / 3432 | 403.27 / 3432 | 68.67 / 48 | 5.9x | Index Only Scan using idx_trades_slab_net_created_cover on trades (heap fetches 0) |
| A2b CANDLE_AGG_SQL slab0, 1h bars, 30d | 722 | 621.54 / 4023 | 760.09 / 4023 | 420.53 / 160 | 1.8x | Index Only Scan using idx_trades_slab_net_created_cover on trades (heap fetches 0) |
| A2c queryTradesForCandles indexer-db.ts:162 (slab0, 7d, limit 50000) | 3405 | 12.54 / 3423 | 113.81 / 3423 | 3.84 / 39 | 29.7x | Index Only Scan using idx_trades_slab_net_created_cover on trades (heap fetches 0) |
| A3 queryLastTradePriceBefore indexer-db.ts:281 (slab0) | 1 | 0.10 / 4 | 0.07 / 4 | 0.16 / 4 | 0.4x | Index Only Scan using idx_trades_slab_net_created_cover on trades (heap fetches 0) |
| A4 TRADE_FEED_SQL chart/trade-feed.ts:72 (20 slabs, after = 1h ago) | 56 | 1.52 / 125 | 1.00 / 125 | 2.01 / 125 | 0.5x | Index Scan using idx_trades_network on trades |
| A5 queryStatsAggregate indexer-db.ts:455 (global, full scan) | 1 | 23813.02 / 251172 | 24642.84 / 251172 | 21319.90 / 251172 | 1.2x | Index Scan using idx_trades_network on trades |
| A6 queryKnownSlabs indexer-db.ts:494 | 100 | 62.28 / 452 | 137.64 / 452 | 100.38 / 452 | 1.4x | Index Only Scan using idx_trades_network on trades (heap fetches 0) |
| A7a leaderboard 24h indexer-db.ts:546 | 50 | 285.13 / 4099 | 324.86 / 4070 | 14.08 / 83 | 23.1x | Index Only Scan using idx_trades_net_created_cover on trades (heap fetches 0) |
| A7b leaderboard 7d indexer-db.ts:562 | 50 | 1004.66 / 4099 | 1337.89 / 4070 | 690.38 / 552 | 1.9x | Index Only Scan using idx_trades_net_created_cover on trades (heap fetches 0) |
| A7c leaderboard alltime indexer-db.ts:578 | 50 | 1587.75 / 4099 | 1738.04 / 4070 | 1530.54 / 4070 | 1.1x | Seq Scan on trades |
| A8 queryTraderStatsAggregate indexer-db.ts:706 (heavy trader0) | 1 | 76.07 / 2429 | 34.38 / 2429 | 15.79 / 44 | 2.2x | Index Only Scan using idx_trades_trader_net_created_cover on trades (heap fetches 0) |
| A9 queryTraderStatsRows indexer-db.ts:748 (trader0) | 3573 | 18.46 / 2429 | 18.43 / 2429 | 2.65 / 41 | 7.0x | Index Only Scan using idx_trades_trader_net_created_cover on trades (heap fetches 0) |
| A10a trader trades count indexer-db.ts:793 (trader0, no slab) | 1 | 1.17 / 32 | 4.20 / 50 | 2.05 / 41 | 2.1x | Index Only Scan using idx_trades_trader_net_created_cover on trades (heap fetches 0) |
| A10b trader trades page indexer-db.ts:830 (trader0, offset 100) | 20 | 0.47 / 129 | 0.55 / 129 | 1.02 / 129 | 0.5x | Index Scan using idx_trades_trader_created on trades |
| A10c trader trades page with slab filter indexer-db.ts:818 (trader0, slab0) | 20 | 1.40 / 215 | 0.88 / 216 | 1.53 / 216 | 0.6x | Bitmap Heap Scan on trades |
| A11 queryLpVaultPosition indexer-db.ts:900 | 0 | 0.02 / 0 | 0.02 / 0 | 0.02 / 0 | 0.9x | Seq Scan on lp_vault_positions |
| I1 volume_24h_by_slab StatsCollector.ts:565 | 300 | 298.57 / 4675 | 11.97 / 647 | 5.14 / 738 | 2.3x |  |
| I2 fetchStoredLegsMany storedLegs.ts:65 (100 sigs) | 200 | 4.26 / 404 | 1.87 / 404 | 6.04 / 404 | 0.3x | Index Scan using uq_trades_sig_asset_leg on trades |
| I2b fetchStoredLegs storedLegs.ts:25 (1 sig) | 2 | 0.12 / 4 | 0.10 / 4 | 0.09 / 4 | 1.1x | Bitmap Heap Scan on trades |
| I3 fetchTraderPositionEvidence traderNetPosition.ts:84 (trader0) | 67 | 0.34 / 76 | 0.15 / 7 | 0.17 / 7 | 0.9x | Index Only Scan using idx_trades_position_lookup_cover on trades (heap fetches 0) |
| I4 LpVaultStore.loadEvents lpVault/store.ts:244 | 10 | 0.12 / 21 | 0.12 / 21 | 0.14 / 21 | 0.9x | Index Scan using idx_lp_vault_events_position on lp_vault_events |
| I5 markets keyset marketRows.ts:74 | 234 | 1.24 / 10 | 1.07 / 10 | 1.20 / 10 | 0.9x | Seq Scan on markets |
| I6 refreshMarketMetadata StatsCollector.ts:464 | 50 | 0.07 / 4 | 0.06 / 4 | 0.07 / 4 | 0.9x | Seq Scan on markets |
| K1 keeper register-poll markets read (keeper_status=active) | 249 | 0.28 / 10 | 0.28 / 10 | 0.28 / 10 | 1.0x | Seq Scan on markets |
| V1 v22_events by slab (no reader in repo/app; indexer writes only) slab0 newest 50 | 50 | 0.18 / 52 | 0.17 / 52 | 0.18 / 52 | 1.0x | Index Scan using v22_events_slab_time on v22_events |

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

| State | shape | rows/run | min secs | median secs | median rows/s | median WAL bytes | median Btree WAL | median Heap WAL | median FPI |
|---|---|---|---|---|---|---|---|---|---|
| S0 | single | 5000 | 4.26 | 24.12 | 207 | 32734008 | 31513962 | 877891 | 30013720 |
| S0 | batch50 | 10000 | 16.75 | 23.36 | 428 | 49204680 | 46085922 | 2912238 | 38964084 |
| S1 | single | 5000 | 11.26 | 42.98 | 116 | 35941216 | 34707587 | 877891 | 32868388 |
| S1 | batch50 | 10000 | 15.36 | 32.68 | 306 | 61115872 | 57954482 | 2911624 | 48035896 |
| S2 | single | 5000 | 19.64 | 38.69 | 129 | 51277008 | 49985785 | 877891 | 46991592 |
| S2 | batch50 | 10000 | 29.95 | 69.30 | 144 | 89473200 | 86214568 | 2911914 | 70934584 |

The wall-clock columns are not usable here: the same state's 5,000-row run took 4.3 s to 56 s across reps as the host load
moved, so the S1/S2 difference is inside that noise.

**EXPLAIN (ANALYZE, BUFFERS, WAL) of one seeded 10,000-row upsert** (`writes-explain.sh`; same rows in every state, rolled back;
"after checkpoint" = CHECKPOINT first, "steady" = pages already logged since the last checkpoint, so no full-page images):

| State | mode | min exec ms (5) | WAL records | WAL FPI | WAL bytes | shared hit | hit/row |
|---|---|---|---|---|---|---|---|
| S0 | after checkpoint | 19689 | 80819 | 5023 | 42628913 | 252427 | 25.2 |
| S1 | after checkpoint | 22134 | 90889 | 6189 | 52462206 | 261547 | 26.2 |
| S2 | after checkpoint | 24428 | 121410 | 9213 | 80029628 | 332288 | 33.2 |
| S0 | steady | 21381 | 80819 | 0 | 7340292 | 252428 | 25.2 |
| S1 | steady | 17326 | 90889 | 0 | 8705524 | 261549 | 26.2 |
| S2 | steady | 24545 | 121412 | 0 | 14029700 | 332295 | 33.2 |

## Is the write cost worth it?

**Insert CPU** (`writes-cpu.sh`: backend user/system CPU from `log_executor_stats` for the same seeded 10,000-row upsert,
steady state, rolled back, 7 runs per state; CPU time is far less load-sensitive than wall time here: 1 s of CPU took 4-28 s elapsed):

| State | runs | min user CPU s | median user CPU s | median user+sys s | median elapsed s | rows/CPU-s (median user+sys) |
|---|---|---|---|---|---|---|
| S0 | 7 | 0.848 | 0.940 | 1.075 | 13.3 | 9301 |
| S1 | 7 | 0.866 | 0.928 | 1.196 | 15.1 | 8361 |
| S2 | 7 | 1.193 | 1.335 | 1.723 | 16.2 | 5803 |


Every S2 run used more CPU than every S1 run (S2 min 1.19 s > S1 max 1.03 s user), so this one is a real signal, not noise.

| Per inserted trade, S1 -> S2 | Cost |
|---|---|
| backend CPU (median user+sys) | 120 -> 172 µs (+53 µs, +44%) |
| buffer touches | 26.2 -> 33.2 (+7, +27%) |
| WAL records | 9.1 -> 12.1 (+3: one per new index) |
| WAL bytes, steady state | 871 -> 1,403 B (+533 B, +61%) |
| WAL bytes, first touch after a checkpoint (full-page images) | 5.2 -> 8.0 KB (+2.8 KB, +53%); pgbench single-row 7.2 -> 10.3 KB (+43%), 50-row batches 6.1 -> 8.9 KB (+46%) |
| index size | +256 B (64 MB on 250k rows) |

What that buys, per read (S1 -> S2, buffers; times are min of 5 under load):

| Read | buffers saved per call | min ms |
|---|---|---|
| A7a leaderboard 24 h | 4070 -> 83 (3,987) | 325 -> 14 |
| A2 candles 5 m / 7 d | 3432 -> 48 (3,384) | 403 -> 69 |
| A2c queryTradesForCandles | 3423 -> 39 (3,384) | 114 -> 3.8 |
| A2b candles 1 h / 30 d | 4023 -> 160 (3,863) | 760 -> 421 |
| A7b leaderboard 7 d | 4070 -> 552 (3,518) | 1338 -> 690 |
| A8 / A9 trader stats | 2429 -> 44 / 41 (~2,387 each) | 34 -> 16 / 18 -> 2.7 |

Verdict: **worth it, by a wide margin, at this ingest rate.** The data set's rate (250k trades / 30 days, about 8,300 inserts a day,
roughly the devnet rate) costs about 8,300 × 7 = 58k extra buffer touches, 0.44 CPU-seconds and 4-23 MB of WAL per day. One
24 h leaderboard or one chart load saves ~3,400-4,000 buffer touches, so about 15 such reads a day pay for a whole day of extra index
maintenance. The ingest path is RPC- and round-trip-bound (one PostgREST call per transaction), so +53 µs of DB CPU per row does not
change its throughput; the DB-bound ceiling for batched inserts drops from ~8,400 to ~5,800 rows per CPU-second, far above the real
rate. Where it would stop paying: a write-heavy network with few app readers (e.g. ~1M+ inserts a day and a handful of chart or
leaderboard views); re-measure before applying there.
