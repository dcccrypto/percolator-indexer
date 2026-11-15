# Write cost of the #283 covering indexes

Local throwaway Postgres 18.3 (unix socket only), schema from `supabase/migrations/*` without the #283 file, seeded with
`bench/slow-queries/seed.sql` (500k trades), vacuumed and checkpointed, cloned per run (`CREATE DATABASE ... TEMPLATE`).
`insert_batch.sql` is the indexer's `insertTradeRows` shape (upsert with `onConflict tx_signature,asset_index,leg_index`
+ `ignoreDuplicates` = `INSERT ... ON CONFLICT DO NOTHING`, 100 rows, `toDbRow` columns). pgbench `-c 1 -t 200`:
200 batches x 100 rows per run, 6 runs per arm, arm order rotated each run. `./run.sh 6` reproduces; raw lines in `results.tsv`.

Arms: **A** today's schema (old `idx_trades_position_lookup` only) · **B** #283 as shipped (old index kept + 2 new) ·
**C** #283 then the manual `DROP INDEX idx_trades_position_lookup` · **D** volume index only · **E** position cover only (old dropped).

Median of 6 runs:

| Arm | tps (batches/s) | ms/batch | p50 / p95 ms | WAL bytes/batch | WAL vs A | table MB | indexes MB | new idx MB |
|---|---|---|---|---|---|---|---|---|
| A baseline | 6.83 | 146.7 | 145.0 / 234.7 | 383,012 | - | 65.9 | 164.8 | 0 |
| B #283 as shipped | 6.16 | 165.5 | 161.1 / 262.4 | 646,596 | +69% | 65.9 | 243.2 | 76.8 |
| C #283 + old dropped | 7.20 | 140.8 | 136.4 / 211.8 | 484,272 | +26% | 65.9 | 200.2 | 76.8 |
| D volume only | 8.09 | 124.2 | 121.4 / 224.0 | 394,269 | +3% | 65.9 | 189.6 | 23.4 |
| E position cover only (old dropped) | 7.51 | 133.2 | 126.0 / 209.7 | 473,070 | +24% | 65.9 | 175.4 | 53.4 |

## How much to trust the timings

Not much. The Mac mini load average was ~28 for the whole run, and one batch of 100 rows took ~100-200 ms (a quiet
box does this in a few ms). Within one arm the 6 runs span 1.7x (A: 99-171 ms/batch), far more than the gaps between
arms, and D/E/C came out *faster* than A, which cannot be a real effect of adding indexes. Read the timing columns as
"no slowdown is distinguishable from noise", with B the only arm whose median is above A (+13%, inside A's own spread).
Do not quote a % write slowdown from them.

The deterministic numbers are WAL bytes (A runs differ by <0.3%) and sizes:
- #283 as shipped (B) writes +264 KB WAL per 100-row batch (+69%, ~2.6 KB/row) and adds 77 MB of index (+47% index size).
- After the manual drop of the old index (C) it is +101 KB/batch (+26%) and +35 MB net.
- The volume index is nearly free to write (+11 KB/batch, +3%: partial, and `created_at` is monotonic so it only
  appends to the right edge). The position cover index carries the whole write cost (+90 KB/batch net of the dropped one),
  because `trader` is random so each row dirties a different leaf and its INCLUDE payload (incl. `tx_signature`) is wide.

## Against the read savings (same box, 500k rows, from bench/slow-queries/README.md)

Per stats poll cycle: Q1 saves ~3.3 ms (7.43 -> 4.10); per closed position, Q3 saves ~5.8 ms (6.15 -> 0.39). Production
write rate is not measured here; at the bench rate a 100-row batch costs ~100 KB extra WAL (arm C) versus ~6 ms saved per
close read. WAL and read savings are different currencies, so the break-even depends on trades/sec vs closes/sec on the live DB.

## Recommendation

- Keep `idx_trades_volume_24h`: ~3% WAL, 23 MB, 9x fewer buffers on Q1. No write argument against it.
- Keep `idx_trades_position_lookup_cover` **and do the manual drop of `idx_trades_position_lookup`** (arm C). Shipping the
  two without the drop (arm B) is the worst case: +69% WAL and +77 MB for no extra read benefit. Net of the drop it is +26% WAL.
- If the live write rate is high enough that +26% WAL matters (replication lag, WAL volume), drop only the position cover
  and keep the old index; Q3 then stays at ~6 ms.
- Re-run `./run.sh` on a quiet host before quoting any latency percentage.
