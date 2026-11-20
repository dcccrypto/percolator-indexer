#!/bin/bash
# writes-cpu.sh REPS: backend user+system CPU seconds (log_executor_stats) for the same seeded 10,000-row upsert, steady state, rolled back.
. "$(dirname "$0")/env.sh"
REPS=${1:-5}; OUT=$D/writes-cpu.tsv; echo -e "rep\tstate\tuser_s\tsys_s\telapsed_s" > $OUT
Q="INSERT INTO trades (slab_address, trader, side, size, price, fee, tx_signature, asset_index, leg_index, is_liquidation, network, size_provenance)
SELECT 'slab' || floor(300 * power(random(), 2))::int, 'trader' || floor(5000 * power(random(), 2))::int,
       (array['long','short'])[1 + (random() < 0.5)::int], round((random() * 1e9)::numeric, 0), round((10 + random() * 90)::numeric, 6),
       round((random() * 1e6)::numeric, 0), 'insx_' || g, 0, g % 2, false, 'devnet', NULL
  FROM generate_series(1, 10000) g
ON CONFLICT (tx_signature, asset_index, leg_index) DO NOTHING RETURNING tx_signature, asset_index, leg_index"
for r in $(seq 1 $REPS); do for s in t0 t1 t2; do
  $PG/dropdb -h $D -p $PORT -U postgres --if-exists w_$s 2>/dev/null; $PG/createdb -h $D -p $PORT -U postgres -T $s w_$s
  o=$(printf '%s\n' "SET client_min_messages=log;" "BEGIN;" "SELECT setseed(0.7);" "SET log_executor_stats=on;" "$Q;" "SET log_executor_stats=off;" "ROLLBACK;" | px w_$s -At 2>&1 >/dev/null)
  echo -e "$r\t$s\t$(echo "$o" | sed -n 's/^![[:space:]]*\([0-9.]*\) s user, \([0-9.]*\) s system, \([0-9.]*\) s elapsed.*/\1\t\2\t\3/p' | head -1)" >> $OUT
done; done
for s in t0 t1 t2; do $PG/dropdb -h $D -p $PORT -U postgres --if-exists w_$s; done
cat $OUT
