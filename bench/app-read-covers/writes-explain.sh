#!/bin/bash
# writes-explain.sh REPS: EXPLAIN (ANALYZE, BUFFERS, WAL) of one seeded 10k-row trades upsert, fresh template copy per (rep, state), rolled back.
. "$(dirname "$0")/env.sh"
REPS=${1:-5}; OUT=$D/writes-explain.tsv; echo -e "rep\tstate\tcheckpointed\texec_ms\twal_records\twal_fpi\twal_bytes\tshared_hit\tshared_dirtied\tshared_written" > $OUT
Q="EXPLAIN (ANALYZE, BUFFERS, WAL, FORMAT JSON) INSERT INTO trades (slab_address, trader, side, size, price, fee, tx_signature, asset_index, leg_index, is_liquidation, network, size_provenance)
SELECT 'slab' || floor(300 * power(random(), 2))::int, 'trader' || floor(5000 * power(random(), 2))::int,
       (array['long','short'])[1 + (random() < 0.5)::int], round((random() * 1e9)::numeric, 0), round((10 + random() * 90)::numeric, 6),
       round((random() * 1e6)::numeric, 0), 'insx_' || g, 0, g % 2, false, 'devnet', NULL
  FROM generate_series(1, 10000) g
ON CONFLICT (tx_signature, asset_index, leg_index) DO NOTHING RETURNING tx_signature, asset_index, leg_index"
for r in $(seq 1 $REPS); do for s in t0 t1 t2; do for ck in yes no; do
  $PG/dropdb -h $D -p $PORT -U postgres --if-exists w_$s 2>/dev/null; $PG/createdb -h $D -p $PORT -U postgres -T $s w_$s
  if [ $ck = yes ]; then px postgres -c CHECKPOINT; else px w_$s -c "SELECT count(*) FROM trades" >/dev/null; px w_$s -c "SELECT count(*) FROM trades" >/dev/null; fi
  j=$(printf '%s\n' "BEGIN;" "SELECT setseed(0.7);" "$Q;" "ROLLBACK;" | px w_$s -At)
  echo "$j" | python3 -I -c "
import sys,json; t=sys.stdin.read(); j=json.loads(t[t.index('['):t.rindex(']')+1])[0]; p=j['Plan']
print('\t'.join(map(str,['$r','$s','$ck',j['Execution Time'],p.get('WAL Records'),p.get('WAL FPI'),p.get('WAL Bytes'),p.get('Shared Hit Blocks'),p.get('Shared Dirtied Blocks'),p.get('Shared Written Blocks')])))" >> $OUT
done; done; done
$PG/dropdb -h $D -p $PORT -U postgres --if-exists w_t0; $PG/dropdb -h $D -p $PORT -U postgres --if-exists w_t1; $PG/dropdb -h $D -p $PORT -U postgres --if-exists w_t2
cat $OUT
