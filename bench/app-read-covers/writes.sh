#!/bin/bash
# writes.sh REPS: fixed batches on a fresh template copy per (rep, state), states interleaved.
. "$(dirname "$0")/env.sh"
cd "$S"; mkdir -p $D; REPS=${1:-5}; OUT=$D/writes.tsv; echo -e "rep\tstate\tshape\trows\tsecs\ttps\twal_bytes\tbtree_bytes\theap_bytes\tfpi_bytes" > $OUT
px postgres -c "CREATE EXTENSION IF NOT EXISTS pg_walinspect" >/dev/null
lsn() { px postgres -At -c "select pg_current_wal_insert_lsn()"; }
walrow() { px postgres -At -F$'\t' -c "select pg_wal_lsn_diff('$2','$1')::bigint,
  coalesce(sum(combined_size) filter (where \"resource_manager/record_type\"='Btree'),0), coalesce(sum(combined_size) filter (where \"resource_manager/record_type\" in ('Heap','Heap2')),0), sum(fpi_size)
  from pg_get_wal_stats('$1','$2')"; }
run() { # state shape script tx
  local s=$1 sh=$2 f=$3 n=$4 w=w_$1
  px postgres -c "CHECKPOINT"; local a=$(lsn); local t0=$(python3 -c 'import time;print(time.time())')
  local o=$($PG/pgbench -h $D -p $PORT -U postgres -n -c 1 -t $n --random-seed=42 -f $f $w 2>&1)
  local t1=$(python3 -c 'import time;print(time.time())'); local b=$(lsn)
  local tps=$(echo "$o" | sed -n 's/^tps = \([0-9.]*\).*/\1/p' | head -1)
  local rows=$n; [ $sh = batch50 ] && rows=$((n*50))
  echo -e "$r\t$s\t$sh\t$rows\t$(python3 -c "print(round($t1-$t0,3))")\t$tps\t$(walrow $a $b)" >> $OUT
}
for r in $(seq 1 $REPS); do for s in t0 t1 t2; do
  $PG/dropdb -h $D -p $PORT -U postgres --if-exists w_$s 2>/dev/null; $PG/createdb -h $D -p $PORT -U postgres -T $s w_$s
  run $s single 'ins1.sql' 5000
  run $s batch50 'ins50.sql' 200
  px w_$s -At -c "select '$r $s rows', count(*) from trades" ; $PG/dropdb -h $D -p $PORT -U postgres w_$s
done; done
cat $OUT
