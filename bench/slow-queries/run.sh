#!/bin/bash
# usage: run.sh   -> p50/p95/mean ms per query over 300 runs each (pgbench, fixed seed). PGBIN/PGPORT/PGSOCK override.
export LC_ALL=en_US.UTF-8
B=${PGBIN:-/opt/homebrew/opt/postgresql@18/bin}
P="-h ${PGSOCK:-/tmp} -p ${PGPORT:-55433}"
cd "$(dirname "$0")" && rm -f pgbench_log.*
for q in q1 q2 q3; do
  $B/pgbench $P bench -n -f $q.sql -t 300 -c 1 -l --random-seed=7 >/dev/null
  printf "%s " $q; awk '{print $3/1000}' pgbench_log.* | sort -n | awk '{a[NR]=$1; s+=$1} END {printf "p50=%.3fms p95=%.3fms mean=%.3fms n=%d\n", a[int(NR*0.5)], a[int(NR*0.95)], s/NR, NR}'
  rm -f pgbench_log.*
done
