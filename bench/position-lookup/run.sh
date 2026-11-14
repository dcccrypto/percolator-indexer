#!/bin/bash
# usage: run.sh   -> prints p50/p95/mean ms over 200 requests (pgbench, fixed seed) and a hash of the response bodies
export LC_ALL=en_US.UTF-8
B=/opt/homebrew/opt/postgresql@16/bin
P="-h /tmp -p 55432"
cd /tmp/idxperf3 && rm -f pgbench_log.*
$B/pgbench $P bench -n -f q.sql -t 200 -c 1 -l --random-seed=7 >/dev/null
awk '{print $3/1000}' pgbench_log.* | sort -n | awk '{a[NR]=$1; s+=$1} END {printf "p50=%.3fms p95=%.3fms mean=%.3fms n=%d\n", a[int(NR*0.5)], a[int(NR*0.95)], s/NR, NR}'
# response-body hash: every (trader,slab,asset) for 1500 traders, full rows
$B/psql $P bench -At -c "SELECT md5(string_agg(r, '|' ORDER BY t)) FROM (SELECT t, (SELECT coalesce(string_agg(concat(side,',',size,',',is_liquidation,',',created_at,',',tx_signature), ';' ORDER BY id),'') FROM (SELECT * FROM trades WHERE trader='trader'||t AND slab_address='slab'||(t%8) AND asset_index=(t%2) AND network='devnet' AND tx_signature<>'x' ORDER BY id LIMIT 1000) q) r FROM generate_series(1,1500) t) z"
