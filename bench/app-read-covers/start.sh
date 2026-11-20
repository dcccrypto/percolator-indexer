#!/bin/bash
. "$(dirname "$0")/env.sh"
mkdir -p $D; rm -rf $D/data; $PG/initdb -D $D/data -U postgres -A trust >$D/initdb.log 2>&1
$PG/pg_ctl -D $D/data -o "-p $PORT -k $D -c listen_addresses='' -c shared_buffers=512MB -c max_wal_size=8GB -c checkpoint_timeout=1h -c fsync=off -c full_page_writes=on -c maintenance_work_mem=512MB -c max_parallel_workers_per_gather=2 -c autovacuum=off" -l $D/pg.log -w start
