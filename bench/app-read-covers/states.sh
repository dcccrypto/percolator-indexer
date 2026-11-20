#!/bin/bash
. "$(dirname "$0")/env.sh"
C="$PG/createdb -h $D -p $PORT -U postgres"
$PG/dropdb -h $D -p $PORT -U postgres --if-exists t1; $PG/dropdb -h $D -p $PORT -U postgres --if-exists t2
$C -T t0 t1; date +%T; px t1 -f $R/db/manual/20261115120000_trades_covering_read_indexes.sql; date +%T; px t1 -c "VACUUM (ANALYZE) trades"
$C -T t1 t2; s=$(date +%s); px t2 -f $R/db/manual/20261120120000_trades_app_read_cover_indexes.sql; echo "t2 manual build secs $(( $(date +%s)-s ))"
echo "rerun manual (IF NOT EXISTS no-op):"; px t2 -f $R/db/manual/20261120120000_trades_app_read_cover_indexes.sql 2>&1
echo "stand-in on populated t2 copy and t1:"; px t1 --single-transaction -f $R/supabase/migrations/20261120120000_trades_app_read_cover_indexes.sql 2>&1
px t2 --single-transaction -f $R/supabase/migrations/20261120120000_trades_app_read_cover_indexes.sql 2>&1; echo "(t2: no warning expected)"
px t2 -c "VACUUM (ANALYZE) trades"
for db in t0 t1 t2; do echo "== $db"; px $db -At -c "select indexrelid::regclass, pg_relation_size(indexrelid), indisvalid from pg_index where indrelid='trades'::regclass order by 1"; px $db -At -c "select 'table', pg_relation_size('trades'), 'all_idx', pg_indexes_size('trades')"; done
