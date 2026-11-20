#!/bin/bash
# fresh.sh <repo-snapshot-dir> <tag>: build a fresh DB from supabase/migrations, apply twice, tx + autocommit
. "$(dirname "$0")/env.sh"
R=${1:-$R}; T=${2:-head}; OUT=$D/fresh-$T; mkdir -p $OUT
for mode in tx ac; do
  db=fresh_${T}_$mode; $PG/dropdb -h $D -p $PORT -U postgres --if-exists $db; $PG/createdb -h $D -p $PORT -U postgres $db
  px $db -f $S/stub.sql >/dev/null
  for pass in 1 2; do
    ok=0; err=0
    for f in $(ls $R/supabase/migrations/*.sql | sort); do
      b=$(basename $f); X=; [ $mode = tx ] && X=--single-transaction
      if px $db $X -f $f >$OUT/$mode-$pass-$b.log 2>&1; then ok=$((ok+1)); else err=$((err+1)); echo "ERR $mode pass$pass $b: $(grep -m1 ERROR $OUT/$mode-$pass-$b.log)"; fi
    done
    echo "$mode pass$pass ok=$ok err=$err"
    $PG/pg_dump -h $D -p $PORT -U postgres -s $db | grep -v '^\\\(un\)\?restrict' > $OUT/$mode-schema-$pass.sql
  done
  cmp -s $OUT/$mode-schema-1.sql $OUT/$mode-schema-2.sql && echo "$mode: pass2 schema identical to pass1" || echo "$mode: SCHEMA DIFFERS"
  px $db -At -c "select indexrelid::regclass||' valid='||indisvalid from pg_index where indrelid='trades'::regclass order by 1"
  grep -h WARNING $OUT/$mode-*.log | sort | uniq -c
done
