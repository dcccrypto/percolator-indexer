#!/bin/bash
# Write-cost bench for the #283 covering indexes. LOCAL THROWAWAY POSTGRES ONLY (unix socket, no network).
# Replays the indexer's real batch insert (insertTradeRows: PostgREST upsert, onConflict
# tx_signature,asset_index,leg_index, ignoreDuplicates => INSERT ... ON CONFLICT DO NOTHING, 100 rows,
# columns exactly as toDbRow) as pgbench: 200 batches x 100 rows per run.
#
# Prereq: the DB built per bench/slow-queries/README.md WITHOUT the #283 migration, seeded with
# bench/slow-queries/seed.sql, `create sequence wr_seq`, VACUUM (ANALYZE) trades, CHECKPOINT.
# Name that database $SRC (default bench). Each run clones it (CREATE DATABASE ... TEMPLATE), so every run
# starts from the same bytes; the bench DB must have no other connections while cloning.
#
# usage: run.sh [runs=6]       env: PGBIN PGPORT PGSOCK SRC OUT
# arms:  A = baseline (idx_trades_position_lookup only, today's schema)
#        B = #283 as shipped (old idx_trades_position_lookup kept + idx_trades_volume_24h + idx_trades_position_lookup_cover)
#        C = #283 after the manual step (old idx_trades_position_lookup dropped)
#        D = volume index only, E = position cover only (old one dropped)   [attribution]
# Output: one TSV line per run: arm run tps ms_per_batch p50_ms p95_ms wal_bytes_per_batch table_mb index_mb(total) new_idx_mb
export LC_ALL=en_US.UTF-8
B=${PGBIN:-/opt/homebrew/opt/postgresql@18/bin}
P="-h ${PGSOCK:-/tmp} -p ${PGPORT:-55443}"
SRC=${SRC:-bench}
RUNS=${1:-6}
OUT=${OUT:-/tmp/wrc283-results.tsv}
HERE="$(cd "$(dirname "$0")" && pwd)"
q() { $B/psql $P -X -At -d "$1" -c "$2"; }

VOL="CREATE INDEX idx_trades_volume_24h ON trades (network, created_at) INCLUDE (slab_address, size) WHERE is_liquidation = false AND size IS NOT NULL"
POS="CREATE INDEX idx_trades_position_lookup_cover ON trades (trader, slab_address, asset_index, network, id) INCLUDE (side, size, is_liquidation, created_at, tx_signature)"
DROPOLD="DROP INDEX idx_trades_position_lookup"

: > "$OUT"
for run in $(seq 1 "$RUNS"); do
  # rotate arm order each run so drift on a busy host does not line up with one arm
  arms=(A B C D E); k=$(( (run-1) % 5 )); order=("${arms[@]:$k}" "${arms[@]:0:$k}")
  for arm in "${order[@]}"; do
    db="wr_${arm}_${run}"
    $B/dropdb $P --if-exists "$db"; $B/createdb $P -T "$SRC" "$db" || exit 1
    case $arm in
      B) q "$db" "$VOL" >/dev/null; q "$db" "$POS" >/dev/null ;;
      C) q "$db" "$VOL" >/dev/null; q "$db" "$POS" >/dev/null; q "$db" "$DROPOLD" >/dev/null ;;
      D) q "$db" "$VOL" >/dev/null ;;
      E) q "$db" "$POS" >/dev/null; q "$db" "$DROPOLD" >/dev/null ;;
    esac
    q "$db" "VACUUM (ANALYZE) trades" >/dev/null; q "$db" "CHECKPOINT" >/dev/null
    new_mb=$(q "$db" "select coalesce(round(sum(pg_relation_size(indexrelid))/1048576.0,1),0) from pg_index where indexrelid::regclass::text in ('idx_trades_volume_24h','idx_trades_position_lookup_cover')")
    lsn0=$(q "$db" "select pg_current_wal_lsn()")
    rm -f "$HERE"/pgbench_log.*
    (cd "$HERE" && $B/pgbench $P "$db" -n -f insert_batch.sql -t 200 -c 1 -l --random-seed=$run > /tmp/wrc283-pgb.txt 2>&1)
    lsn1=$(q "$db" "select pg_current_wal_lsn()")
    wal=$(q "$db" "select ($(echo "'$lsn1'")::pg_lsn - '$lsn0'::pg_lsn)/200")
    tps=$(awk '/^tps/ {print $3}' /tmp/wrc283-pgb.txt); lat=$(awk '/latency average/ {print $4}' /tmp/wrc283-pgb.txt)
    read p50 p95 < <(awk '{print $3/1000}' "$HERE"/pgbench_log.* | sort -n | awk '{a[NR]=$1} END {print a[int(NR*0.5)], a[int(NR*0.95)]}')
    rm -f "$HERE"/pgbench_log.*
    rows=$(q "$db" "select count(*) from trades"); [ "$rows" = 520000 ] || echo "WARN $db rows=$rows (expected 520000)" >&2
    tbl=$(q "$db" "select round(pg_table_size('trades')/1048576.0,1)"); idx=$(q "$db" "select round(pg_indexes_size('trades')/1048576.0,1)")
    printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$arm" "$run" "$tps" "$lat" "$p50" "$p95" "$wal" "$tbl" "$idx" "$new_mb" | tee -a "$OUT"
    $B/dropdb $P "$db"
  done
done
