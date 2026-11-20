# Sourced by the bench scripts. Override with COV_DIR / COV_PORT / PG / REPO.
PG=${PG:-/opt/homebrew/opt/postgresql@16/bin}; D=${COV_DIR:-/tmp/cov-bench}; PORT=${COV_PORT:-55477}
S=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd); R=${REPO:-$(cd "$S/../.." && pwd)}
export LC_ALL=en_US.UTF-8 COV_DIR=$D COV_PORT=$PORT
px() { local db=$1; shift; "$PG/psql" -h "$D" -p "$PORT" -U postgres -d "$db" -X -q -v ON_ERROR_STOP=1 "$@"; }
