Local-only bench for the trader position lookup (fetchTraderPositionEvidence). Needs a throwaway Postgres 16.

    initdb -D /tmp/pg && pg_ctl -D /tmp/pg -o "-p 55432 -k /tmp" start && createdb -h /tmp -p 55432 bench
    psql -h /tmp -p 55432 bench -f seed.sql      # 100k trades, ~10k traders, 8 slabs
    ./run.sh                                     # p50/p95 over 200 requests + md5 of the response rows
    # then create the index from supabase/migrations/20261114130000_*.sql (without CONCURRENTLY) and run ./run.sh again
