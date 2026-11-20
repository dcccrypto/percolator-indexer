\echo Q1
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, COSTS OFF) SELECT t.slab_address, COALESCE(SUM(ABS(t.size)),0)::numeric, COUNT(*)::bigint FROM trades t WHERE t.network='devnet' AND t.is_liquidation=false AND t.size IS NOT NULL AND t.created_at >= NOW() - INTERVAL '24 hours' GROUP BY t.slab_address;
\echo Q2
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, COSTS OFF) SELECT tx_signature, slab_address, asset_index, leg_index, trader, side, size, is_liquidation FROM trades WHERE tx_signature IN (SELECT 'sig'||(1000+g*997+7) FROM generate_series(1,100) g) AND network='devnet';
\echo Q3
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, COSTS OFF) SELECT side, size, is_liquidation, created_at, tx_signature FROM trades WHERE trader='trader0' AND slab_address='slab0' AND asset_index=0 AND network='devnet' AND tx_signature<>'x' ORDER BY id ASC LIMIT 1000 OFFSET 0;
