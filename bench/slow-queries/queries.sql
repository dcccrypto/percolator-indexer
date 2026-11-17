-- SQL equivalents of the indexer's recurring reads (PostgREST shapes). One statement per label.
-- Q1 StatsCollector.syncVolumeForAllDBMarkets (every cycle): the RPC itself
\echo Q1 volume_24h_by_slab
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY ON) SELECT * FROM volume_24h_by_slab('devnet');
-- Q2 fetchStoredLegsMany: 100 signatures from a polled window, mix of stored and new
\echo Q2 stored legs IN(100)
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY ON)
SELECT tx_signature, slab_address, asset_index, leg_index, trader, side, size, is_liquidation
FROM trades WHERE tx_signature IN (SELECT 'sig'||(1000+g*997+7) FROM generate_series(1,100) g) AND network='devnet';
-- Q3 fetchTraderPositionEvidence (per close): heavy trader, one position
\echo Q3 position evidence
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY ON)
SELECT side, size, is_liquidation, created_at, tx_signature FROM trades
WHERE trader='trader0' AND slab_address='slab0' AND asset_index=0 AND network='devnet' AND tx_signature<>'x'
ORDER BY id ASC LIMIT 1000 OFFSET 0;
-- Q4 LpVaultStore.loadEvents
\echo Q4 lp events
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY ON)
SELECT signature,ix_index,kind,slot,market_slab,lp_mint,collateral_atoms::text,lp_amount::text FROM lp_vault_events
WHERE network='devnet' AND registry='reg1' AND user_wallet='user5' ORDER BY slot LIMIT 10000;
-- Q5 market_stats upsert target / markets keyset read
\echo Q5 markets keyset
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY ON)
SELECT * FROM markets WHERE network='devnet' ORDER BY slab_address LIMIT 1000;
