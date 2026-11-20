\set ON_ERROR_STOP 0
\set NET '''devnet'''
\set EX '''{slab1,slab2}'''
\echo ==Q A1 queryTrades indexer-db.ts:111 (hot slab0, limit 100)
SELECT id, slab_address, trader, tx_signature, side, size::text, price::text, fee::text, created_at, asset_index FROM trades WHERE slab_address='slab0' AND network='devnet' ORDER BY created_at DESC LIMIT 100;
\echo ==Q A1b queryTrades cold slab299
SELECT id, slab_address, trader, tx_signature, side, size::text, price::text, fee::text, created_at, asset_index FROM trades WHERE slab_address='slab299' AND network='devnet' ORDER BY created_at DESC LIMIT 100;
\echo ==Q A2 CANDLE_AGG_SQL indexer-db.ts:203 (slab0, 5m bars, 7d)
SELECT t, o, h, l, c, v FROM (
  SELECT (floor(extract(epoch FROM created_at))::bigint / 300::bigint) * 300::bigint AS t,
         ((array_agg(price ORDER BY created_at, id))[1])::text AS o, max(price)::text AS h, min(price)::text AS l,
         ((array_agg(price ORDER BY created_at DESC, id DESC))[1])::text AS c, coalesce(sum(abs(size)), 0)::text AS v
    FROM trades WHERE slab_address = 'slab0' AND network = 'devnet'
     AND created_at >= now() - interval '7 days' AND created_at < now()
     AND price IS NOT NULL AND price > 0 AND price < 'Infinity'::numeric AND (size IS NULL OR abs(size) < 'Infinity'::numeric)
   GROUP BY 1 ORDER BY 1 DESC LIMIT 10000) b ORDER BY t;
\echo ==Q A2b CANDLE_AGG_SQL slab0, 1h bars, 30d
SELECT t, o, h, l, c, v FROM (
  SELECT (floor(extract(epoch FROM created_at))::bigint / 3600::bigint) * 3600::bigint AS t,
         ((array_agg(price ORDER BY created_at, id))[1])::text AS o, max(price)::text AS h, min(price)::text AS l,
         ((array_agg(price ORDER BY created_at DESC, id DESC))[1])::text AS c, coalesce(sum(abs(size)), 0)::text AS v
    FROM trades WHERE slab_address = 'slab0' AND network = 'devnet'
     AND created_at >= now() - interval '30 days' AND created_at < now()
     AND price IS NOT NULL AND price > 0 AND price < 'Infinity'::numeric AND (size IS NULL OR abs(size) < 'Infinity'::numeric)
   GROUP BY 1 ORDER BY 1 DESC LIMIT 10000) b ORDER BY t;
\echo ==Q A2c queryTradesForCandles indexer-db.ts:162 (slab0, 7d, limit 50000)
SELECT price::text, size::text, created_at FROM trades WHERE slab_address='slab0' AND network='devnet' AND created_at >= now()-interval '7 days' AND created_at <= now() ORDER BY created_at DESC LIMIT 50000;
\echo ==Q A3 queryLastTradePriceBefore indexer-db.ts:281 (slab0)
SELECT price::text FROM trades WHERE slab_address='slab0' AND network='devnet' AND created_at < now()-interval '3 days' AND price IS NOT NULL AND price > 0 ORDER BY created_at DESC LIMIT 1;
\echo ==Q A4 TRADE_FEED_SQL chart/trade-feed.ts:72 (20 slabs, after = 1h ago)
SELECT t.id::text AS id, t.slab_address, t.price::text, t.size::text, t.side, t.created_at, (extract(epoch FROM t.created_at) * 1000000)::bigint::text AS cursor_us
  FROM unnest((select array_agg('slab'||g) from generate_series(0,19) g)::text[], (select array_agg(((extract(epoch from now()-interval '1 hour'))*1000000)::bigint) from generate_series(0,19))::bigint[], (select array_agg('00000000-0000-0000-0000-000000000000'::uuid) from generate_series(0,19))::uuid[]) AS c(slab, after_us, after_id)
  CROSS JOIN LATERAL (SELECT id, slab_address, price, size, side, created_at FROM trades
     WHERE network = 'devnet' AND slab_address = c.slab
       AND created_at >= timestamptz 'epoch' + c.after_us * interval '1 microsecond'
       AND (created_at, id) > (timestamptz 'epoch' + c.after_us * interval '1 microsecond', c.after_id)
     ORDER BY created_at DESC, id DESC LIMIT 50) t;
\echo ==Q A5 queryStatsAggregate indexer-db.ts:455 (global, full scan)
SELECT COUNT(DISTINCT slab_address)::text, COUNT(DISTINCT trader)::text,
  COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours')::text,
  COALESCE(SUM(ABS(size::numeric) * price::numeric / 1e6) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours'),0)::text
 FROM trades WHERE network='devnet' AND is_liquidation IS NOT TRUE;
\echo ==Q A6 queryKnownSlabs indexer-db.ts:494
SELECT DISTINCT slab_address FROM trades WHERE network='devnet' LIMIT 100;
\echo ==Q A7a leaderboard 24h indexer-db.ts:546
SELECT trader, COUNT(*)::text, SUM(ABS(size::numeric)*price::numeric/1e6)::text, MAX(created_at) FROM trades
 WHERE created_at >= NOW()-INTERVAL '24 hours' AND network='devnet' AND is_liquidation IS NOT TRUE AND NOT (slab_address = ANY(:EX::text[]))
 GROUP BY trader ORDER BY SUM(ABS(size::numeric)*price::numeric/1e6) DESC LIMIT 50;
\echo ==Q A7b leaderboard 7d indexer-db.ts:562
SELECT trader, COUNT(*)::text, SUM(ABS(size::numeric)*price::numeric/1e6)::text, MAX(created_at) FROM trades
 WHERE created_at >= NOW()-INTERVAL '7 days' AND network='devnet' AND is_liquidation IS NOT TRUE AND NOT (slab_address = ANY(:EX::text[]))
 GROUP BY trader ORDER BY SUM(ABS(size::numeric)*price::numeric/1e6) DESC LIMIT 50;
\echo ==Q A7c leaderboard alltime indexer-db.ts:578
SELECT trader, COUNT(*)::text, SUM(ABS(size::numeric)*price::numeric/1e6)::text, MAX(created_at) FROM trades
 WHERE network='devnet' AND is_liquidation IS NOT TRUE AND NOT (slab_address = ANY(:EX::text[]))
 GROUP BY trader ORDER BY SUM(ABS(size::numeric)*price::numeric/1e6) DESC LIMIT 50;
\echo ==Q A8 queryTraderStatsAggregate indexer-db.ts:706 (heavy trader0)
SELECT count(*)::text, count(*) FILTER (WHERE side='long')::text, count(*) FILTER (WHERE side<>'long')::text,
 COALESCE(sum(trunc(abs(trunc(size::numeric))*floor(price::numeric*1000000+0.5)/1000000)),0)::bigint::text,
 COALESCE(sum(floor(fee::numeric*1000000+0.5)),0)::bigint::text, count(*) FILTER (WHERE fee>0)::text,
 count(*) FILTER (WHERE price IS NULL OR price<=0)::text, count(DISTINCT slab_address)::text, min(created_at), max(created_at)
 FROM trades WHERE trader='trader0' AND network='devnet' AND NOT (slab_address = ANY(:EX::text[]));
\echo ==Q A9 queryTraderStatsRows indexer-db.ts:748 (trader0)
SELECT side, size::text, price::text, fee::text, slab_address, created_at FROM trades WHERE trader='trader0' AND network='devnet' ORDER BY created_at ASC LIMIT 10000;
\echo ==Q A10a trader trades count indexer-db.ts:793 (trader0, no slab)
SELECT COUNT(*)::text FROM trades WHERE trader='trader0' AND network='devnet' AND NOT (slab_address = ANY(:EX::text[])) AND (NULL::text IS NULL OR side = NULL::text);
\echo ==Q A10b trader trades page indexer-db.ts:830 (trader0, offset 100)
SELECT id, slab_address, trader, side, size::text, price::text, fee::text, tx_signature, created_at, asset_index FROM trades
 WHERE trader='trader0' AND network='devnet' AND NOT (slab_address = ANY(:EX::text[])) AND (NULL::text IS NULL OR side = NULL::text) ORDER BY created_at DESC LIMIT 20 OFFSET 100;
\echo ==Q A10c trader trades page with slab filter indexer-db.ts:818 (trader0, slab0)
SELECT id, slab_address, trader, side, size::text, price::text, fee::text, tx_signature, created_at, asset_index FROM trades
 WHERE trader='trader0' AND slab_address='slab0' AND network='devnet' AND NOT (slab_address = ANY(:EX::text[])) AND (NULL::text IS NULL OR side = NULL::text) ORDER BY created_at DESC LIMIT 20 OFFSET 0;
\echo ==Q A11 queryLpVaultPosition indexer-db.ts:900
SELECT registry, market_slab, lp_shares::text FROM lp_vault_positions WHERE network='devnet' AND market_slab='slab5' AND user_wallet='user5' LIMIT 1;
\echo ==Q I1 volume_24h_by_slab StatsCollector.ts:565
SELECT * FROM volume_24h_by_slab('devnet');
\echo ==Q I2 fetchStoredLegsMany storedLegs.ts:65 (100 sigs)
SELECT id, tx_signature, slab_address, asset_index, leg_index, trader, side, size, is_liquidation FROM trades
 WHERE tx_signature IN (SELECT 'sig'||(1000+g*997+7) FROM generate_series(1,100) g) AND network='devnet' ORDER BY id LIMIT 1000;
\echo ==Q I2b fetchStoredLegs storedLegs.ts:25 (1 sig)
SELECT slab_address, asset_index, leg_index, trader, side, size, is_liquidation FROM trades WHERE tx_signature='sig5000' AND network='devnet';
\echo ==Q I3 fetchTraderPositionEvidence traderNetPosition.ts:84 (trader0)
SELECT side, size, is_liquidation, created_at, tx_signature FROM trades WHERE trader='trader0' AND slab_address='slab0' AND asset_index=0 AND network='devnet' AND tx_signature<>'x' ORDER BY id ASC LIMIT 1000;
\echo ==Q I4 LpVaultStore.loadEvents lpVault/store.ts:244
SELECT signature,ix_index,kind,slot,market_slab,lp_mint,collateral_atoms::text,lp_amount::text FROM lp_vault_events WHERE network='devnet' AND registry='reg1' AND user_wallet='user6' ORDER BY slot, signature LIMIT 1000;
\echo ==Q I5 markets keyset marketRows.ts:74
SELECT * FROM markets WHERE network='devnet' AND slab_address > 'slab150' ORDER BY slab_address LIMIT 1000;
\echo ==Q I6 refreshMarketMetadata StatsCollector.ts:464
SELECT slab_address, mainnet_ca FROM markets WHERE network='devnet' AND metadata_source='auto' AND logo_url IS NULL AND mainnet_ca IS NOT NULL LIMIT 50;
\echo ==Q I7 market_stats upsert target StatsCollector.ts:607 (300-row upsert, plan of ON CONFLICT)
EXPLAIN_ONLY_PLACEHOLDER
\echo ==Q K1 keeper register-poll markets read (keeper_status=active)
SELECT * FROM markets WHERE keeper_status='active' AND network='devnet' ORDER BY created_at LIMIT 1000;
\echo ==Q V1 v22_events by slab (no reader in repo/app; indexer writes only) slab0 newest 50
SELECT * FROM v22_events WHERE slab_address='slab0' ORDER BY block_time DESC LIMIT 50;
