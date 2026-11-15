-- One indexer trade batch: 100 fills in one INSERT ... ON CONFLICT DO NOTHING (insertTradeRows shape).
-- Columns are exactly toDbRow(): no id, no created_at (default now()). Mix: ~30k skewed traders, 12 slabs
-- (network follows the slab like seed.sql), 2 assets, ~1% null size / null side, ~1% liquidation markers.
SELECT nextval('wr_seq') AS b \gset
INSERT INTO trades (slab_address, trader, side, size, price, fee, tx_signature, asset_index, leg_index, is_liquidation, size_provenance, network)
SELECT 'slab'||(g%12), 'trader'||floor(30000*power(random(),3))::int,
  CASE WHEN g%97=0 THEN NULL WHEN random()<0.5 THEN 'long' ELSE 'short' END,
  CASE WHEN g%97=0 THEN NULL ELSE (random()*1e9)::bigint END, 100+random(), 0,
  'wsig'||:b||'_'||(g/2), (g%2), 0, (g%101=0), 'matcher_exact',
  CASE WHEN (g%12)<6 THEN 'devnet' ELSE 'mainnet' END
FROM generate_series(1,100) g
ON CONFLICT (tx_signature, asset_index, leg_index) DO NOTHING;
