INSERT INTO trades (slab_address, trader, side, size, price, fee, tx_signature, asset_index, leg_index, is_liquidation, network, size_provenance)
SELECT 'slab' || floor(300 * power(random(), 2))::int, 'trader' || floor(5000 * power(random(), 2))::int,
       (array['long','short'])[1 + (random() < 0.5)::int], round((random() * 1e9)::numeric, 0), round((10 + random() * 90)::numeric, 6),
       round((random() * 1e6)::numeric, 0), 'ing50_' || gen_random_uuid(), 0, g % 2, false, 'devnet', NULL
  FROM generate_series(1, 50) g
ON CONFLICT (tx_signature, asset_index, leg_index) DO NOTHING RETURNING tx_signature, asset_index, leg_index;
