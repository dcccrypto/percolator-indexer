\set t random_zipfian(1, 10000, 1.05)
\set s random(0,7)
\set a random(0,1)
SELECT side, size, is_liquidation, created_at, tx_signature FROM trades WHERE trader='trader'||:t AND slab_address='slab'||:s AND asset_index=:a AND network='devnet' AND tx_signature<>'x' ORDER BY id ASC LIMIT 1000 OFFSET 0;
