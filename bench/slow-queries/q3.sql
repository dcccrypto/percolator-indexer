\set t random(0,30)
SELECT side, size, is_liquidation, created_at, tx_signature FROM trades WHERE trader='trader'||:t AND slab_address='slab'||((:t % 6)) AND asset_index=(:t % 2) AND network='devnet' AND tx_signature<>'x' ORDER BY id ASC LIMIT 1000 OFFSET 0;
