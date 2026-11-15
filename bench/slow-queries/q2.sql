\set s random(1,400)
SELECT tx_signature, slab_address, asset_index, leg_index, trader, side, size, is_liquidation FROM trades WHERE tx_signature IN (SELECT 'sig'||(1000+g*997+:s) FROM generate_series(1,100) g) AND network='devnet';
