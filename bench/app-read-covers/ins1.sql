\set a random(0, 999999)
\set b random(0, 999999)
\set slab int(300 * pow(:a / 1000000.0, 2))
\set trader int(5000 * pow(:b / 1000000.0, 2))
\set sz random(1, 1000000000)
\set px random(10000000, 100000000)
\set fee random(0, 1000000)
\set side random(0, 1)
INSERT INTO trades (slab_address, trader, side, size, price, fee, tx_signature, asset_index, leg_index, is_liquidation, network, size_provenance)
VALUES ('slab' || :slab, 'trader' || :trader, CASE WHEN :side = 0 THEN 'long' ELSE 'short' END, :sz, :px / 1e6, :fee,
        'ing1_' || :client_id || '_' || gen_random_uuid(), 0, 0, false, 'devnet', NULL)
ON CONFLICT (tx_signature, asset_index, leg_index) DO NOTHING RETURNING tx_signature, asset_index, leg_index;
