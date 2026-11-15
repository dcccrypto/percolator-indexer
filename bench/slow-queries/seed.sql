-- Seeds the schema built from supabase/migrations/*.sql (apply those first; see README.md).
-- LOCAL THROWAWAY DB ONLY. Rows are labelled with both CHECK-allowed network values only so the
-- network filter has something to exclude; nothing here touches a real network.
-- 1.2M trades over 30 days, 12 markets, ~30k traders (skewed), plus lp_vault rows.
SELECT setseed(0.42);
INSERT INTO markets (slab_address, network, mint_address, deployer)
SELECT 'slab'||g, CASE WHEN g<6 THEN 'devnet' ELSE 'mainnet' END, 'mint'||g, 'dep' FROM generate_series(0,11) g;

INSERT INTO trades(slab_address,trader,side,size,price,tx_signature,asset_index,leg_index,is_liquidation,network,created_at)
SELECT 'slab'||(g%12), 'trader'||floor(30000*power(random(),3))::int,
  CASE WHEN g%97=0 THEN NULL WHEN random()<0.5 THEN 'long' ELSE 'short' END,
  CASE WHEN g%97=0 THEN NULL ELSE (random()*1e9)::bigint END, 100+random(),
  'sig'||g, (g%2), 0, (g%97=0), CASE WHEN (g%12)<6 THEN 'devnet' ELSE 'mainnet' END,
  now() - (g*2.16||' seconds')::interval          -- 1.2M rows * 2.16s = 30 days
FROM generate_series(1,1200000) g;

-- lp vault: 200 users with 40 events each
INSERT INTO lp_vault_events (signature, ix_index, network, kind, program_id, market_slab, registry, user_wallet, lp_mint, collateral_atoms, lp_amount, domain, slot, block_time)
SELECT 'lpsig'||g||'_'||e, 0, 'devnet', CASE WHEN e%3=0 THEN 'request_redeem' ELSE 'deposit' END, 'prog', 'slab'||(g%6), 'reg'||(g%4), 'user'||g, 'mint', 1000+e, 1000+e, 1, 1000000+e*10+g, now()
FROM generate_series(1,200) g, generate_series(1,40) e;
ANALYZE;
