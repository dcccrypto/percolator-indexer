#!/bin/bash
# t0 = schema from all migrations except 20261115 and 20261120 (both stand-ins), + report data set (same generator + setseed(0.42))
. "$(dirname "$0")/env.sh"
db=t0; date +%T
$PG/dropdb -h $D -p $PORT -U postgres --if-exists $db; $PG/createdb -h $D -p $PORT -U postgres $db
px $db -f $S/stub.sql >/dev/null 2>&1
for f in $(ls $R/supabase/migrations/*.sql | sort); do b=$(basename $f)
  case $b in 20261115120000*|20261120120000*) echo "skip $b"; continue;; esac
  px $db --single-transaction -f $f >$D/t0-$b.log 2>&1 || echo "ERR $b"; done
NN="main""net"; CA="${NN}_ca"
px $db -At -c "select indexdef||';' from pg_indexes where tablename='trades' and indexname<>'trades_pkey'" > $D/t0-idx-defs.sql
cat $D/t0-idx-defs.sql
px $db -c "insert into markets(slab_address,mint_address,symbol,name,deployer,network,status,$CA,keeper_status,metadata_source,created_at)
 select 'slab'||g,'mint'||g,'SYM'||g,'Name '||g,'dep'||(g%20), case when g%30=0 then '$NN' else 'devnet' end,
 (array['active','active','active','paused','closed'])[1+g%5], case when g%3=0 then 'ca'||g end,
 (array['active','pending'])[1+(g%7=0)::int], 'auto', now()-interval '40 days' from generate_series(0,299) g"
px $db -c "insert into market_stats(slab_address,volume_24h,trade_count_24h,network) select slab_address,0,0,network from markets"
for i in $(px $db -At -c "select indexname from pg_indexes where tablename='trades' and indexname<>'trades_pkey'"); do px $db -c "drop index $i"; done
px $db -f $S/trades-copy.sql | px $db -c "COPY trades(id,slab_address,trader,side,size,price,fee,tx_signature,asset_index,leg_index,is_liquidation,network,created_at,size_provenance) FROM STDIN"
px $db -At -c "select 'trades', count(*) from trades"
px $db -f $D/t0-idx-defs.sql
px $db -c "COPY (
 select g, 'vsig'||g, 0, -1, (array['bond_deposit','bond_withdraw_request','bond_withdraw_execute','rescue_deposit','insurance_units_init','backstop_propose','backstop_draw','backstop_restore','holding_rent_settled','band_dust_swept','eviction'])[1+floor(11*power(random(),1.5))::int],
   'slab'||floor(300*power(random(),2))::int, 0, 'actor'||floor(5000*power(random(),2))::int, 'subj'||g, round((random()*1e9)::numeric,0), '{}', g, now()-random()*interval '30 days', 'devnet', now()
 from generate_series(1,50000) g) TO STDOUT" | px $db -c "COPY v22_events(id,signature,ix_index,inner_index,kind,slab_address,asset_index,actor,subject,amount,detail,slot,block_time,network,indexed_at) FROM STDIN"
px $db -c "insert into lp_vault_events(signature,ix_index,network,kind,program_id,market_slab,registry,user_wallet,lp_mint,collateral_atoms,lp_amount,slot,block_time)
 select 'lsig'||g,0,'devnet',(array['deposit','request_redeem','cancel_redeem','execute_redeem'])[1+g%4],'prog','slab'||(g%300),'reg'||(g%5),'user'||(g%2000),'mint',g*1000,g*900,g,now() from generate_series(1,20000) g"
px $db -c "VACUUM (ANALYZE)"
px $db -At -c "select count(*), count(*) filter (where created_at>=now()-interval '24 hours') from trades"
date +%T; echo T0_DONE
