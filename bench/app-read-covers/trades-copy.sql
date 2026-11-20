select setseed(0.42) \g /dev/null
COPY (
 select gen_random_uuid(), 'slab'||floor(300*power(random(),2))::int, 'trader'||floor(5000*power(random(),2))::int,
   case when liq then null else (array['long','short'])[1+(random()<0.5)::int] end,
   case when liq then null else round((random()*1e9)::numeric,0) end,
   case when liq then null else round((10+random()*90)::numeric,6) end,
   case when liq then 0 else round((random()*1e6)::numeric,0) end,
   'sig'||(g/2), (g%3)::smallint, (g%2)::smallint, liq, 'devnet', now()-random()*interval '30 days', null
 from (select g, random()<0.01 as liq from generate_series(1,250000) g) s ) TO STDOUT;
