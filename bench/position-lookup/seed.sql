CREATE TABLE trades (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(), slab_address TEXT NOT NULL, trader TEXT NOT NULL,
  side TEXT, size NUMERIC, price NUMERIC, fee NUMERIC DEFAULT 0, tx_signature TEXT,
  asset_index SMALLINT NOT NULL DEFAULT 0, leg_index SMALLINT NOT NULL DEFAULT 0,
  is_liquidation BOOLEAN NOT NULL DEFAULT false, size_provenance TEXT,
  network TEXT NOT NULL DEFAULT 'devnet', created_at TIMESTAMPTZ DEFAULT NOW());
CREATE UNIQUE INDEX uq_trades_sig_asset_leg ON trades(tx_signature, asset_index, leg_index);
CREATE INDEX idx_trades_slab ON trades(slab_address, created_at DESC);
CREATE INDEX idx_trades_trader_created ON trades(trader, created_at DESC);
CREATE INDEX idx_trades_network ON trades(network, slab_address, created_at DESC);
CREATE INDEX brin_trades_created_at ON trades USING BRIN (created_at);
-- 100k trades, 10k traders, 8 slabs, 2 assets; skewed so a few traders are heavy
INSERT INTO trades(slab_address,trader,side,size,price,tx_signature,asset_index,leg_index,is_liquidation,network,created_at)
SELECT 'slab'||(g%8), 'trader'||floor(10000*power(random(),3))::int,
 CASE WHEN g%97=0 THEN NULL WHEN random()<0.5 THEN 'long' ELSE 'short' END,
 (random()*1e9)::bigint, 100+random(), 'sig'||g, (g%2), 0, (g%97=0), 'devnet',
 now() - (g||' seconds')::interval
FROM generate_series(1,100000) g;
ANALYZE trades;
