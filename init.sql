-- init.sql
SET timezone = 'UTC';

CREATE TABLE IF NOT EXISTS analytics_coin_prices (
    id SERIAL PRIMARY KEY,
    coin_id TEXT NOT NULL,
    price_usd NUMERIC(18, 4) NOT NULL,
    price_vnd NUMERIC(18, 4) NOT NULL,
    market_cap_usd NUMERIC(24, 2),
    total_volume_usd NUMERIC(24, 2),
    extracted_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_coin_time UNIQUE (coin_id, extracted_at)
);
-- Tối ưu hóa chỉ mục tìm kiếm chuỗi thời gian cho SRE
CREATE INDEX IF NOT EXISTS idx_coin_time ON analytics_coin_prices(coin_id, extracted_at DESC);