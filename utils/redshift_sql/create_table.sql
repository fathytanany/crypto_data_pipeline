CREATE TABLE IF NOT EXISTS crypto_data (
    id VARCHAR(50),
    symbol VARCHAR(20),
    name VARCHAR(50),
    current_price FLOAT,
    market_cap BIGINT,
    total_volume FLOAT,        
    last_updated TIMESTAMP,
    price_change_percentage_24h FLOAT,
    ath FLOAT,
    atl FLOAT,
    PRIMARY KEY (id, last_updated)

);
