CREATE TABLE IF NOT EXISTS crypto_data (
    id VARCHAR(50),
    symbol VARCHAR(20),
    name VARCHAR(50),
    current_price FLOAT,
    market_cap BIGINT,
    last_updated TIMESTAMP
);
