select
    timestamp,
    symbol,
    category,
    interval,
    open,
    high,
    low,
    close,
    volume,
    turnover
from {{ source('crypto_raw', 'btcusdt_klines') }}