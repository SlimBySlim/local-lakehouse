{{
    config(
        materialized='incremental',
        unique_key='trading_date'
    )
}}

select
    date(timestamp) as trading_date,

    min(low) as daily_low,
    max(high) as daily_high,

    min_by(open, timestamp) as daily_open,
    max_by(close, timestamp) as daily_close,

    avg(close) as avg_close_price,
    sum(volume) as total_volume,
    count(*) as candle_count

from {{ ref('stg_btc_klines') }}

{% if is_incremental() %}
where date(timestamp) > (
    select coalesce(max(trading_date), date '1900-01-01')
    from {{ this }}
)
{% endif %}

group by 1