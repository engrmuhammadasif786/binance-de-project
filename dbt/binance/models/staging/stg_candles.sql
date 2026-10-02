with source as (
    select * from {{ source('raw', 'raw_candles') }}
)
select
    symbol,
    cast(open_time as timestamp) as open_time,
    cast(open_date as date) as open_date,
    open, high, low, close, volume, quote_volume, trades
from source
