with candles as (
    select * from {{ ref('stg_candles') }}
),
metrics as (
    select
        symbol,
        open_date,
        close as closing_price,
        volume,
        quote_volume,
        trades,
        (close - open) / nullif(open, 0) * 100 as daily_return_pct,
        (high - low) / nullif(close, 0) * 100 as intraday_range_pct
    from candles
)
select
    *,
    stddev(daily_return_pct) over (
        partition by symbol order by open_date
        rows between 6 preceding and current row
    ) as volatility_7d
from metrics
