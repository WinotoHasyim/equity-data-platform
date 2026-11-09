select *
from {{ ref('daily_bars') }}
where
    open is null
    or high is null
    or low is null
    or close is null
    or vwap is null
    or batch_key is null
    or source is null
    or dataset is null
    or feed is null
    or timeframe is null
    or adjustment is null
    or schema_version is null
    or open <= 0
    or high <= 0
    or low <= 0
    or close <= 0
    or vwap <= 0
    or high != greatest(open, high, low, close)
    or low != least(open, high, low, close)
    or vwap < low
    or vwap > high
    or volume < 0
    or trade_count < 0
    or source != 'alpaca'
    or dataset != 'daily_bars'
    or feed != 'iex'
    or timeframe != '1_day'
    or adjustment != 'raw'
    or schema_version != 1
