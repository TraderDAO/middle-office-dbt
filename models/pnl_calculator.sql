WITH symbol_PnL as (
    select
        *
    from
       {{ ref('raw_pnl') }}
)
select
    symbol,
    time,
    avg_bought_price,
    avg_sold_price,
    unrealizedPnL as unrealized_pnl,
    realized_PnL
from
    symbol_PnL
ORDER BY time DESC LIMIT 1