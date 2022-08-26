with last_time as (
    select
        symbol,
        max(time) last_trading_time
    from
        {{ ref('trading_pnl_by_order') }}
    group by
        symbol
)

select 
    distinct tpnl.*
from 
   {{ ref('trading_pnl_by_order') }} tpnl
join 
    last_time on last_time.last_trading_time = tpnl.time and last_time.symbol = tpnl.symbol