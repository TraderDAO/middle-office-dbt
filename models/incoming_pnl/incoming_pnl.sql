with last_incoming_time as (
    select
        symbol,
        max(settlement_timestamp) last_settlement_time
    from
        {{ ref('incoming_pnl_historical') }}
    group by
        symbol
)

select 
    distinct sp.*
from 
   {{ ref('incoming_pnl_historical') }}  sp
join 
    last_incoming_time on last_incoming_time.last_settlement_time = sp.settlement_timestamp