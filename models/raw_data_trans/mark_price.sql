with last_time as (
    select
        max(timestamp) lasttime
    from
        dbt_traderdao.markprice
        -- public.markprice

)
select
    distinct symbol,
    price as mark_price,
    timestamp as mark_time,
    lasttime
from
    dbt_traderdao.markprice
    -- public.markprice
    join last_time on last_time.lasttime = timestamp