with last_time as (
    select
        max(timestamp) last_mark_time,
        symbol
    from
        dbt_traderdao.markprice
        -- public.markprice
    group by symbol
),
mark_table as (
    select
    distinct mp.symbol, mp.price as mark_price, mp.datetime as mark_time, mp.receivetime as receive_time,
    last_mark_time
    from
        dbt_traderdao.markprice mp
        -- public.markprice
    join last_time on last_time.last_mark_time = mp.timestamp
    order by mp.symbol
)
select 
    split_part(symbol, '/', 1) as symbol,
    mark_price, mark_time, receive_time
from 
    mark_table
