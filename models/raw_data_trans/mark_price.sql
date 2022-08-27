with last_time as (
    select
        max(timestamp) last_mark_time,
        symbol
    from
        {% if target.name == 'dev' %} dbt_traderdao.markprice mp
        {% elif target.name == 'prod' %} public.markprice mp
        {% endif %}
    group by symbol
),
mark_table as (
    select
        distinct mp.symbol, mp.price as mark_price, mp.datetime as mark_time, mp.receivetime as receive_time,
        last_mark_time
    from
        {% if target.name == 'dev' %} dbt_traderdao.markprice mp
        {% elif target.name == 'prod' %} public.markprice mp
        {% endif %}
    join last_time on last_time.last_mark_time = mp.timestamp
    order by mp.symbol
)
select 
    split_part(symbol, '/', 1) as symbol,
    mark_price, mark_time, receive_time
from 
    mark_table
