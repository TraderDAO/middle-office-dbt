

with last_time as (
    select
        max(timestamp) last_settlement_time,
        symbol
    from
        {% if target.name == 'dev' %} dbt_traderdao.settlementprice sp
        {% elif target.name == 'prod' %} public.settlementprice sp
        {% endif %}
    group by symbol
)

    select
        distinct split_part(sp.symbol, '/', 1) as symbol,
        sp.price as settlement_price,
        sp.timestamp as settlement_timestamp,
        sp.datetime as settlement_time,
        sp.receivetime as receive_time,
        last_time.last_settlement_time
    from
        {% if target.name == 'dev' %} dbt_traderdao.settlementprice sp
        {% elif target.name == 'prod' %} public.settlementprice sp
        {% endif %}
    join last_time on last_time.last_settlement_time = sp.timestamp
