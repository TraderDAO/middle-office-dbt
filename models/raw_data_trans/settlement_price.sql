select
    split_part(symbol, '/', 1) as symbol,
    price as settlement_price,
    timestamp as settlement_timestamp,
    datetime as settlement_time,
    receivetime as receive_time
from
    -- dbt_traderdao.settlementprice 
    -- public.settlementprice
        {% if target.name == 'dev' %}  dbt_traderdao.settlementprice  
        {% elif target.name == 'prod' %}  public.settlementprice
        {% endif %}