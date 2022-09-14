SELECT
    distinct orderid,
    symbol,
    base,
    side,
    price,
    amount,
    cost,
    executedqty,
    remaining,
    time,
    datetime
FROM
    {% if target.name == 'dev' %}  dbt_traderdao.orderstable 
    {% elif target.name == 'prod' %}  public.orderstable
    {% endif %}
WHERE
    executedqty > 0 and symbol != 'USDC'


   