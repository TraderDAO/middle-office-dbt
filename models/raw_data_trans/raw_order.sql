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
    time
FROM
        {% if target.name == 'dev' %}  dbt_traderdao.orderstable 
        {% elif target.name == 'prod' %}  public.orderstable
        {% endif %}
    -- dbt_traderdao.orderstable 
    -- public.orderstable
WHERE
    executedqty > 0
order by
    time

   