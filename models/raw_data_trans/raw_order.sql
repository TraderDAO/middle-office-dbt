SELECT
    orderid,
    symbol,
    base,
    side,
    price,
    amount,
    cost,
    executedqty,
    remaining,
    TIME,
    datetime
FROM
    {% if target.name == 'dev' %}
        dbt_traderdao.orderstable {% elif target.name == 'prod' %}
        PUBLIC.orderstable
    {% endif %}
WHERE
    executedqty > 0
    AND symbol != 'USDC'
