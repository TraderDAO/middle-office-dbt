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
        PUBLIC.orderstable {% elif target.name == 'prod' %}
        PUBLIC.orderstable
    {% endif %}
WHERE
    executedqty > 0
    AND symbol != 'USDC'
