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
    dbt_traderdao.orderstable -- public.orderstable
WHERE
    executedqty > 0
order by
    time