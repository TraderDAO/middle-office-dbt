  
    SELECT
        orderid,
        side,
        symbol,
        price,
        cost,
        executedqty,
        time
    FROM
        dbt_traderdao.orderstable
        -- public.orderstable
    WHERE
        executedqty > 0
    order by time