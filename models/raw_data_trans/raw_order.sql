
    
    
    SELECT
        orderid,
        side,
        symbol,
        price,
        cost,
        executedqty,
        time
    FROM
        dbt_traderdao.orderstable -- portfolio
    WHERE
        executedqty > 0
    order by time