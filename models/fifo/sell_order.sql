SELECT
    orderid,
    symbol,
    time,
    executedqty as qty_sold,
    sum(executedqty) over w as cum_sold
FROM
    {{ ref('raw_order') }}  --dbt_orangesky.orderstable
WHERE
    side = 'sell' 
WINDOW w AS ( PARTITION BY symbol ORDER BY time)