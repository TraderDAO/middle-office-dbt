select
    symbol,
    orderid,
    buy_cost,
    time,
    sum(buy_cost) over (
        PARTITION by symbol
        order by time
    ) as buy_cost_cum
FROM
   {{ ref('buy_cost') }}