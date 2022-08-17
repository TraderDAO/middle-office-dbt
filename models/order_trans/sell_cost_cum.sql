select
    symbol,
    orderid,
    sell_cost,
    time,
    sum(sell_cost) over (
        PARTITION by symbol
        order by time
    ) as sell_cost_cum
FROM
    {{ ref('sell_cost') }}