select
    symbol,
    orderid,
    cost,
    time,
    case
        WHEN side = 'sell' THEN cost
        ELSE 0
    END sell_cost
FROM
    {{ ref('raw_order') }}