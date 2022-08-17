SELECT
    symbol,
    orderid,
    cost,
    time,
    case
        WHEN side = 'buy' THEN cost
        ELSE 0
    END buy_cost
FROM
    {{ ref('raw_order') }}