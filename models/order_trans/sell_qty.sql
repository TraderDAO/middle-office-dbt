    SELECT
        symbol,
        time,
        orderid,
        executedqty as sell_qty
    FROM
        {{ ref('raw_order') }}
    WHERE
        side = 'sell'