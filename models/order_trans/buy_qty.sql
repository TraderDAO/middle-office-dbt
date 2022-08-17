    SELECT
        symbol,
        time,
        orderid,
        executedqty as buy_qty
    FROM
        {{ ref('raw_order') }}
    WHERE
        side = 'buy'

