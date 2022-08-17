    select
        orderid,
        sum(buy_qty) over (
            PARTITION By symbol
            ORDER By time
        ) as buy_qty_cum
    from
        {{ ref('order_with_qty') }}
