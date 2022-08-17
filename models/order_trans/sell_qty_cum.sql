    select
        orderid,
        sum(sell_qty) over (
            PARTITION By symbol 
            ORDER By time
        ) as sell_qty_cum
    from
        {{ ref('order_with_qty') }}
