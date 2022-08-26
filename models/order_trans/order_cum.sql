select
    *,
    sum(buy_qty) over (
        PARTITION By symbol
        ORDER By
            time
    ) as buy_qty_cum,
    sum(sell_qty) over (
        PARTITION By symbol
        ORDER By
            time
    ) as sell_qty_cum,
    sum(sell_cost) over (
        PARTITION by symbol
        order by
            time
    ) as sell_cost_cum,
    sum(buy_cost) over (
        PARTITION by symbol
        order by time
    ) as buy_cost_cum
from
    {{ ref('orders') }}