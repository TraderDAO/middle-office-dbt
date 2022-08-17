select
    *,
    sell_cost_cum - (
        case
            when cum_sold_cost = 0 THEN sum(cum_sold_cost) over (
                PARTITION by symbol
                order by
                    time
            )
            else cum_sold_cost
        end
    ) as realized_pnl,
    case when total_qty = 0 then 0 else 
    ROUND((total_cost + (sell_cost_cum - (
        case
            when cum_sold_cost = 0 THEN sum(cum_sold_cost) over (
                PARTITION by symbol
                order by
                    time
            )
            else cum_sold_cost
        end
    ))) / total_qty, 6) end avg_bought_price,
    case
        WHEN sell_qty_cum > 0 THEN ROUND(sell_cost_cum / sell_qty_cum, 6)
        ELSE 0
    END avg_sold_price
from
    {{ ref('order_with_qtycum') }}