with daily_order as (
    select 
        *,
        case when side = 'sell' then executedqty else 0 end sell_qty,
        case when side = 'buy' then executedqty else 0 end buy_qty,
        case WHEN side = 'sell' THEN cost ELSE 0 END sell_cost,
        case WHEN side = 'buy' THEN cost ELSE 0 END buy_cost
    from
        {{ ref('trading_table') }}
),

daily_order_cum as (
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
        daily_order
),
last_time as (
    select symbol, max(time) lasttime
    from daily_order_cum
    group by symbol
)

select 
    orderid,
    time,
    daily_order_cum.symbol,
    side,
    price,
    executedqty,
    cost,
    buy_qty_cum,
    buy_cost_cum,
    sell_qty_cum,
    sell_cost_cum,
    case when buy_cost_cum > 0 then Round(buy_cost_cum/ buy_qty_cum, 6) else 0 end daily_avg_bought_price,
    case when sell_cost_cum > 0 then Round(sell_cost_cum/ sell_qty_cum, 6) else 0 end daily_avg_sold_price
from 
    daily_order_cum
join
    last_time on last_time.lasttime = daily_order_cum.time and last_time.symbol = daily_order_cum.symbol

