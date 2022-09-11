with raw_cum_bought_cost as (
    select
        *,
        case
            when cum_bought_cost is null THEN 0
            else 1
        end rawCumSoldCost
    from
        {{ ref('order_with_unrealized_pnl') }}
),
cum_bought_cost_par as (
    select
        *,
        sum(rawCumSoldCost) over (
            PARTITION By symbol
            ORDER By
                time
        ) cumSoldCostPar
    from
        raw_cum_bought_cost
),
real_cum_bought_cost as (
    select
    *,
    first_value(cum_bought_cost) over(PARTITION By symbol, cumSoldCostPar) realCumSoldCost
    from
    cum_bought_cost_par
),
adj_cum_bought_cost as (
    SELECT *, case when realCumSoldCost is null then 0 else realCumSoldCost end adj_cum_bought_cost
    from real_cum_bought_cost
),

realized as (
    select
        distinct *,
        sell_cost_cum - adj_cum_bought_cost as realized_pnl,
        case when total_qty = 0 then 0 else 
            ROUND((total_cost + (sell_cost_cum - adj_cum_bought_cost)) / total_qty, 6) 
        end avg_bought_price,
        case
            WHEN sell_qty_cum > 0 THEN ROUND(sell_cost_cum / sell_qty_cum, 6)
            ELSE 0
        END avg_sold_price
    from adj_cum_bought_cost
    order by time
)

select 
    *, 
    unrealized - realized_pnl as unrealizedpnl,
    sum(realized_pnl) over ( PARTITION by symbol order by time) as realized
from 
    realized