select
    distinct orderscum.*,
    orderscum.buy_cost_cum - orderscum.sell_cost_cum as total_cost,
    orderscum.buy_qty_cum - orderscum.sell_qty_cum as total_qty,
    (orderscum.mark_price * (orderscum.buy_qty_cum - orderscum.sell_qty_cum)) - (orderscum.buy_cost_cum - orderscum.sell_cost_cum) as unrealizedPnL,
    -- case
    --     WHEN cum_sold_cost > 0 THEN cum_sold_cost
    --     ELSE 0
    -- END 
   cumcost.cum_sold_cost

FROM {{ ref('order_cum') }} orderscum
LEFT JOIN {{ ref('cum_sold_cost') }} cumcost on cumcost.orderid = orderscum.orderid

order by time