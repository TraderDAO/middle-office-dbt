select
    order_with_qty.orderid,
    order_with_qty.symbol,
    order_with_qty.side,
    order_with_qty.price,
    order_with_qty.cost,
    order_with_qty.time,
    -- order_with_qty.sell_qty,
    -- order_with_qty.buy_qty,
    order_with_qty.mark_price,
    order_with_qty.executedqty,
    buyqtycum.buy_qty_cum,
    sellqtycum.sell_qty_cum,
    buycost.buy_cost_cum,
    sellcost.sell_cost_cum,
    buycost.buy_cost_cum - sellcost.sell_cost_cum as total_cost,
    buyqtycum.buy_qty_cum - sellqtycum.sell_qty_cum as total_qty,
    buyqtycum.buy_qty_cum - sellqtycum.sell_qty_cum as incomming_qty,
    (order_with_qty.mark_price * (buyqtycum.buy_qty_cum - sellqtycum.sell_qty_cum)) - (buycost.buy_cost_cum - sellcost.sell_cost_cum) as unrealizedPnL,
    -- case
    --     WHEN cum_sold_cost > 0 THEN cum_sold_cost
    --     ELSE 0
    -- END 
    cum_sold_cost

FROM {{ ref('order_with_qty') }}
LEFT JOIN {{ ref('buy_qty_cum') }} buyqtycum on buyqtycum.orderid = order_with_qty.orderid
LEFT JOIN {{ ref('sell_qty_cum') }} sellqtycum on sellqtycum.orderid = order_with_qty.orderid
LEFT JOIN {{ ref('buy_cost_cum') }} buycost on buycost.orderid = order_with_qty.orderid
LEFT JOIN {{ ref('sell_cost_cum') }} sellcost on sellcost.orderid = order_with_qty.orderid
LEFT JOIN {{ ref('cum_sold_cost') }} cumcost on cumcost.orderid = order_with_qty.orderid