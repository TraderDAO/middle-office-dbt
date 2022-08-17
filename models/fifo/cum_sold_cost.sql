SELECT
    orderid,
    tneg.symbol,
    time,
    qty_sold,
    cum_sold,
    tpos.qty_bought,
    buy_order_id,
    prev_bought,
    total_cost,
    prev_total_cost,
    round(prev_total_cost + ((tneg.cum_sold - tpos.prev_bought)/(tpos.qty_bought - tpos.prev_bought))*(total_cost-prev_total_cost), 2) as cum_sold_cost
FROM {{ ref('sell_order') }} tneg
LEFT JOIN {{ ref('buy_order') }} tpos 
on (tneg.cum_sold BETWEEN tpos.prev_bought AND tpos.qty_bought) AND tpos.symbol = tneg.symbol