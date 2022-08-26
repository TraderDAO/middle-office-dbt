select
    raworder.*,
    case when raworder.side = 'sell' then executedqty else 0 end sell_qty,
    case when raworder.side = 'buy' then executedqty else 0 end buy_qty,
    case WHEN raworder.side = 'sell' THEN cost ELSE 0 END sell_cost,
    case WHEN raworder.side = 'buy' THEN cost ELSE 0 END buy_cost,
    markprice.mark_price,
    markprice.mark_time,
    markprice.receive_time

from {{ ref('raw_order') }} raworder
JOIN {{ ref('mark_price') }} markprice on raworder.symbol = markprice.symbol