select
    raworder.orderid,
    raworder.symbol,
    raworder.side,
    raworder.price,
    raworder.cost,
    raworder.time,
    markprice.mark_price,
    case WHEN buyqty.buy_qty > 0 THEN buyqty.buy_qty
    ELSE 0
    END buy_qty,
    case WHEN sellqty.sell_qty > 0 THEN sellqty.sell_qty
    ELSE 0
    END sell_qty

from {{ ref('raw_order') }} raworder
JOIN {{ ref('mark_price') }} markprice on raworder.symbol = markprice.symbol
LEFT JOIN {{ ref('buy_qty') }} buyqty on buyqty.orderid = raworder.orderid
LEFT JOIN {{ ref('sell_qty') }} sellqty on sellqty.orderid = raworder.orderid