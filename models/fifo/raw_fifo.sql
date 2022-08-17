SELECT
    orderid,
    symbol,
    time,
    qty_sold, 
    case
        when qty_sold = 0 then -1
        else round((cum_sold_cost - coalesce(lag(cum_sold_cost) over w, 0))/qty_sold, 2)
    end fifo_price,
    qty_bought,
    prev_bought,
    total_cost,
    prev_total_cost,
    cum_sold_cost,
    coalesce(lag(cum_sold_cost) over w, 0) as prev_cum_sold_cost
FROM
    {{ ref('cum_sold_cost') }}
WINDOW w AS (PARTITION BY symbol ORDER BY time)