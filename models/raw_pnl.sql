WITH raw_symbol as (
    SELECT
        orderid,
        side,
        symbol,
        price,
        cost,
        executedqty,
        time
    FROM
        sbinancedata.portfolio -- orderstable
    WHERE
        executedqty > 0
    Order By
        time
),
markprice as(
    select
        price as mark_price,
        timestamp "Mark Time"
    from
        sbinancedata.markprice
    ORDER BY
        timestamp DESC
    LIMIT
        1
), buyqty as (
    SELECT
        time,
        orderid,
        executedqty as buyqty
    FROM
        raw_symbol
    WHERE
        side = 'buy'
),
sellqty as (
    SELECT
        time,
        orderid,
        executedqty as sellqty
    FROM
        raw_symbol
    WHERE
        side = 'sell'
),
symbol_with_qty as (
    SELECT
        raw_symbol.orderid,
        raw_symbol.symbol,
        raw_symbol.side,
        raw_symbol.price,
        raw_symbol.executedqty,
        raw_symbol.cost,
        raw_symbol.time,
        markprice.mark_price,
        CASE
            WHEN buyqty.buyqty > 0 THEN buyqty.buyqty
            ELSE 0
        END buyqty,
        CASE
            WHEN sellqty.sellqty > 0 THEN sellqty.sellqty
            ELSE 0
        END sellqty
    FROM
        raw_symbol
        LEFT JOIN markprice on raw_symbol.orderid IS NOT NULL
        LEFT JOIN buyqty on buyqty.orderid = raw_symbol.orderid
        LEFT JOIN sellqty on sellqty.orderid = raw_symbol.orderid
),
buyqty_cum as (
    select
        orderid,
        buyqty,
        time,
        sum(buyqty) over (
            order by
                time
        ) as buyqty_cum
    from
        symbol_with_qty
),
sellqty_cum as (
    select
        orderid,
        buyqty,
        time,
        sum(sellqty) over (
            order by
                time
        ) as sellqty_cum
    from
        symbol_with_qty
),
symbol_with_cum_qty as (
    SELECT
        symbol_with_qty.*,
        buyqty_cum.buyqty_cum,
        sellqty_cum.sellqty_cum
    FROM
        symbol_with_qty
        LEFT JOIN buyqty_cum on buyqty_cum.orderid = symbol_with_qty.orderid
        LEFT JOIN sellqty_cum on sellqty_cum.orderid = symbol_with_qty.orderid
),
buycost as (
    SELECT
        orderid,
        cost,
        time,
        case
            WHEN side = 'buy' THEN cost
            ELSE 0
        END buycost
    FROM
        symbol_with_cum_qty
),
sellcost as (
    select
        orderid,
        cost,
        time,
        case
            WHEN side = 'sell' THEN cost
            ELSE 0
        END sellcost
    FROM
        symbol_with_cum_qty
),
symbol_with_cost as (
    SELECT
        symbol_with_cum_qty.*,
        buycost.buycost,
        sellcost.sellcost
    FROM
        symbol_with_cum_qty
        LEFT JOIN buycost on buycost.orderid = symbol_with_cum_qty.orderid
        LEFT JOIN sellcost on sellcost.orderid = symbol_with_cum_qty.orderid
),
buycost_cum as (
    select
        orderid,
        buycost,
        time,
        sum(buycost) over (
            order by
                time
        ) as buycost_cum
    FROM
        symbol_with_cost
),
sellcost_cum as (
    select
        orderid,
        sellcost,
        time,
        sum(sellcost) over (
            order by
                time
        ) as sellcost_cum
    FROM
        symbol_with_cost
),
symbol_with_cum_cost as (
    SELECT
        symbol_with_cost.*,
        buycost_cum.buycost_cum,
        sellcost_cum.sellcost_cum
    FROM
        symbol_with_cost
        LEFT JOIN buycost_cum on buycost_cum.orderid = symbol_with_cost.orderid
        LEFT JOIN sellcost_cum on sellcost_cum.orderid = symbol_with_cost.orderid
),
symbol_with_total_cost_qty as (
    Select
        orderid,
        symbol,
        side,
        price,
        cost,
        executedqty,
        time,
        mark_price,
        buyqty_cum as buy_qty,
        buycost_cum as buy_cost,
        sellqty_cum as sold_qty,
        sellcost_cum as sold_cost,
        (buycost_cum - sellcost_cum) as total_cost,
        (buyqty_cum - sellqty_cum) as total_qty,
        (buyqty_cum - sellqty_cum) "incoming_qty"
    from
        symbol_with_cum_cost
),
symbol_with_unrealizedPnL as (
    SELECT
        *,
        (mark_price * total_qty) - total_cost as unrealizedPnL,
        CASE
            WHEN incoming_qty - LAG(incoming_qty) OVER (
                ORDER BY
                    time
            ) < 0 THEN 1
            ELSE -1
        END "Realized"
    FROM
        symbol_with_total_cost_qty
),
raw_fifo as (
    SELECT
        orderid,
        symbol,
        time,
        qty_sold,
        cum_sold,
        qty_bought,
        buy_order_id,
        case
            when qty_sold = 0 then -1
            else round(
                (
                    cum_sold_cost - coalesce(lag(cum_sold_cost) over w, 0)
                ) / qty_sold,
                2
            )
        end fifo_price,
        prev_bought,
        total_cost,
        prev_total_cost,
        cum_sold_cost,
        coalesce(lag(cum_sold_cost) over w, 0) as prev_cum_sold_cost
    FROM
        (
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
                prev_total_cost -- 4
,
                round(
                    prev_total_cost + (
                        (tneg.cum_sold - tpos.prev_bought) /(tpos.qty_bought - tpos.prev_bought)
                    ) *(total_cost - prev_total_cost),
                    2
                ) as cum_sold_cost
            FROM
                (
                    SELECT
                        orderid,
                        symbol,
                        time,
                        executedqty as qty_sold,
                        sum(executedqty) over w as cum_sold
                    FROM
                        sbinancedata.portfolio
                    WHERE
                        side = 'sell' WINDOW w AS (
                            PARTITION BY symbol
                            ORDER BY
                                time
                        ) -- 1
                ) tneg
                LEFT JOIN (
                    SELECT
                        symbol,
                        orderid as buy_order_id,
                        sum(executedqty) over w as qty_bought,
                        coalesce(sum(executedqty) over prevw, 0) as prev_bought,
                        executedqty * price as cost,
                        sum(executedqty * price) over w as total_cost,
                        coalesce(sum(executedqty * price) over prevw, 0) as prev_total_cost
                    FROM
                        sbinancedata.portfolio
                    WHERE
                        side = 'buy' WINDOW w AS (
                            PARTITION BY symbol
                            ORDER BY
                                time
                        ),
                        prevw AS (
                            PARTITION BY symbol
                            ORDER BY
                                time ROWS BETWEEN unbounded preceding
                                AND 1 preceding
                        ) -- 2
                ) tpos -- 3
                ON tneg.cum_sold BETWEEN tpos.prev_bought
                AND tpos.qty_bought
                AND tneg.symbol = tpos.symbol
        ) t WINDOW w AS (
            PARTITION BY symbol
            ORDER BY
                time
        )
    ORDER BY
        time
),
symbol_with_realized_PnL as (
    SELECT
        symbol_with_unrealizedPnL.*,
        case
            WHEN cum_sold_cost > 0 THEN cum_sold_cost
            ELSE 0
        END cum_sold_cost
    FROM
        symbol_with_unrealizedPnL
        LEFT JOIN raw_fifo on symbol_with_unrealizedPnL.orderid = raw_fifo.orderid
),
symbol_with_agg_sold_cost as (
    SELECT
        symbol_with_realized_PnL.*,
        case
            when cum_sold_cost = 0 THEN sum(cum_sold_cost) over (
                order by
                    time
            )
            else cum_sold_cost
        end agg_sold_cost
    FROM
        symbol_with_realized_PnL
),
symbol_with_agg_realized_PnL as (
    SELECT
        symbol_with_agg_sold_cost.*,
        sold_cost - agg_sold_cost as realized_PnL
    FROM
        symbol_with_agg_sold_cost
),
symbol_with_real_cost as (
    SELECT
        symbol_with_agg_realized_PnL.*,
        total_cost + realized_PnL real_cost
    FROM
        symbol_with_agg_realized_PnL
)
SELECT
    symbol_with_real_cost.orderid,
    symbol_with_real_cost.symbol,
    symbol_with_real_cost.side,
    symbol_with_real_cost.price,
    symbol_with_real_cost.executedqty,
    symbol_with_real_cost.cost,
    symbol_with_real_cost.time,
    symbol_with_real_cost.mark_price,
    symbol_with_real_cost.buy_qty,
    symbol_with_real_cost.buy_cost,
    symbol_with_real_cost.sold_qty,
    symbol_with_real_cost.sold_cost,
    symbol_with_real_cost.total_cost,
    symbol_with_real_cost.total_qty,
    symbol_with_real_cost.incoming_qty,
    symbol_with_real_cost.unrealizedPnL,
    symbol_with_real_cost.realized_PnL,
    real_cost,
    ROUND(real_cost / total_qty, 6) avg_bought_price,
    case
        WHEN sold_qty > 0 THEN ROUND(sold_cost / sold_qty, 6)
        ELSE 0
    END avg_sold_price
FROM
    symbol_with_real_cost