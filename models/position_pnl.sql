-- Data in the Last Order
WITH last_update_time AS (
    SELECT
        symbol,
        MAX(TIME) AS lasttime
    FROM
        {{ ref('order_with_realized_pnl') }}
    GROUP BY
        symbol
),
-- Join target_weight, position_weight, net_weight
with_weight AS(
    SELECT
        abc.symbol,
        abc.total_qty,
        abc.mark_price,
        bbb.weight AS target_weight
    FROM
        {{ ref('order_with_realized_pnl') }}
        abc
        JOIN last_update_time
        ON last_update_time.lasttime = abc.time
        LEFT JOIN {{ ref('target_weight') }}
        bbb
        ON bbb.symbol = abc.symbol
),
with_position_value AS (
    SELECT
        symbol,
        total_qty * mark_price AS position_value,
        target_weight,
        CASE
            WHEN symbol IS NULL THEN 0
            ELSE 1
        END AS condition
    FROM
        with_weight
),
with_total_value AS (
    SELECT
        *,
        SUM(position_value) over(
            PARTITION BY condition
        ) AS total_value
    FROM
        with_position_value
),
with_position_weight AS (
    SELECT
        symbol,
        target_weight,
        ROUND((position_value / total_value), 2) AS position_weight
    FROM
        with_total_value
),
with_net_weight AS (
    SELECT
        *,
        target_weight - position_weight AS net_weight
    FROM
        with_position_weight
),
-- Join position data
POSITION AS (
    SELECT
        DISTINCT owrpnl.symbol,
        with_net_weight.target_weight,
        with_net_weight.position_weight,
        with_net_weight.net_weight,
        owrpnl.total_qty,
        owrpnl.total_qty * owrpnl.mark_price AS position_value,
        owrpnl.avg_bought_price,
        owrpnl.buy_qty_cum AS bought_qty,
        owrpnl.avg_sold_price,
        owrpnl.sell_qty_cum AS sold_qty,
        CASE
            WHEN owrpnl.total_qty = 0 THEN 0
            ELSE owrpnl.unrealizedpnl
        END unrealized_pnL,
        owrpnl.realized_pnL,
        owrpnl.mark_price,
        owrpnl.mark_time,
        incoming.incoming_pnL,
        CASE
            WHEN trading.trading_pnl_cum IS NULL THEN 0
            ELSE trading.trading_pnl_cum
        END trading_pnL,
        CASE
            WHEN dp.daily_avg_bought_price IS NULL THEN 0
            ELSE dp.daily_avg_bought_price
        END,
        CASE
            WHEN dp.buy_qty_cum IS NULL THEN 0
            ELSE dp.buy_qty_cum
        END daily_bought_qty,
        CASE
            WHEN dp.daily_avg_sold_price IS NULL THEN 0
            ELSE dp.daily_avg_sold_price
        END,
        CASE
            WHEN dp.sell_qty_cum IS NULL THEN 0
            ELSE dp.sell_qty_cum
        END daily_sold_qty,
        incoming.settlement_time
    FROM
        {{ ref('order_with_realized_pnl') }}
        owrpnl
        JOIN last_update_time
        ON last_update_time.lasttime = owrpnl.time
        AND owrpnl.symbol = last_update_time.symbol
        JOIN with_net_weight
        ON with_net_weight.symbol = owrpnl.symbol
        JOIN {{ ref('incoming_pnl') }}
        incoming
        ON incoming.symbol = owrpnl.symbol
        LEFT JOIN {{ ref('trading_pnl') }}
        trading
        ON trading.symbol = owrpnl.symbol
        LEFT JOIN {{ ref('daily_price') }}
        dp
        ON dp.symbol = owrpnl.symbol
),
-- union with the stable coin
with_stable_coin AS (
    SELECT
        *
    FROM
        POSITION
    UNION
    SELECT
        *
    FROM
        {{ ref('stable_coin_table') }}
    ORDER BY
        position_value DESC
),
-- handle null data in the target weight
with_case_weight AS (
    SELECT
        symbol,
        CASE
            WHEN target_weight IS NULL THEN 0.00
            ELSE target_weight
        END target_weight,
        CASE
            WHEN position_weight IS NULL THEN 0.00
            ELSE position_weight
        END position_weight,
        target_weight - position_weight,
        total_qty,
        position_value,
        avg_bought_price,
        bought_qty,
        avg_sold_price,
        sold_qty,
        unrealized_pnL,
        realized_pnL,
        mark_price,
        mark_time,
        incoming_pnl,
        trading_pnl,
        daily_avg_bought_price,
        daily_bought_qty,
        daily_avg_sold_price,
        daily_sold_qty,
        settlement_time
    FROM
        with_stable_coin
)
SELECT
    symbol,
    target_weight,
    position_weight,
    target_weight - position_weight net_weight,
    total_qty,
    position_value,
    avg_bought_price,
    bought_qty,
    avg_sold_price,
    sold_qty,
    unrealized_pnL,
    realized_pnL,
    mark_price,
    mark_time,
    incoming_pnl,
    trading_pnl,
    daily_avg_bought_price,
    daily_bought_qty,
    daily_avg_sold_price,
    daily_sold_qty,
    settlement_time
FROM
    with_case_weight
