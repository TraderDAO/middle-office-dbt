SELECT
    symbol,
    orderid as buy_order_id,
    sum(executedqty) over w as qty_bought,
    coalesce(sum(executedqty) over prevw, 0) as prev_bought,
    executedqty * price as cost,
    sum(executedqty * price) over w as total_cost,
    coalesce(sum(executedqty * price) over prevw, 0) as prev_total_cost
FROM
    {{ ref('raw_order') }}        --dbt_orangesky.orderstable
WHERE
    side = 'buy' WINDOW w AS (
        PARTITION BY symbol
        ORDER BY time
    ),
    prevw AS (
        PARTITION BY symbol
        ORDER BY
            time ROWS BETWEEN unbounded preceding
            AND 1 preceding
    )