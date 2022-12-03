WITH last_time AS (
    SELECT
        MAX(TIMESTAMP) last_time,
        symbol
    FROM
        {% if target.name == 'dev' %}
            dbt_traderdao.stablecoinpnl stable {% elif target.name == 'prod' %}
            PUBLIC.stablecoinpnl stable
        {% endif %}
    GROUP BY
        symbol
)
SELECT
    DISTINCT stable.symbol,
    stable.target_weight,
    stable.position_weight,
    stable.net_weight,
    stable.total_qty,
    stable.position_value,
    stable.avg_bought_price,
    stable.bought_qty,
    stable.avg_sold_price,
    stable.sold_qty,
    stable.unrealized_pnl,
    stable.realized_pnl,
    stable.mark_price,
    stable.mark_time,
    stable.incoming_pnl,
    -- last_time.last_time,
    stable.trading_pnl,
    stable.daily_avg_bought_price,
    stable.daily_bought_qty,
    stable.daily_avg_sold_price,
    stable.daily_sold_qty,
    stable.settlement_time
FROM
    {% if target.name == 'dev' %}
        dbt_traderdao.stablecoinpnl stable {% elif target.name == 'prod' %}
        PUBLIC.stablecoinpnl stable
    {% endif %}
    JOIN last_time
    ON last_time.last_time = stable.timestamp
    AND last_time.symbol = stable.symbol
