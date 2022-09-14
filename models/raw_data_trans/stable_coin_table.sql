with last_time as (
    select
        max(timestamp) last_time,
        symbol
    from
        {% if target.name == 'dev' %} dbt_traderdao.stablecoinpnl stable
        {% elif target.name == 'prod' %} public.stablecoinpnl stable
        {% endif %}
    group by symbol
)
    select
        distinct stable.symbol, stable.total_qty, stable.position_value, 
        stable.avg_bought_price, stable.bought_qty, 
        stable.avg_sold_price, stable.Sold_qty,
        stable.unrealized_pnl, stable.realized_pnl, stable.mark_price,
        stable.mark_time, stable.incoming_pnl,
        -- last_time.last_time,
        stable.trading_pnl,
        stable.daily_avg_bought_price,
        stable.daily_bought_qty,
        stable.daily_avg_sold_price,
        stable.daily_sold_qty,
        stable.settlement_time
    from
        {% if target.name == 'dev' %} dbt_traderdao.stablecoinpnl stable
        {% elif target.name == 'prod' %} public.stablecoinpnl stable
        {% endif %}
    join last_time on last_time.last_time = stable.timestamp and last_time.symbol = stable.symbol

