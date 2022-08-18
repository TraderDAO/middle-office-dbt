with position_pnl as (
    select
        symbol,
        time,
        avg_bought_price,
        avg_sold_price,
        unrealizedPnL as unrealized_pnl,
        realized_pnl,
        RANk() over (
            PARTITION by symbol
            order by
                time
        )
    from
        {{ ref('order_with_realized_pnl') }}
),
lasttime as (
    select
        symbol,
        max(time) as lasttime
    from
        position_pnl
    group by
        symbol
)


 select
        position_pnl.symbol,
        position_pnl.time,
         position_pnl.avg_bought_price,
         position_pnl.avg_sold_price,
         position_pnl.unrealized_pnl,
         position_pnl.realized_pnl,
        lasttime.lasttime
    from
         position_pnl
    join 
        lasttime on lasttime.lasttime = position_pnl.time