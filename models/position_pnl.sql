with abc as (
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
        abc
    group by
        symbol
)


 select
        abc.symbol,
        abc.time,
         abc.avg_bought_price,
         abc.avg_sold_price,
         abc.unrealized_pnl,
         abc.realized_pnl,
        lasttime.lasttime
    from
         abc
    join 
        lasttime on lasttime.lasttime = abc.time