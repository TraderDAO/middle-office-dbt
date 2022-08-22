with position_pnl as (
    select
        symbol,
        time,
        avg_bought_price,
        buy_qty_cum,
        avg_sold_price,
        sell_qty_cum,
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
        -- position_pnl.time as last_tx_time,
        position_pnl.avg_bought_price,
        position_pnl.buy_qty_cum as bought_qty,
        position_pnl.avg_sold_price,
        position_pnl.sell_qty_cum as Sold_qty,
        position_pnl.unrealized_pnl,
        position_pnl.realized_pnl,
        incoming.mark_price,
        incoming.incoming_pnl,
        -- incoming.incoming_time,
        case when trading.orders_trading_pnl is null then 0
        else trading.orders_trading_pnl end
    from
         position_pnl
    join lasttime on lasttime.lasttime = position_pnl.time
    join {{ ref('incomingPnL') }} incoming on incoming.symbol = position_pnl.symbol
    LEFT join {{ ref('tradingPnL') }} trading on trading.symbol = position_pnl.symbol
    order by realized_pnl DESC


 