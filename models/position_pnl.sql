with last_update_time as (
    select
        symbol,
        max(time) as lasttime
    from
       {{ ref('order_with_realized_pnl') }}
    group by
        symbol
)
    select
        -- owrpnl.orderid,
        owrpnl.symbol,
        owrpnl.avg_bought_price,
        owrpnl.buy_qty_cum as bought_qty,
        owrpnl.avg_sold_price,
        owrpnl.sell_qty_cum as Sold_qty,
        owrpnl.unrealizedpnl as unrealized_pnl,
        owrpnl.realized_pnl,
        owrpnl.mark_price,
        owrpnl.mark_time,
        incoming.incoming_pnl,
        incoming.settlement_time,
        case when trading.trading_pnl_cum is null then 0
        else trading.trading_pnl_cum end
    from
       {{ ref('order_with_realized_pnl') }} owrpnl
    join last_update_time on last_update_time.lasttime = owrpnl.time and owrpnl.symbol = last_update_time.symbol
    join {{ ref('incoming_pnl') }} incoming on incoming.symbol = owrpnl.symbol
    LEFT join {{ ref('trading_pnl') }} trading on trading.symbol = owrpnl.symbol
    order by realized_pnl DESC
   



 