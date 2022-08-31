with last_update_time as (
    select
        symbol,
        max(time) as lasttime
    from
       {{ ref('order_with_realized_pnl') }}
    group by
        symbol
),
position as (
    select
        distinct owrpnl.symbol,
        owrpnl.total_qty,
        owrpnl.total_qty * owrpnl.mark_price as position_value,
        owrpnl.avg_bought_price,
        owrpnl.buy_qty_cum as bought_qty,
        owrpnl.avg_sold_price,
        owrpnl.sell_qty_cum as Sold_qty,
        owrpnl.unrealizedpnl as unrealized_pnL,
        owrpnl.realized_pnL,
        owrpnl.mark_price,
        owrpnl.mark_time,
        incoming.incoming_pnL,
        case when trading.trading_pnl_cum is null then 0
        else trading.trading_pnl_cum end trading_pnL,

        case when dp.daily_avg_bought_price is null then 0
        else dp.daily_avg_bought_price end,

        case when dp.buy_qty_cum is null then 0
        else dp.buy_qty_cum end daily_bought_qty,

        case when dp.daily_avg_sold_price is null then 0
        else dp.daily_avg_sold_price end,
        
        case when dp.sell_qty_cum is null then 0
        else dp.sell_qty_cum end daily_sold_qty,
        incoming.settlement_time

    from
       {{ ref('order_with_realized_pnl') }} owrpnl
    join last_update_time on last_update_time.lasttime = owrpnl.time and owrpnl.symbol = last_update_time.symbol
    join {{ ref('incoming_pnl') }} incoming on incoming.symbol = owrpnl.symbol
    LEFT join {{ ref('trading_pnl') }} trading on trading.symbol = owrpnl.symbol
    LEFT join {{ ref('daily_price') }} dp on dp.symbol = owrpnl.symbol
)

select * from position
union
select * from {{ ref('stable_coin_table') }}
order by position_value DESC
