with raw_incoming as (
    select
        distinct symbol,
        price as incoming_price,
        timestamp as incoming_time
    from
        dbt_traderdao.incomingprice
),
incoming_table as (
    select
        distinct trpnl.orderid,
        trpnl.time,
        trpnl.symbol,
        trpnl.side,
        trpnl.price,
        trpnl.total_qty,
        trpnl.mark_price,
        raw_incoming.incoming_price,
        raw_incoming.incoming_time
    from
        {{ ref('order_with_realized_pnl') }} trpnl
        join raw_incoming on raw_incoming.symbol = trpnl.symbol
    where
        raw_incoming.incoming_time > trpnl.time
),
last_trade as (
    select
        symbol,
        max(time) lasttrade
    from
        incoming_table
    group by
        symbol
),
incoming_pnl_table as (
    select
        incoming_table.symbol,
        incoming_table.orderid,
        incoming_table.total_qty,
        incoming_table.mark_price - incoming_table.incoming_price incoming_gain_pu,
        incoming_table.mark_price,
        incoming_table.incoming_price,
        incoming_table.time,
        incoming_table.incoming_time,
        lasttrade
    from
        incoming_table
    join 
        last_trade on last_trade.lasttrade = incoming_table.time 
)

select 
    symbol,
    mark_price,
    incoming_price,
    total_qty * incoming_gain_pu as incoming_pnl,
    incoming_time,
    lasttrade as symbol_last_traded
from
    incoming_pnl_table