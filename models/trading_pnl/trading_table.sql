-- with trades_today as (
    select
        distinct trpnl.orderid,
        trpnl.time,
        trpnl.symbol,
        trpnl.side,
        trpnl.price,
        trpnl.executedqty,
        trpnl.cost,
        trpnl.total_qty,
        trpnl.mark_price,
        sp.settlement_price,
        sp.settlement_time,
        sp.settlement_timestamp,
        sp.receive_time
    from
        {{ ref('order_with_realized_pnl') }} trpnl
        join {{ ref('settlement_price') }} sp on sp.symbol = trpnl.symbol
    where
        sp.settlement_timestamp < trpnl.time
-- ),
-- last_incoming_time as (
--     select
--         symbol,
--         max(settlement_timestamp) last_settlement_time
--     from
--         trades_today
--     group by
--         symbol
-- )

-- select 
--    distinct trades_today.*
-- from 
--    trades_today
-- join 
--     last_incoming_time on last_incoming_time.last_settlement_time = trades_today.settlement_timestamp