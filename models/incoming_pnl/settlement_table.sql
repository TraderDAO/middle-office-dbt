select
    distinct trpnl.orderid,
    trpnl.time,
    trpnl.symbol,
    trpnl.side,
    trpnl.price,
    trpnl.total_qty,
    trpnl.mark_price,
    sp.settlement_price,
    sp.settlement_time,
    sp.settlement_timestamp,
    sp.receive_time
from
    {{ ref('order_with_realized_pnl') }} trpnl
    left join {{ ref('settlement_price') }} sp on sp.symbol = trpnl.symbol
where
    sp.settlement_timestamp > trpnl.time