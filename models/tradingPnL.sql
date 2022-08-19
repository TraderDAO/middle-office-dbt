with raw_incoming as (
    select
        distinct symbol,
        price as incoming_price,
        timestamp as incoming_time
    from
        dbt_traderdao.incomingprice
),

trading_table as (
    select
        distinct trpnl.orderid,
        trpnl.time,
        trpnl.symbol,
        trpnl.side,
        trpnl.price,
        -- trpnl.buy_qty,
        -- trpnl.sell_qty,
        trpnl.executedqty,
        trpnl.total_qty,
        trpnl.mark_price,
        raw_incoming.incoming_price,
        raw_incoming.incoming_time
    from
        {{ ref('order_with_realized_pnl') }} trpnl
        join raw_incoming on raw_incoming.symbol = trpnl.symbol
    where
        raw_incoming.incoming_time < trpnl.time
),
order_trading_pnl as (
    select
    symbol,
    orderid,
    time,
    side,
    case
        when side = 'buy' then 1
        else -1
    end trade_side,
    price,
    executedqty,
    mark_price,
    ( mark_price - price ) * executedqty as ordertradingpnl
    from
        trading_table 
),
orders_trading_pnl as (
    select 
    symbol,
    orderid,
    side,
    time,
    sum(ordertradingpnl) over (PARTITION by symbol order by time) as orders_trading_pnl
    from 
        order_trading_pnl
),
last_trade as (
    select
        symbol,
        max(time) as lasttrade
    from
        orders_trading_pnl
    group by
        symbol
)

select 
    orders_trading_pnl.*,
    last_trade.lasttrade
from 
    orders_trading_pnl
join 
    last_trade on last_trade.lasttrade = orders_trading_pnl.time 



    
    
