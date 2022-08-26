with order_trading_pnl as (
    select
        *,
        case
            when side = 'buy' then 1
            else -1
        end trade_side,
        ( mark_price - price ) * executedqty as unsign_ordertradingpnl
    from
        {{ ref('trading_table') }} 
),
trading_pnl_with_side as (
    select *, unsign_ordertradingpnl * trade_side as trading_pnl
    from order_trading_pnl
)

    select 
        *,
        sum(trading_pnl) over (PARTITION by symbol order by time) as trading_pnl_cum
    from 
        trading_pnl_with_side
