-- with sum_pnl as (
    select 
        -- unrealized_pnl, realized_pnl, mark_price, mark_time, incoming_pnl, trading_pnl, settlement_time,
        sum(position_value) as account_value,
        sum(unrealized_pnl) as unrealized_pnL,
        sum(realized_pnl) as realized_pnL,
        sum(realized_pnl) + sum(unrealized_pnl) as total_pnL,
        sum(incoming_pnl) as incoming_pnL,
        sum(trading_pnl) as trading_pnL,
        sum(incoming_pnl) + sum(trading_pnl) as netting_pnL,
        settlement_time
    from {{ ref('position_pnl') }}
    group by settlement_time
-- ),
-- settlement_time as (
--     select settlement_time
--     from {{ ref('position_pnl') }}
--     LIMIT 1
-- )

-- select *
-- from sum_pnl
