with incoming_pnl_per_unit as (
    select
        st.*,
        st.mark_price - st.settlement_price incoming_gain_pu,
        lasttrade
    from
        {{ ref('settlement_table') }} st
        join {{ ref('settlement_last_trade') }} slt
        on slt.lasttrade = st.time
)

select 
    *,
    total_qty * incoming_gain_pu as incoming_pnl
from 
    incoming_pnl_per_unit
