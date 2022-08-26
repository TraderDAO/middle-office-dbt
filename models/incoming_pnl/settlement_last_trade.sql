select
    symbol,
    max(time) lasttrade
from
    {{ ref('settlement_table') }}
group by
    symbol