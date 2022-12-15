-- with today as (
--     select
--         distinct max(timestamp) as today_timestamp
--     from
--         {% if target.name == 'dev' %}  dbt_traderdao.settlementprice
--         {% elif target.name == 'prod' %}  public.settlementprice
--         {% endif %}
-- ),
-- orders as (
--     SELECT
--         distinct orders.*,
--         today.today_timestamp
--     FROM
--         {% if target.name == 'dev' %}  dbt_traderdao.orderstable  orders
--         {% elif target.name == 'prod' %}  public.orderstable orders
--         {% endif %}
--     join today on today.today_timestamp is not null
-- )
SELECT
    *
FROM
    {% if target.name == 'dev' %}
        PUBLIC.orderstable orders {% elif target.name == 'prod' %}
        PUBLIC.orderstable orders
    {% endif %}
WHERE
    openstatus = 'open'
