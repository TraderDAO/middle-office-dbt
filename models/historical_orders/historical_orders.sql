WITH today AS (
    SELECT
        DISTINCT MAX(TIMESTAMP) AS today_timestamp
    FROM
        {% if target.name == 'dev' %}
            PUBLIC.settlementprice {% elif target.name == 'prod' %}
            PUBLIC.settlementprice
        {% endif %}
),
orders AS (
    SELECT
        DISTINCT orders.*,
        today.today_timestamp
    FROM
        {% if target.name == 'dev' %}
            PUBLIC.orderstable orders {% elif target.name == 'prod' %}
            PUBLIC.orderstable orders
        {% endif %}
        JOIN today
        ON today.today_timestamp IS NOT NULL
)
SELECT
    *
FROM
    orders
WHERE
    TIME < today_timestamp
