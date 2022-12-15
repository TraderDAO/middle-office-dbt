WITH last_time AS (
    SELECT
        MAX(TIMESTAMP) last_mark_time,
        symbol
    FROM
        {% if target.name == 'dev' %}
            PUBLIC.markprice mp {% elif target.name == 'prod' %}
            PUBLIC.markprice mp
        {% endif %}
    GROUP BY
        symbol
),
last_receivetime AS (
    SELECT
        MAX(receivetimestamp) last_receive_time,
        symbol
    FROM
        {% if target.name == 'dev' %}
            PUBLIC.markprice mp {% elif target.name == 'prod' %}
            PUBLIC.markprice mp
        {% endif %}
    GROUP BY
        symbol
),
mark_table AS (
    SELECT
        DISTINCT mp.symbol,
        mp.price AS mark_price,
        mp.datetime AS mark_time,
        mp.receivetime AS receive_time,
        mp.timestamp,
        last_mark_time
    FROM
        {% if target.name == 'dev' %}
            PUBLIC.markprice mp {% elif target.name == 'prod' %}
            PUBLIC.markprice mp
        {% endif %}
        JOIN last_time
        ON last_time.last_mark_time = mp.timestamp
        JOIN last_receivetime
        ON last_receivetime.last_receive_time = mp.receivetimestamp
    ORDER BY
        mp.symbol
)
SELECT
    SPLIT_PART(
        symbol,
        '/',
        1
    ) AS symbol,
    mark_price,
    mark_time,
    receive_time
FROM
    mark_table
