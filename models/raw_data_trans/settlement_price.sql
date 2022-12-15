WITH last_time AS (
    SELECT
        MAX(TIMESTAMP) last_settlement_time,
        symbol
    FROM
        {% if target.name == 'dev' %}
            PUBLIC.settlementprice sp {% elif target.name == 'prod' %}
            PUBLIC.settlementprice sp
        {% endif %}
    GROUP BY
        symbol
),
last_receive_time AS (
    SELECT
        MAX(receivetimestamp) receivetimestamp,
        symbol
    FROM
        {% if target.name == 'dev' %}
            PUBLIC.settlementprice sp {% elif target.name == 'prod' %}
            PUBLIC.settlementprice sp
        {% endif %}
    GROUP BY
        symbol
)
SELECT
    DISTINCT SPLIT_PART(
        sp.symbol,
        '/',
        1
    ) AS symbol,
    sp.price AS settlement_price,
    sp.timestamp AS settlement_timestamp,
    sp.datetime AS settlement_time,
    sp.receivetime AS receive_time,
    sp.receivetime AS receivetimestamp,
    last_receive_time.receivetimestamp AS last_receive,
    last_time.last_settlement_time
FROM
    {% if target.name == 'dev' %}
        PUBLIC.settlementprice sp {% elif target.name == 'prod' %}
        PUBLIC.settlementprice sp
    {% endif %}
    JOIN last_time
    ON last_time.last_settlement_time = sp.timestamp
    JOIN last_receive_time
    ON last_receive_time.receivetimestamp = sp.receivetimestamp
