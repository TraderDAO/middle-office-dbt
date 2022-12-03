SELECT
    DISTINCT symbol,
    weight
FROM
    {% if target.name == 'dev' %}
        dbt_traderdao.targetweight {% elif target.name == 'prod' %}
        PUBLIC.targetweight
    {% endif %}
