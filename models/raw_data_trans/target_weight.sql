SELECT
    DISTINCT symbol,
    weight
FROM
    {% if target.name == 'dev' %}
        PUBLIC.targetweight {% elif target.name == 'prod' %}
        PUBLIC.targetweight
    {% endif %}
