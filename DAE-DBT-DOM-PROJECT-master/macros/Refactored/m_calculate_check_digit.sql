{% macro m_calculate_check_digit(sku_expr) %}
    MOD(
        ((FLOOR((
            {% for i in range(20) %}
                CASE WHEN LENGTH({{ sku_expr }}) >= {{ i + 1 }} THEN
                    CASE WHEN MOD({{ i + 1 }}, 2) = 1 THEN
                        CASE WHEN CAST(SUBSTR({{ sku_expr }}, {{ i + 1 }}, 1) AS INTEGER) * 2 > 9
                            THEN CAST(SUBSTR(CAST(CAST(SUBSTR({{ sku_expr }}, {{ i + 1 }}, 1) AS INTEGER) * 2 AS VARCHAR), 1, 1) AS INTEGER)
                               + CAST(SUBSTR(CAST(CAST(SUBSTR({{ sku_expr }}, {{ i + 1 }}, 1) AS INTEGER) * 2 AS VARCHAR), 2, 1) AS INTEGER)
                            ELSE CAST(SUBSTR({{ sku_expr }}, {{ i + 1 }}, 1) AS INTEGER) * 2
                        END
                    ELSE 0 END
                ELSE 0 END
                {% if not loop.last %}+{% endif %}
            {% endfor %}
        ) + (
            {% for i in range(20) %}
                CASE WHEN LENGTH({{ sku_expr }}) >= {{ i + 1 }} THEN
                    CASE WHEN MOD({{ i + 1 }}, 2) = 0
                        THEN CAST(SUBSTR({{ sku_expr }}, {{ i + 1 }}, 1) AS INTEGER)
                    ELSE 0 END
                ELSE 0 END
                {% if not loop.last %}+{% endif %}
            {% endfor %}
        )) / 10) + 1) * 10) - (
            {% for i in range(20) %}
                CASE WHEN LENGTH({{ sku_expr }}) >= {{ i + 1 }} THEN
                    CASE WHEN MOD({{ i + 1 }}, 2) = 1 THEN
                        CASE WHEN CAST(SUBSTR({{ sku_expr }}, {{ i + 1 }}, 1) AS INTEGER) * 2 > 9
                            THEN CAST(SUBSTR(CAST(CAST(SUBSTR({{ sku_expr }}, {{ i + 1 }}, 1) AS INTEGER) * 2 AS VARCHAR), 1, 1) AS INTEGER)
                               + CAST(SUBSTR(CAST(CAST(SUBSTR({{ sku_expr }}, {{ i + 1 }}, 1) AS INTEGER) * 2 AS VARCHAR), 2, 1) AS INTEGER)
                            ELSE CAST(SUBSTR({{ sku_expr }}, {{ i + 1 }}, 1) AS INTEGER) * 2
                        END
                    ELSE 0 END
                ELSE 0 END
                {% if not loop.last %}+{% endif %}
            {% endfor %}
        ) - (
            {% for i in range(20) %}
                CASE WHEN LENGTH({{ sku_expr }}) >= {{ i + 1 }} THEN
                    CASE WHEN MOD({{ i + 1 }}, 2) = 0
                        THEN CAST(SUBSTR({{ sku_expr }}, {{ i + 1 }}, 1) AS INTEGER)
                    ELSE 0 END
                ELSE 0 END
                {% if not loop.last %}+{% endif %}
            {% endfor %}
        ),
        10
    )
{% endmacro %}
