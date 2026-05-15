{% macro m_dom_info_check(
        p_database,
        p_search_value,
        p_schema=None
    ) %}

    {% if execute %}

        {# Clean old results for this value #}
        {% call statement('cleanup') %}
            delete from SILVER_DEV_DB.DOM_SILVER_DEV.DB_VALUE_SEARCH_RESULTS
            where search_value = '{{ p_search_value }}'
        {% endcall %}

        {# Get candidate columns #}
        {% call statement('get_columns', fetch_result=True) %}

            select 
                table_schema,
                table_name,
                column_name
            from {{ p_database }}.information_schema.columns
            where (
                    data_type ilike '%CHAR%'
                 or data_type ilike '%TEXT%'
                 or data_type ilike '%STRING%'
                  )
              and table_schema != 'INFORMATION_SCHEMA'
              
              {% if p_schema %}
                and upper(table_schema) = upper('{{ p_schema }}')
              {% endif %}
              
            order by table_schema, table_name

        {% endcall %}

        {% set columns = load_result('get_columns')['data'] %}

        {# Loop safely per column #}
        {% for row in columns %}

            {% set schema = row[0] %}
            {% set table  = row[1] %}
            {% set column = row[2] %}

            {% set sql %}
                insert into SILVER_DEV_DB.DOM_SILVER_DEV.DB_VALUE_SEARCH_RESULTS
                select
                    '{{ p_search_value }}',
                    '{{ p_database }}',
                    '{{ schema }}',
                    '{{ table }}',
                    '{{ column }}',
                    count(*),
                    current_timestamp()
                from {{ p_database }}.{{ schema }}.{{ table }}
                where {{ column }}::varchar ilike '%{{ p_search_value }}%'
                having count(*) > 0
            {% endset %}

            {% call statement('insert_result') %}
                {{ sql }}
            {% endcall %}

        {% endfor %}

    {% endif %}

{% endmacro %}
