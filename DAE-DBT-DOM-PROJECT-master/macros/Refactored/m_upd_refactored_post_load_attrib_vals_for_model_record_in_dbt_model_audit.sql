{% macro m_upd_refactored_post_load_attrib_vals_for_model_record_in_dbt_model_audit( p_pipeline_name, modelgrp, modelid, batchid, p_inc_load_ts_column='src_load_ts' ) %}

    {% if not execute %}
        {{ return(false) }}
    {% endif %}

    {% if modelid is not string %}
        {% set modelid_db = modelid.database %}
    {% else %}
        {% set modelid_db = modelid.split('.')[0] %}
    {% endif %}

    {# Fetch all matching query details (CREATE_VIEW + MERGE) concatenated via listagg #}
    {% call statement('fetch_query_details', fetch_result=true) %}
        select 
            listagg(concat(query_type,'  ::  ', query_id), '\n\n')
                within group (order by start_time) as query_id,
            listagg(concat(query_type,'  ::  ',
                replace(query_text,'''','''''')), '\n\n')
                within group (order by start_time) as query_text
        from table({{ modelid_db }}.information_schema.query_history_by_session())
        where (query_type ilike 'create_view' or query_type ilike 'merge' or query_type ilike 'CREATE_TABLE_AS_SELECT')
        and query_text like '%{{ modelid }}%'
        and query_text not ilike '%dbt_monitor%'
        limit 1
    {% endcall %}

    {% set result = load_result('fetch_query_details') %}

    {% if not result['data'] or result['data']|length == 0 %}
        {{ return(None) }}
    {% endif %}

    {% set query_id = result['data'][0][0] %}
    {% set query_text = result['data'][0][1] %}

    {# Fetch the MERGE query_id specifically for RESULT_SCAN #}
    {% call statement('fetch_merge_query_id', fetch_result=true) %}
        select query_id, query_type
        from table({{ modelid_db }}.information_schema.query_history_by_session(RESULT_LIMIT => 20))
        where (query_type ilike 'MERGE' or query_type ilike 'CREATE_TABLE_AS_SELECT')
          and query_text like '%{{ modelid }}%'
          and query_text not ilike '%dbt_monitor%'
          and execution_status = 'SUCCESS'
        order by end_time desc
        limit 1
    {% endcall %}

    {% set merge_result = load_result('fetch_merge_query_id') %}
    {% set merge_query_id = merge_result['data'][0][0] if merge_result['data'] and merge_result['data']|length > 0 else '' %}
    {% set merge_query_type = merge_result['data'][0][1] if merge_result['data'] and merge_result['data']|length > 0 else '' %}

    {# Get rows_inserted and rows_updated #}
    {% if merge_query_type == 'MERGE' %}
        {% call statement('fetch_merge_counts', fetch_result=true) %}
            select
                "number of rows inserted" as rows_inserted,
                "number of rows updated" as rows_updated
            from table(result_scan('{{ merge_query_id }}'))
        {% endcall %}
        {% set merge_counts = load_result('fetch_merge_counts') %}
        {% set rows_inserted = merge_counts['data'][0][0] if merge_counts['data'] and merge_counts['data']|length > 0 else 0 %}
        {% set rows_updated = merge_counts['data'][0][1] if merge_counts['data'] and merge_counts['data']|length > 0 else 0 %}
    {% else %}
        {# For CTAS/CREATE_VIEW, count rows in the target as inserts #}
        {% call statement('fetch_row_count', fetch_result=true) %}
            select count(1) as row_count from {{ modelid }}
        {% endcall %}
        {% set row_count_result = load_result('fetch_row_count') %}
        {% set rows_inserted = row_count_result['data'][0][0] if row_count_result['data'] and row_count_result['data']|length > 0 else 0 %}
        {% set rows_updated = 0 %}
    {% endif %}

    {# Update the audit table #}
    {% set upd_sql %}
    update {{ source('dbt_config', 'dbt_model_audit_t') }} tg
    set tg.inc_load_ts = nvl(ts.{{ p_inc_load_ts_column }}, tg.inc_load_ts),
        tg.model_load_status = 'completed',
        tg.rows_inserted = {{ rows_inserted }},
        tg.rows_updated = {{ rows_updated }},
        tg.end_ts = current_timestamp(),
        tg.query_id = '{{ query_id }}',
        tg.query_text = '{{ query_text }}'
    from (
        select max({{ p_inc_load_ts_column }}) as {{ p_inc_load_ts_column }}
        from {{ modelid }}
    ) ts
    where tg.model_id = '{{ modelid }}'
      and tg.model_grp = '{{ modelgrp }}'
      and tg.pipeline_name = '{{ p_pipeline_name }}'
      and tg.batch_id = '{{ batchid }}'
    {% endset %}

    {% do run_query(upd_sql) %}

{% endmacro %}
