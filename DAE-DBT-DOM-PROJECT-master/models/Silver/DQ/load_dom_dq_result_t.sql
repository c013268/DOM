{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_load_dq_result( var('p_pipeline_name'), v_batch_id ) %}


{{
    config(
		materialized='table',
        transient=true, 
        post_hook="drop table if exists {{ this }}"
    )
}}


select 'dq_result' result   --dbt run --select load_dom_dq_result_t