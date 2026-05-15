{{
    config(
		pre_hook = ["{{ fl_utils.m_load_dq_group_config( 'dom_dq_group' ) }}"],
        materialized='table',
        transient=true, 
        post_hook="drop table if exists {{ this }}"
    )
}}


select 'dom_dq_group' result   --dbt run --select load_dom_dq_group_t