{{
    config(
		pre_hook = ["{{ fl_utils.m_load_dq_project_config( 'dom_dq_project' ) }}"],
        materialized='table',
        transient=true, 
        post_hook="drop table if exists {{ this }}"
    )
}}


select 'dom_dq_project' result   --dbt run --select load_dom_dq_project_t