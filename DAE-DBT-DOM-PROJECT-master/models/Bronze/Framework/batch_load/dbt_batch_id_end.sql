{{
    config(
		pre_hook = ["{{ fl_utils.m_batch_id_update_status_to_completed( var('p_pipeline_name') | trim ) }}"],
        materialized='table',
        transient=true, 
        post_hook="drop table if exists {{ this }}"
    )
}}


select 'completed' batch_status