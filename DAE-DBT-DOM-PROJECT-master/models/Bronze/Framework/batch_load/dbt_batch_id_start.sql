{{
    config(
		pre_hook = ["{{ fl_utils.m_batch_id_calc_n_ins( var('p_pipeline_name') | trim ) }}"],
        materialized='table',
        transient=true, 
        post_hook="drop table if exists {{ this }}"
    )
}}

select 'running' batch_status