{{
    config(
		pre_hook = ["{{ fl_utils.m_model_load_with_model_grp( 'nonpriority_2_inv_mgmt_silver', var('p_full_model_name') | trim, var('p_is_backfill') | trim ) }}"],
        materialized='table',
        transient=true, 
        post_hook="drop table if exists {{ this }}"
    )
}}

select 'nonpriority_2_inv_mgmt_silver' model_grp