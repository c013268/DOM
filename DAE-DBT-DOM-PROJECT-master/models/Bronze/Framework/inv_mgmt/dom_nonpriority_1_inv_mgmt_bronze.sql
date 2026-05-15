{{
    config(
		pre_hook = ["{{ fl_utils.m_model_load_with_model_grp( 'nonpriority_1_inv_mgmt_bronze', var('p_full_model_name') | trim ) }}"],
        materialized='table',
        transient=true, 
        post_hook="drop table if exists {{ this }}"
    )
}}

select 'nonpriority_1_inv_mgmt_bronze' model_grp