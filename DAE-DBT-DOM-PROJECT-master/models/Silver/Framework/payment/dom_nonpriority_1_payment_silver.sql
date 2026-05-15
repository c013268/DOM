{{
    config(
		pre_hook = ["{{ fl_utils.m_model_load_with_model_grp( 'nonpriority_1_payment_silver', var('p_full_model_name') | trim, var('p_is_backfill') | trim ) }}"],
        materialized='table',
        transient=true, 
        post_hook="drop table if exists {{ this }}"
    )
}}


select 'nonpriority_1_payment_silver' model_grp