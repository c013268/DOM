{{
    config(
		pre_hook = ["{{ fl_utils.m_model_load_with_model_grp( 'priority_2_ord_fulflmnt_bronze', var('p_full_model_name') | trim ) }}"],
        materialized='table',
        transient=true, 
        post_hook="drop table if exists {{ this }}"
    )
}}


select 'priority_2_ord_fulflmnt_bronze' model_grp