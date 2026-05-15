{{
    config(
		pre_hook = ["{{ fl_utils.m_load_dq_rule_exp_config( 'dq_rule_exp_dom_gold_dq_rule_attribute_length_check' ) }}",
                    "{{ fl_utils.m_load_dq_rule_exp_config( 'dq_rule_exp_dom_gold_dq_rule_attribute_within_range_check' ) }}",
                    "{{ fl_utils.m_load_dq_rule_exp_config( 'dq_rule_exp_dom_gold_dq_rule_conditional_null' ) }}",
                    "{{ fl_utils.m_load_dq_rule_exp_config( 'dq_rule_exp_dom_gold_dq_rule_not_null' ) }}",
                    "{{ fl_utils.m_load_dq_rule_exp_config( 'dq_rule_exp_dom_gold_dq_rule_referential_integrity' ) }}",
                    "{{ fl_utils.m_load_dq_rule_exp_config( 'dq_rule_exp_dom_gold_dq_rule_unique_type2' ) }}"],
        materialized='table',
        transient=true, 
        post_hook="drop table if exists {{ this }}"
    )
}}


select 'dq_rule_exp' result   --dbt run --select load_dom_dq_rule_exp_t