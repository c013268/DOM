{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "profile_id", "promo_id" ]  %}
	
{% set target_context_typ2_column_list = 
    [ "promo_type_id", "event_id", "item_event_id", "is_offer_num_generated", "is_offer_num_reqd", "promo_grp", "promo_desc", "promo_loc_grp", "desc", "created_by", "item_is_offer_num_generated", "item_is_offer_num_req", "item_apply_on_reg_price", "item_count_of_offer_num", "item_coupon_cd", "item_coupon_max_redemptions", "item_coupon_use_drives_promo", "item_enable_prompting", "item_max_redemptions", "item_offer_num_max_redemptions", "item_offer_ref_num", "prc_min_qualify_prompt_units", "promo_short_desc", "prort_across_qual_and_target", "prort_per_redemption", "restrict_to_qualified_units", "tag_names", "promo_cond", "qualifier_desc", "benefit_bounceback", "benefit_fixed_disc_for_grp", "benefit_fixed_disc_per_unit", "benefit_fixed_price_for_grp", "benefit_fixed_price_for_unit", "benefit_free_unit", "benefit_pct", "bounceback_cd", "bounceback_coupon_type", "bounceback_desc", "charge_type", "count_of_offer_num", "coupon_cd", "enable_prompting", "exclude_exceptions", "exclude_qual_from_target", "hdr_level_promo", "include_full_price_items", "include_sale_items", "max_promo_amt", "max_redemptions", "max_target_amt", "max_target_units", "max_unit_price_for_qual", "max_unit_price_for_target", "minimize_target_units_used", "min_qual_amt", "min_qual_units", "min_target_units", "min_unit_price_for_target", "min_unit_price_for_qual", "offer_num_max_redemptions", "offer_ref_num", "promo_qual_short_desc"]  %}

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk","hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}"
                    ,"{{  fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_promo_silver','mao_prc_combo_promotion_config_v'), this, 'combo_promotion_config_pk' )  }}"
                    ,"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ), 'src_load_ts' ) }}"], 
    meta={'strategy': 'merge', 'update_condition': 'active_flg'}
) }}

with
	item_promotion as (
		select
			ipc.profile_id,
			ipc.promo_id,
			ipc.event_id as item_event_id,
            ipc.promo_group,
            ipc.promo_desc,
            ipc.promo_location_group,
			ipc.is_offer_number_generated as item_is_offer_number_generated,
            ipc.is_offer_number_required as item_is_offer_number_reqd,
            ipc.apply_on_regular_price as item_apply_on_regular_price,
            ipc.count_of_offer_number as item_count_of_offer_number,
            ipc.coupon_code as item_coupon_code,
            ipc.coupon_max_redemptions as item_coupon_max_redemptions,
            ipc.coupon_use_drives_promo as item_coupon_use_drives_promo,
            ipc.enable_prompting as item_enable_prompting,
            ipc.max_redemptions as item_max_redemptions,
            ipc.offer_number_max_redemptions as item_offer_number_max_redemptions,
            ipc.offer_reference_number as item_offer_ref_number,
            ipc.prc_min_qualify_prompt_units,
            ipc.promo_short_desc,
            ipc.prorate_across_qual_and_target,
            ipc.prorate_per_redemption,
            ipc.restrict_to_qualified_units,
            ipc.tag_names,
            ioq.item_promotion_config_pk,
            ioq.promo_condition,
            ioq.qualifier_desc,
			ipc.effective_start_date as item_effective_start_date,
            ipc.effective_end_date as item_effective_end_date
		from
			{{ source("src_promo_silver", "mao_prc_item_promotion_config_v") }} ipc 
			left join {{ source("src_promo_silver", "mao_prc_item_order_qualifiers_v") }} ioq on (ipc.profile_id=ioq.profile_id and ipc.pk=ioq.item_promotion_config_pk)
	),
    promo_stg as (
        select
		    ipr.item_promotion_config_pk as item_promo_config_pk,
			cpc.pk as combo_promo_config_pk,
            cpc.promo_id as promo_id,
            prt.promo_type_id as promo_type_id,
            prt.profile_id as profile_id,
            cpc.event_id as event_id,
            ipr.item_event_id as item_event_id,
            cpc.is_offer_number_generated as is_offer_num_generated,
            cpc.is_offer_number_required as is_offer_num_reqd,						
            ipr.promo_group as promo_grp,
            ipr.promo_desc as promo_desc,		
            ipr.promo_location_group as promo_loc_grp,
            prt.description as desc,
            prt.created_by as created_by,			
            ipr.item_is_offer_number_generated as item_is_offer_num_generated,
            ipr.item_is_offer_number_required as item_is_offer_num_reqd,			
            ipr.item_apply_on_regular_price as item_apply_on_reg_price,
            ipr.item_count_of_offer_number as item_count_of_offer_num,	
            ipr.item_coupon_code as item_coupon_cd,
            ipr.item_coupon_max_redemptions as item_coupon_max_redemptions,
            ipr.item_coupon_use_drives_promo as item_coupon_use_drives_promo,	
            ipr.item_enable_prompting as item_enable_prompting,
            ipr.item_max_redemptions as item_max_redemptions,	
            ipr.item_offer_number_max_redemptions as item_offer_num_max_redemptions,
            ipr.item_offer_reference_number as item_offer_ref_num,	
            ipr.prc_min_qualify_prompt_units as prc_min_qualify_prompt_units,
            ipr.promo_short_desc as promo_short_desc,	
            ipr.prorate_across_qual_and_target as prort_across_qual_and_target,
            ipr.prorate_per_redemption as prort_per_redemption,	
            ipr.restrict_to_qualified_units as restrict_to_qualified_units,
            ipr.tag_names as tag_names,
            ipr.promo_condition as promo_cond,
            ipr.qualifier_desc as qualifier_desc,
            cpc.benefit_bounceback as benefit_bounceback,
            cpc.benefit_fixed_disc_for_group as benefit_fixed_disc_for_grp,
            cpc.benefit_fixed_disc_per_unit as benefit_fixed_disc_per_unit,
            cpc.benefit_fixed_price_for_group as benefit_fixed_price_for_grp,
            cpc.benefit_fixed_price_for_unit as benefit_fixed_price_for_unit,
            cpc.benefit_free_unit as benefit_free_unit,
            cpc.benefit_percentage as benefit_pct,	
            cpc.bounceback_code as bounceback_cd,
            cpc.bounceback_coupon_type as bounceback_coupon_type,
            cpc.bounceback_desc as bounceback_desc,	
            cpc.charge_type as charge_type,
            cpc.count_of_offer_number as count_of_offer_num,
            cpc.coupon_code as coupon_cd,
            cpc.enable_prompting as enable_prompting,
            cpc.exclude_exceptions as exclude_exceptions,
            cpc.exclude_qual_from_target as exclude_qual_from_target,
            cpc.header_level_promo as hdr_level_promo,
            cpc.include_full_price_items as include_full_price_items,
            cpc.include_sale_items as include_sale_items,
            cpc.max_promo_amount as max_promo_amt,
            cpc.max_redemptions as max_redemptions,
            cpc.max_target_amount as max_target_amt,
            cpc.max_target_units as max_target_units,	
            cpc.max_unit_price_for_qual as max_unit_price_for_qual,
            cpc.max_unit_price_for_target as max_unit_price_for_target,	
            cpc.minimize_target_units_used as minimize_target_units_used,
            cpc.min_qual_amount as min_qual_amt,
            cpc.min_qual_units as min_qual_units,
            cpc.min_target_units as min_target_units,
            cpc.min_unit_price_for_qual as min_unit_price_for_qual,
            cpc.min_unit_price_for_target as min_unit_price_for_target,
            cpc.offer_number_max_redemptions as offer_num_max_redemptions,
			cpc.offer_reference_number as offer_ref_num,
			cpc.promo_qual_short_desc as promo_qual_short_desc,
            ipr.item_effective_start_date,
            ipr.item_effective_end_date,
            cpc.effective_start_date,
            cpc.effective_end_date,
            cpc.src_load_ts as src_load_ts
        from 
			{{ source("src_promo_silver", "mao_prc_combo_promotion_config_v") }} cpc
			left join {{ source("src_promo_silver", "mao_prc_combo_order_qualifiers_v") }} coq on (cpc.profile_id=coq.profile_id and cpc.pk=coq.combo_promotion_config_pk)
			left join item_promotion ipr on (cpc.profile_id=ipr.profile_id and cpc.promo_id=ipr.promo_id)
			left join {{ source("src_promo_silver", "mao_prc_promotion_type_v") }} prt on (cpc.profile_id=prt.profile_id and cpc.promo_type=prt.promo_type_id)
    {% if is_incremental() %}
        where
            cpc.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
    {% endif %}
	),
	promo_main as
	(
	select
		src.*
		,{{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk
		,{{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
	from 
		promo_stg src
	)
select
    src.*
    ,current_timestamp()::timestamp_ntz as start_ts
    ,null::timestamp as  end_ts
	,'Y'::varchar(1) as active_flg
	,'Y'::varchar(1) as reporting_flg
    ,{{ v_batch_id }}::decimal(38,0) as batch_id
    ,current_timestamp()::timestamp_ntz as etl_load_ts
    ,current_timestamp()::timestamp_ntz as etl_updt_ts
from promo_main src