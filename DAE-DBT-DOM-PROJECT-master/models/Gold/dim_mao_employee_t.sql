{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    ["user_id"]  %}

{% set target_context_typ2_column_list = 
    ["user_type_id", "primary_org_id", "account_type_id", "idnt_type_id", "is_active", "first_name", "last_name", "addr_addr1", "addr_addr2", "addr_addr3", "addr_country", "addr_city", "addr_county", "addr_phone", "addr_postal_cd", "addr_state", "gender", "has_access_to_all_b_us", "has_access_to_all_locs", "user_bus_unit_c", "user_grp_c", "user_loc_sci_c", "user_loc_c", "start_dt", "end_dt"]  %}

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk","hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}"
                    ,"{{  fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_org_silver','mao_org_user_v'), this, 'user_pk' )  }}"
                    ,"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ), 'src_load_ts' ) }}"], 
    meta={'strategy': 'merge', 'update_condition': 'active_flg'}
) }}

with org_user as
	(select
		src.pk as user_pk,
		src.user_id as user_id,
		src.user_type_id as user_type_id,
		src.primary_org_id as primary_org_id,
		src.account_type_id as account_type_id,
		src.identity_type_id as idnt_type_id,
		src.is_active as is_active,
		src.first_name as first_name,
		src.last_name as last_name,
		src.address_address1 as addr_addr1,
		src.address_address2 as addr_addr2,
		src.address_address3 as addr_addr3,
		src.address_country as addr_country,
		src.address_city as addr_city,
		src.address_county as addr_county,
		src.address_phone as addr_phone,
		src.address_postalcode as addr_postal_cd,
		src.address_state as addr_state,
		src.gender as gender,
		src.has_access_to_all_b_us as has_access_to_all_b_us,
		src.has_access_to_all_locations as has_access_to_all_locs,
		src.userbusinessunit_c as user_bus_unit_c,
		src.usergroup_c as user_grp_c,
		src.userlocationsci_c as user_loc_sci_c,
		src.userlocation_c as user_loc_c,
		src.start_date as start_dt,
		src.end_date as end_dt,
		src.src_load_ts as src_load_ts,
	from {{ source('src_org_silver','mao_org_user_v') }} src
    {% if is_incremental() %}
	where 
		src.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
	{% endif %}
	),
	org_user_main as
	(
	select
		src.*
		,{{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk
		,{{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
	from 
		org_user src
	)
select
    src.*
    ,current_timestamp()::timestamp_ntz as start_ts
    ,null::timestamp_ntz as end_ts
	,'Y'::varchar(1) as active_flg
	,'Y'::varchar(1) as reporting_flg
    ,{{ v_batch_id }}::decimal(38,0) as batch_id
    ,current_timestamp()::timestamp_ntz as etl_load_ts
    ,current_timestamp()::timestamp_ntz as etl_updt_ts
from org_user_main src
