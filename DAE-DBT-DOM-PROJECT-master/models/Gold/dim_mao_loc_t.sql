{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}
{% set target_key_column_list = ["loc_id"]  %}
 
{% set target_context_typ2_column_list = 
    ["loc_status_id", "loc_type_id", "prnt_org_id", "loc_sub_type_id", "bus_unit_id", "bus_unit_grp_id", "loc_contact_id", "loc_contact_type_id", "loc_name", 
	"loc_status_name", "loc_status_desc", "loc_type_name", "loc_type_desc", "loc_sub_type_name", "loc_sub_type_desc","loc_contact_type_name", "loc_contact_type_desc", 
	"loc_addr1", "loc_addr2", "loc_addr_city", "loc_addr_country", "loc_addr_email", "loc_addr_first_name", "loc_addr_last_name", 
	"loc_addr_phone_no", "loc_addr_postal_cd", "loc_addr_state"]  %}
	
{{ config(
    materialized="incremental", 
    unique_key=["hash_sk","hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}"
                    ,"{{  fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_org_silver','mao_org_location_v'), this, 'loc_pk' )  }}"
                    ,"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ), 'src_load_ts' ) }}"], 
    meta={'strategy': 'merge', 'update_condition': 'active_flg'}
) }}
with 
mao_org_location as (
    select lo.*
    from {{ source('src_org_silver','mao_org_location_v')}} lo
    {% if is_incremental() %}
        where
            lo.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
    {% endif %}
),
loc_t as
 (select
  lo.pk as loc_pk,
  lo.location_id as loc_id,
  lo.location_status_id as loc_status_id,  
  lo.location_type_id as loc_type_id,  
  lo.parent_org_id as prnt_org_id,  
  lo.location_sub_type_id as loc_sub_type_id,  
  null as bus_unit_id,
  null as bus_unit_grp_id,
  --bu.business_unit_id,
  --bu.business_unit_group_id,  
  lc.location_contact_id as loc_contact_id,
  lc.location_contact_type_id as loc_contact_type_id,   
  lo.location_name as loc_name,
  ls.location_status_name as loc_status_name,
  ls.location_status_desc as loc_status_desc,
  lt.location_type_name as loc_type_name,
  lt.location_type_desc as loc_type_desc,
  lst.location_sub_type_name as loc_sub_type_name,
  lst.location_sub_type_desc as loc_sub_type_desc,
  lct.location_contact_type_name as loc_contact_type_name,
  lct.location_contact_type_desc as loc_contact_type_desc,
  lo.address_address1 as loc_addr1,
  lo.address_address2 as loc_addr2,
  lo.address_city as loc_addr_city,
  lo.address_country as loc_addr_country,
  lo.address_email as loc_addr_email,
  lo.address_firstname as loc_addr_first_name,
  lo.address_lastname as loc_addr_last_name,
  lo.address_phone as loc_addr_phone_no,
  lo.address_postalcode as loc_addr_postal_cd,
  lo.address_state as loc_addr_state,
  lo.src_load_ts as src_load_ts
 from 
  mao_org_location lo 
  left join {{ source('src_org_silver','mao_org_location_status_v') }}  ls  on (lo.location_status_id = ls.location_status_id )
  left join {{ source('src_org_silver','mao_org_location_type_v') }} lt  on (lo.location_type_id = lt.location_type_id )
  left join {{ source('src_org_silver','mao_org_location_sub_type_v') }} lst  on (lst.profile_id='FL-INC-NA' and lo.location_sub_type_id = lst.location_sub_type_id )
  left join {{ source('src_org_silver','mao_org_location_contact_v') }} lc  on (lc.location_contact_id not like 'Test%' and lo.pk = lc.location_pk )
  left join {{ source('src_org_silver','mao_org_location_contact_type_v') }} lct  on (lc.location_contact_type_id = lct.location_contact_type_id )
  --left join {{ source('src_org_silver','mao_org_business_unit_v') }} bu  on (oh.organization_pk = o.pk)
 ),
 loc_main as
 (
 select
  src.*
  ,{{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk
  ,{{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
 from 
  loc_t src
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
from loc_main src