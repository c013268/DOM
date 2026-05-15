{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}
 
{% set target_key_column_list =
    [ "org_id", "prnt_org_id", "child_org_id" ]  %}
   
{% set target_context_typ2_column_list =
    [ "shipper_org_id", "org_type_id", "is_managed_org", "is_enterprise_org", "is_base_img_provider", "org_name", "web_str_url" ]  %}
 
{{ config(
    materialized="incremental", 
    unique_key=["hash_sk","hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}"
                    ,"{{ fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_org_silver','mao_org_organization_v'), this, 'org_pk' ) }}"
                    ,"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ), 'src_load_ts' ) }}"], 
    meta={'strategy': 'merge', 'update_condition': 'active_flg'}
) }}
 
with org_t as
    (select
        o.pk as org_pk,
        oh.pk as org_hier_pk,
        o.organization_id as org_id,
        oh.parent_organization_id as prnt_org_id,
        oh.child_organization_id as child_org_id,
        o.shipper_organization_id as shipper_org_id,
        o.organization_type_id as org_type_id,
        o.is_managed_organization as is_managed_org,
        o.is_enterprise_org as is_enterprise_org,
        o.is_base_image_provider as is_base_img_provider,
        o.organization_name as org_name,
        o.web_store_url as web_str_url,
        o.src_load_ts as src_load_ts
    from
        {{ source('src_org_silver','mao_org_organization_v')}} o
        left join {{ source('src_org_silver','mao_org_organization_hierarchy_v') }} oh  on (oh.organization_pk = o.pk)
    {% if is_incremental() %}
        where
            o.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
    {% endif %}
    ),
    org_main as
    (
    select
        src.*
        ,{{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk
        ,{{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
    from
        org_t src
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
from org_main src
