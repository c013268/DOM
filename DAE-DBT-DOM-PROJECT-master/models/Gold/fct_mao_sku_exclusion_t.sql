{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "org_id", "exclusion_id" ]  %}

{% set target_context_typ2_column_list = 
    [ "style_id", "color_id", "cat_id", "brand_id", "vendor_id", "department_id", "exclusion_level", "prnt_org_id", "exclusion_type", "environment", "exclusion_start_dt", "exclusion_end_dt", "status", "updated_by", "updated_ts" ]  %}

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
					"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge", 'update_condition': "active_flg"}
) }}

with sku_exclusion_stg as (
    select distinct
        exclusion_id,
        styleid as style_id,
        colorid as color_id,
        cat_id,
        brand_id,
        vendor_id,
        department_id,
        exclusion_level,
        org_id,
        parent_org_id as prnt_org_id,
        exclusion_type,
        environment,
        exclusion_start_date as exclusion_start_dt,
        exclusion_end_date as exclusion_end_dt,
        created_by,
        created_at::timestamp_ntz as created_ts,
        updated_by,
        updated_at::timestamp_ntz as updated_ts,
        status,
        coalesce(updated_at, created_at) as src_load_ts
    from {{ source('src_inv_mgmt_silver', 'mao_sku_exclusion_v') }}
    {% if is_incremental() %}
        where coalesce(updated_at, created_at) > {{ v_inc_load_ts }}
    {% endif %}
),
sku_exclusion_hash as (
    select
        src.*,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list)}} as hash_seq_num
    from
        sku_exclusion_stg as src
)
select
    src.*,
    current_timestamp()::timestamp_ntz as start_ts,
    null::timestamp_ntz as end_ts,
    'Y'::varchar(1) as active_flg,
    'Y'::varchar(1) as reporting_flg,
    {{ v_batch_id }}::number(38, 0) as batch_id,
    current_timestamp()::timestamp_ntz as etl_load_ts,
    current_timestamp()::timestamp_ntz as etl_updt_ts
from
    sku_exclusion_hash as src