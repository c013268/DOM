{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "supply_pk", "supply_alloc_pk" ]  %}
	
{% set target_context_typ2_column_list = 
    [ "segment_id", "oh_avail_supply_qty", "oh_unavail_supply_qty", "rf_supply_qty", "oo_supply_qty", "it_supply_qty", "received_qty", "allocated_qty", "last_txn_dt", "supply_updtd_ts", "alloc_updtd_ts", "supply_created_ts", "alloc_created_ts" ]  %}	

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
					"{{  fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_inv_mgmt_silver','mao_inv_supply_v'), this, 'supply_pk' ) }}",
					"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'format': "iceberg", 'strategy': "merge", 'update_condition': "active_flg"}
) }}

with 
    mao_inv_supply as (        
        select a.*
        from {{ source('src_inv_mgmt_silver', 'mao_inv_supply_v') }} a
        {% if is_incremental() %}
            where
                a.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
    ),
    inv_supply_stg as (
        select
            -- pks
            a.pk as supply_pk,
            nvl(b.pk, -1) as supply_alloc_pk,
            -- IDs
            a.profile_id as supply_profile_id,
            b.profile_id as alloc_profile_id,
            a.reference_id as ord_id,
            a.parent_reference_id as parent_ord_id,
            a.item_id,
            a.reference_type_id as ref_type_id,
            a.location_id as loc_id,
            a.origin_location_id as origin_loc_id,
            a.inventory_transaction_id as inv_txn_id,
            a.segment_id,
            a.parent_reference_type_id as parent_ref_type_id,
            a.parent_reference_detail_id as parent_ref_detail_id,
            a.reference_detail_id as ref_detail_id,
            c.loc_type_id,
            c.loc_sub_type_id,
            -- Flags
            a.is_kits_supply,
            a.is_infinite_supply,
            -- attributes
            a.supply_type,
            a.inventory_transaction_type as inv_txn_type,
            a.inventory_transaction_value as inv_txn_value,
            a.error_code as error_cd,
            a.inventory_type as inv_type,
            a.external_sequence_number as external_seq_num,
            a.country_of_origin,
            a.uom,
            a.eta,
            a.product_status as prd_status,
            a.inventory_attribute1 as inv_attribute1,
            a.inventory_attribute2 as inv_attribute2,
            a.inventory_attribute3 as inv_attribute3,
            a.inventory_attribute4 as inv_attribute4,
            a.inventory_attribute5 as inv_attribute5,
            a.last_transaction_type as last_txn_type,
            a.pending_review,
            a.process as supply_process,
            b.process as alloc_process,
            -- metrics 
            case when lower(a.supply_type) in ('on hand available soon','on hand available') then a.quantity else 0 end as oh_avail_supply_qty,
            case when lower(a.supply_type) in ('on hand unavailable') then a.quantity else 0 end as oh_unavail_supply_qty,
            case when lower(a.supply_type) = 'ecom ring fence' then a.quantity else 0 end as rf_supply_qty,
            case when lower(a.supply_type)  = 'on order' then a.quantity else 0 end as oo_supply_qty,
            case when lower(a.supply_type)  = 'in transit' then a.quantity else 0 end as it_supply_qty,
            a.received_quantity as received_qty,
            b.allocated_quantity as allocated_qty,
            -- dates
            a.last_txn_date::timestamp_ntz as last_txn_dt,
            --ts
            a.updated_timestamp::timestamp_ntz as supply_updtd_ts,
            b.updated_timestamp::timestamp_ntz as alloc_updtd_ts,
            a.created_timestamp::timestamp_ntz as supply_created_ts,
            b.created_timestamp::timestamp_ntz as alloc_created_ts,
            a.src_load_ts::timestamp_ntz as src_load_ts
        from 
            mao_inv_supply a 
            left join {{ source('src_inv_mgmt_silver', 'mao_inv_supply_allocation_v') }} b on a.pk = b.supply_pk
            left join {{ ref('dim_mao_loc_t') }} c on a.location_id = c.loc_id and c.active_flg = 'Y'        
    ),
	inv_supply_hash as (
    select
        src.*,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list)}} as hash_seq_num
    from
        inv_supply_stg as src
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
    inv_supply_hash as src