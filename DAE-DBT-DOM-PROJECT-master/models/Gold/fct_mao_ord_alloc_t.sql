{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "alloc_pk","alloc_id","ord_id","ord_ln_id","org_id"]  %}

{% set target_context_typ2_column_list = 
    [ "rel_id","rel_ln_id","resrv_req_id","resrv_req_dtl_id","alloc_status_id","supply_profile_id","po_id","po_dtl_id",
		"ship_from_loc_id","alloc_dependency_id","inv_type_id","asn_dtl_id","asn_id","grp_id","ship_to_loc_id","ship_via_id",
		"substitution_type_id","inv_segment_id","prd_status_id","carrier_cd","is_virtual","resrv_version","all_or_none",
		"alloc_on","alloc_type","demand_type","cntry_of_origin","inv_attr1","inv_attr2","inv_attr3","inv_attr4","inv_attr5",
		"loc_backlog","ord_open_qty","ord_rel_qty","qty","committed_dlvry_dt","committed_ship_dt","earliest_dlvry_dt",
		"earliest_ship_dt","latest_rel_dt","latest_ship_dt","predicted_dlvry_dt","predicted_ship_dt","rel_dt","resrv_exp_dt",
		"alloc_open_ts","alloc_release_ts" ]  %}
	
{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
					"{{ fl_utils.m_apply_typ1_deletion_on_tgt_model( source('src_ord_mgmt_silver','mao_ord_allocation_v'), this, 'alloc_pk')   }}",
					"{{ fl_utils.m_upd_typ2_status_hist_flg_records( this ,'hash_sk' ,'hash_seq_num', 'alloc_status_id') }}",
					"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge", 'update_condition': "active_flg"}
) }}

with 
    mao_ord_allocation_v as (
        select
            alo.pk,
            alo.order_line_pk,
            alo.org_id,
            alo.allocation_id,
            case when alo.status_id != '' then alo.status_id end::string as status_id,
            alo.reservation_request_id,
            alo.reservation_request_detail_id,
            alo.po_id,
            alo.po_detail_id,
            alo.ship_from_location_id,
            alo.allocation_dependency_id,
            alo.inventory_type_id,
            alo.asn_detail_id,
            alo.asn_id,
            alo.group_id,
            alo.ship_to_location_id,
            alo.ship_via_id,
            alo.substitution_type_id,
            alo.inventory_segment_id,
            alo.product_status_id,
            alo.carrier_code,
            alo.is_virtual,
            alo.quantity,
            alo.allocated_on,
            alo.allocation_type,
            alo.country_of_origin,
            alo.location_backlog,
            alo.inventory_attribute1,
            alo.inventory_attribute2,
            alo.inventory_attribute3,
            alo.inventory_attribute4,
            alo.inventory_attribute5,
            alo.committed_delivery_date,
            alo.committed_ship_date,
            alo.earliest_delivery_date,
            alo.earliest_ship_date,
            alo.latest_release_date,
            alo.latest_ship_date,
            alo.predicted_delivery_date,
            alo.predicted_ship_date,
            alo.release_date,
            alo.created_timestamp,
            alo.src_load_ts
        from {{ source('src_ord_mgmt_silver','mao_ord_allocation_v') }} alo
        {% if is_incremental() and not var('full_load', false) %}
            where
                alo.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
    ),
    mao_ord_release as (
        select
            reh.org_id,
            reh.release_id as rel_id,
            rel.release_line_id as rel_ln_id,
            rel.allocation_id as alloc_id,
            reh.order_id as ord_id,
            rel.order_line_id as ord_ln_id
        from {{ source('src_ord_mgmt_silver','mao_ord_release_v') }} reh
        join {{ source('src_ord_mgmt_silver','mao_ord_release_line_v') }} rel
            on reh.org_id = rel.org_id and reh.pk = rel.release_pk
        qualify row_number() over (
            partition by reh.org_id, reh.order_id, rel.order_line_id
            order by rel.updated_timestamp desc, reh.updated_timestamp desc
        ) = 1
    ),
    mao_ord_allocation as (
        select distinct
            alo.pk as alloc_pk
            ,alo.org_id as org_id
            ,alo.allocation_id as alloc_id
            ,orl.ord_id as ord_id
            ,orl.ord_ln_id as ord_ln_id
            ,rel.rel_id
            ,rel.rel_ln_id
            ,alo.reservation_request_id as resrv_req_id
            ,alo.reservation_request_detail_id as resrv_req_dtl_id
            ,alo.status_id as alloc_status_id
            ,irr.supply_profile_id as supply_profile_id
            ,alo.po_id as po_id
            ,alo.po_detail_id as po_dtl_id
            ,alo.ship_from_location_id as ship_from_loc_id
            ,alo.allocation_dependency_id as alloc_dependency_id
            ,alo.inventory_type_id as inv_type_id
            ,alo.asn_detail_id as asn_dtl_id
            ,alo.asn_id as asn_id
            ,alo.group_id as grp_id
            ,alo.ship_to_location_id as ship_to_loc_id
            ,alo.ship_via_id as ship_via_id
            ,alo.substitution_type_id as substitution_type_id
            ,alo.inventory_segment_id as inv_segment_id
            ,alo.product_status_id as prd_status_id
            ,alo.carrier_code as carrier_cd
            ,alo.is_virtual as is_virtual
            ,irr.reservation_version as resrv_version
            ,irr.all_or_none as all_or_none
            ,alo.allocated_on::timestamp_ntz as alloc_on
            ,alo.allocation_type as alloc_type
            ,irr.demand_type as demand_type
            ,alo.country_of_origin as cntry_of_origin
            ,alo.inventory_attribute1 as inv_attr1
            ,alo.inventory_attribute2 as inv_attr2
            ,alo.inventory_attribute3 as inv_attr3
            ,alo.inventory_attribute4 as inv_attr4
            ,alo.inventory_attribute5 as inv_attr5
            ,alo.location_backlog as loc_backlog
            ,case when lower(alo.status_id) = 'open' then alo.quantity else 0 end as ord_open_qty
            ,case when lower(alo.status_id) = 'released' then alo.quantity else 0 end as ord_rel_qty
            ,alo.quantity as qty
            ,alo.committed_delivery_date::timestamp_ntz as committed_dlvry_dt
            ,alo.committed_ship_date::timestamp_ntz as committed_ship_dt
            ,alo.earliest_delivery_date::timestamp_ntz as earliest_dlvry_dt
            ,alo.earliest_ship_date::timestamp_ntz as earliest_ship_dt
            ,alo.latest_release_date::timestamp_ntz as latest_rel_dt
            ,alo.latest_ship_date::timestamp_ntz as latest_ship_dt
            ,alo.predicted_delivery_date::timestamp_ntz as predicted_dlvry_dt
            ,alo.predicted_ship_date::timestamp_ntz as predicted_ship_dt
            ,alo.release_date::timestamp_ntz as rel_dt
            ,irr.reservation_expiry_date::timestamp_ntz as resrv_exp_dt
            ,case when lower(alo.status_id) = 'open' then alo.created_timestamp::timestamp_ntz else null end as alloc_open_ts
            ,case when lower(alo.status_id) = 'released' then alo.created_timestamp::timestamp_ntz else null end as alloc_release_ts
            ,alo.src_load_ts as src_load_ts
        from mao_ord_allocation_v alo
            join {{ ref('fct_mao_ord_line_t') }} orl
                on alo.org_id = orl.org_id and alo.order_line_pk = orl.ord_ln_pk and orl.active_flg = 'Y'
            left join mao_ord_release rel
                on alo.org_id = rel.org_id and orl.ord_ln_id = rel.ord_ln_id and alo.allocation_id = rel.alloc_id
            left join (
                select org_id, request_id, supply_profile_id, reservation_version, all_or_none, demand_type, reservation_expiry_date
                from {{ source('src_inv_mgmt_silver','mao_inv_reservation_request_v') }}
                where is_confirmed = 1
            ) irr on alo.org_id = irr.org_id and alo.reservation_request_id = irr.request_id
    ),
    mao_ord_allocation_hash as (
        select
            src.*,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
        from
            mao_ord_allocation as src
    )
select
    src.*,
    current_timestamp()::timestamp_ntz as start_ts,
    null::timestamp_ntz as end_ts,
    'Y'::varchar(1) as active_flg,
    'Y'::varchar(1) as reporting_flg,
    'N'::varchar(1) as status_hist_flg,
    {{ v_batch_id }}::number(38, 0) as batch_id,
    current_timestamp()::timestamp_ntz as etl_load_ts,
    current_timestamp()::timestamp_ntz as etl_updt_ts
from
    mao_ord_allocation_hash as src