{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = [ "org_id", "ord_id", "ord_ln_id", "fulflmnt_dtl_id" ] %}

{% set target_context_typ2_column_list = 
    ["shipment_id","fulflmnt_id","item_id","status_id","status_desc","rel_id","rel_ln_id","alloc_id","pkg_id","pkg_dtl_id","inv_id","dlvry_method_id",
	"dlvry_method_sub_type","cancel_reason_id","ship_from_loc_id","ship_via_id","gift_card_no","gift_card_pin","gift_card_value","carrier_cd","tracking_num",
	"serial_num","sgtin","channel","fulflmnt_type","is_rejected","ord_qty","cnl_qty","fulfld_qty","cnlled_dt","fulflmnt_dt","shpd_dt","rel_created_ts","created_by",
	"created_ts","updated_by","updated_ts"] %}

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=[
        "{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
        "{{ fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_ord_mgmt_silver','mao_ord_fulfillment_detail_v'), this, 'fulflmnt_dtl_pk' ) }}",
		"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"
    ], 
    meta={'strategy': "merge", 'update_condition': "active_flg"}
) }}

with
    mao_ord_fulfillment_detail as (
        select
            fd.pk,
            fd.order_line_pk,
            fd.org_id,
            fd.fulfillment_detail_id,
            fd.fulfillment_id,
            fd.shipment_id,
            fd.item_id,
            fd.status_id,
            fd.release_id,
            fd.release_line_id,
            fd.package_id,
            fd.package_detail_id,
            fd.ship_via_id,
            fd.gc_number,
            fd.gc_p_i_n,
            fd.gift_card_number,
            fd.serial_number,
            fd.sgtin,
            fd.tracking_number,
            fd.created_by,
            fd.updated_by,
            fd.fulfillment_date,
            fd.updated_timestamp,
            fd.created_timestamp,
            fd.src_load_ts
        from {{ source('src_ord_mgmt_silver','mao_ord_fulfillment_detail_v') }} fd
        {% if is_incremental() %}
            where
                fd.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
    ),
    mao_ord_status as (
        select status, description
        from {{ source('src_ord_mgmt_silver','mao_fw_status_definition_v') }}
        where lower(profile_id) = 'fl-us'
    ),
    mao_ord_invoice as (
        select org_id, order_pk, invoice_id
        from {{ source('src_ord_mgmt_silver','mao_ord_invoice_v') }}
        qualify row_number() over (partition by org_id, order_pk order by updated_timestamp desc) = 1
    ),
    mao_ord_order_tracking_info as (
        select org_id, order_pk, tracking_number, shipped_date
        from {{ source('src_ord_mgmt_silver','mao_ord_order_tracking_info_v') }}
        qualify row_number() over (partition by org_id, tracking_number order by updated_timestamp desc) = 1
    ),
    mao_ord_order_shipped_dt as (
        select org_id, order_pk, actual_time as shipped_date
        from {{ source('src_ord_mgmt_silver','mao_ord_order_milestone_v') }}
        where lower(milestone_definition_id) = 'order::milestone::shipped'
        qualify row_number() over (partition by org_id, order_pk order by updated_timestamp desc) = 1
    ),
    ord_line_is_backorder as (
        select org_id, order_pk as ord_pk, pk as ord_ln_pk
        from {{ ref('mao_ord_order_line_t') }}
        where max_fulfillment_status_id = '1500'
        group by all
    ),
    mao_ord_order_line as (
        select
            ol.pk,
            ol.org_id,
            ol.order_pk,
            ol.order_id,
            ol.order_line_id,
            ol.delivery_method_id,
            ol.delivery_method_sub_type,
            ol.physical_origin_id,
            ol.gift_card_value,
            oh.json_store as oh_json_store,
            os.shipped_date,
            case when bol.ord_ln_pk is not null then 1 else 0 end as is_backorderFlg
        from {{ source('src_ord_mgmt_silver','mao_ord_order_line_v') }} ol
        join (
            select pk, org_id, json_store
            from {{ source('src_ord_mgmt_silver','mao_ord_order_v') }}
        ) oh on (oh.org_id = ol.org_id and oh.pk = ol.order_pk)
        left join ord_line_is_backorder bol on (ol.org_id = bol.org_id and ol.order_pk = bol.ord_pk and ol.pk=bol.ord_ln_pk)
        left join mao_ord_order_shipped_dt os on oh.org_id = os.org_id and oh.pk = os.order_pk
        qualify row_number() over (partition by ol.org_id, ol.order_id, ol.order_line_id order by ol.updated_timestamp desc) = 1
    ),
    mao_ord_allocation_v as (
        select
            org_id,
            order_line_pk,
            allocation_id,
            ship_from_location_id
        from {{ source('src_ord_mgmt_silver','mao_ord_allocation_v') }}
        qualify row_number() over (partition by org_id, order_line_pk, allocation_id order by updated_timestamp desc) = 1
    ),
    mao_ord_release as (
        select
            reh.org_id,
            reh.release_id as rel_id,
            rel.release_line_id as rel_ln_id,
            reh.order_id as ord_id,
            rel.order_line_id as ord_ln_id,
            coalesce(reh.ship_from_location_id, al.ship_from_location_id) as ship_from_loc_id,
            rel.allocation_id,
            rel.quantity,
            rel.cancelled_quantity,
            rel.fulfilled_quantity,
            rel.cancelled_date,
            reh.created_timestamp as rel_created_ts
        from {{ source('src_ord_mgmt_silver','mao_ord_release_v') }} reh
        join {{ source('src_ord_mgmt_silver','mao_ord_release_line_v') }} rel
            on reh.org_id = rel.org_id and reh.pk = rel.release_pk
        join mao_ord_order_line ol
            on rel.org_id = ol.org_id and reh.order_id = ol.order_id and rel.order_line_id = ol.order_line_id
        left join mao_ord_allocation_v al
            on al.org_id = ol.org_id and al.order_line_pk = ol.pk and al.allocation_id = rel.allocation_id
        qualify row_number() over (
            partition by reh.org_id, reh.release_id, rel.release_line_id
            order by rel.updated_timestamp desc, reh.updated_timestamp desc
        ) = 1
    ),
    mao_ful_fulflmnt as (
        select
            fl.org_id,
            fh.fulfillment_id,
            fl.fulfillment_line_id,
            fh.shipped_date
        from {{ source('src_ord_fulflmnt_silver','mao_ful_fulfillment_line_v') }} fl
        join (
            select pk, org_id, fulfillment_id, shipped_date
            from {{ source('src_ord_fulflmnt_silver','mao_ful_fulfillment_v') }}
        ) fh on fh.org_id = fl.org_id and fh.pk = fl.fulfillment_pk
        qualify row_number() over (partition by fl.org_id, fh.fulfillment_id, fl.fulfillment_line_id order by fl.updated_timestamp desc) = 1
    ),
    cnl_reason as (
        select
            ch.org_id,
            ch.order_line_pk,
            ch.cancel_reason as cancel_reason_id,
            re.description as cancel_reason_desc
        from {{ source('src_ord_mgmt_silver','mao_ord_order_line_cancel_history_v') }} ch
        join (
            select reason_id, description
            from {{ source('src_ord_mgmt_silver','mao_ord_reason_v') }}
            where lower(profile_id) = 'fl-us'
        ) re on ch.cancel_reason = re.reason_id
        qualify row_number() over (partition by ch.org_id, ch.order_line_pk order by ch.updated_timestamp desc) = 1
    ),
    fulfillment_detail_stg as (
        select
            fd.org_id,
            fd.pk as fulflmnt_dtl_pk,
            fd.fulfillment_detail_id as fulflmnt_dtl_id,
            ol.order_id as ord_id,
            ol.order_line_id as ord_ln_id,
            fd.shipment_id,
            fd.fulfillment_id as fulflmnt_id,
            fd.item_id,
            fd.status_id::decimal(38, 0) as status_id,
            st.description as status_desc,
            fd.release_id as rel_id,
            fd.release_line_id as rel_ln_id,
            rel.allocation_id as alloc_id,
            fd.package_id as pkg_id,
            fd.package_detail_id as pkg_dtl_id,
            oi.invoice_id as inv_id,
            ol.delivery_method_id as dlvry_method_id,
            ol.delivery_method_sub_type as dlvry_method_sub_type,
            coalesce(rel.ship_from_loc_id, ol.physical_origin_id) as ship_from_loc_id,
            fd.ship_via_id,
            fd.gc_number as gift_card_no,
            fd.gc_p_i_n as gift_card_pin,
            ol.gift_card_value,
            fd.ship_via_id as carrier_cd,
            fd.tracking_number as tracking_num,
            fd.serial_number as serial_num,
            fd.sgtin,
            parse_json(ol.oh_json_store):"Fields":"extend::source"::string as channel,
            rel.quantity as ord_qty,
            rel.cancelled_quantity as cnl_qty,
            rel.fulfilled_quantity as fulfld_qty,
            case when rel.cancelled_quantity != 0 and (ol.is_backorderFlg = 1 or cnl.cancel_reason_id in ('3000.000','8000.000','13000.000','NoLongerAvailable')) then 1 else 0 end as is_rejected,
            cnl.cancel_reason_id,
            rel.cancelled_date as cnlled_dt,
            fd.fulfillment_date::timestamp_ntz as fulflmnt_dt,
            case when fd.status_id::decimal(38, 0) = 7000 then coalesce(fu.shipped_date, ol.shipped_date, ot.shipped_date)::timestamp_ntz end as shpd_dt,
            case when fu.fulfillment_id is not null then 'Store' else 'DC' end as fulflmnt_type,
            rel.rel_created_ts::timestamp_ntz as rel_created_ts,
            fd.created_by,
            fd.created_timestamp::timestamp_ntz as created_ts,
            fd.updated_by,
            fd.updated_timestamp::timestamp_ntz as updated_ts,
            fd.src_load_ts
        from mao_ord_fulfillment_detail fd
            join mao_ord_order_line ol on fd.org_id = ol.org_id and fd.order_line_pk = ol.pk
            left join mao_ord_invoice oi on ol.org_id = oi.org_id and ol.order_pk = oi.order_pk
            left join mao_ful_fulflmnt fu on fd.release_id = fu.fulfillment_id and fd.release_line_id = fu.fulfillment_line_id
            left join mao_ord_order_tracking_info ot on fd.org_id = ot.org_id and fd.tracking_number = ot.tracking_number
            left join mao_ord_status st on fd.status_id::decimal(38, 0) = st.status
            left join mao_ord_release rel on fd.org_id = rel.org_id and fd.release_id = rel.rel_id and fd.release_line_id = rel.rel_ln_id
            left join cnl_reason cnl on fd.org_id = cnl.org_id and fd.order_line_pk = cnl.order_line_pk
    ),
    fulfillment_detail_main as (
        select
            src.*,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
        from fulfillment_detail_stg src
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
    fulfillment_detail_main as src
