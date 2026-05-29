{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = [ "org_id", "ord_id", "ord_ln_id", "rel_id", "rel_ln_id" ] %}

{% set target_context_typ2_column_list =
    ["fulflmnt_dtl_pk","fulflmnt_dtl_id","shipment_id","fulflmnt_id","item_id","status_id","status_desc","alloc_id","pkg_id","pkg_dtl_id","inv_id",
    "dlvry_method_id","dlvry_method_sub_type","ship_from_loc_id","ship_via_id","gift_card_no","gift_card_pin","gift_card_value","carrier_cd",
    "service_level_cd","tracking_num","serial_num","sgtin","channel","ord_qty","cnl_qty","fulfld_qty","is_rejected","cancel_reason_id",
    "cnlled_dt","fulflmnt_dt","shpd_dt","fulflmnt_type","created_by","created_ts","updated_by","updated_ts"] %}

{{ config(
    materialized="incremental",
    unique_key=["hash_sk", "hash_seq_num"],
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"],
    post_hook=[
        "{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
        "{{ fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_ord_mgmt_silver','mao_ord_release_v'), this, 'rel_ln_pk' ) }}",
        "{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"
    ],
    meta={'strategy': "merge", 'update_condition': "active_flg"}
) }}

with
    mao_ord_release_base as (
        select
            reh.pk as release_pk,
            reh.org_id,
            reh.release_id,
            reh.order_pk,
            reh.order_id,
            reh.ship_from_location_id,
            reh.ship_via_id as reh_ship_via_id,
            reh.carrier_code as reh_carrier_code,
            reh.service_level_code as service_level_cd,
            reh.created_by as reh_created_by,
            reh.updated_by as reh_updated_by,
            reh.created_timestamp as reh_created_timestamp,
            reh.updated_timestamp as reh_updated_timestamp,
            rel.pk as release_line_pk,
            rel.release_line_id,
            rel.order_line_id,
            rel.item_id as rel_item_id,
            rel.allocation_id,
            rel.quantity,
            rel.cancelled_quantity,
            rel.fulfilled_quantity,
            rel.cancelled_date,
            rel.created_by as rel_created_by,
            rel.updated_by as rel_updated_by,
            rel.created_timestamp as rel_created_timestamp,
            rel.updated_timestamp as rel_updated_timestamp,
            rel.src_load_ts
        from {{ source('src_ord_mgmt_silver','mao_ord_release_v') }} reh
        join {{ source('src_ord_mgmt_silver','mao_ord_release_line_v') }} rel
            on reh.org_id = rel.org_id and reh.pk = rel.release_pk
        {% if is_incremental() %}
            where
                rel.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
        qualify row_number() over (
            partition by reh.org_id, reh.release_id, rel.release_line_id
            order by rel.updated_timestamp desc, reh.updated_timestamp desc
        ) = 1
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
    ord_line_is_backorder as (
        select ol.org_id, ol.order_pk as ord_pk, ol.pk as ord_ln_pk
        from {{ ref('mao_ord_order_line_t') }} ol
            left join cnl_reason cnl on (ol.org_id=cnl.org_id and ol.pk=cnl.order_line_pk)
        where max_fulfillment_status_id = '1500'
            and coalesce(cnl.cancel_reason_id,'') in ('3000.000','8000.000','13000.000','NoLongerAvailable','CancelUnallocatedUnits','')
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
        left join ord_line_is_backorder bol on (ol.org_id = bol.org_id and ol.order_pk = bol.ord_pk and ol.pk = bol.ord_ln_pk)
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
    mao_ord_fulfillment_detail as (
        select
            fd.pk as fulfillment_detail_pk,
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
            fd.carrier_code,
            fd.service_level_code,
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
		qualify row_number() over (partition by fd.org_id, fd.order_line_pk, fd.release_id,fd.release_line_id order by updated_timestamp desc) = 1
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
    release_joined as (
        select
            rb.org_id,
            rb.release_pk,
            rb.release_id,
            rb.order_pk,
            rb.order_id,
            rb.ship_from_location_id,
            rb.reh_ship_via_id,
            rb.reh_carrier_code,
            rb.service_level_cd,
            rb.reh_created_by,
            rb.reh_updated_by,
            rb.reh_created_timestamp,
            rb.reh_updated_timestamp,
            rb.release_line_pk,
            rb.release_line_id,
            rb.order_line_id,
            rb.rel_item_id,
            rb.allocation_id,
            rb.quantity,
            rb.cancelled_quantity,
            rb.fulfilled_quantity,
            rb.cancelled_date,
            rb.rel_created_by,
            rb.rel_updated_by,
            rb.rel_created_timestamp,
            rb.rel_updated_timestamp,
            rb.src_load_ts,
            ol.pk as ol_pk,
            ol.delivery_method_id,
            ol.delivery_method_sub_type,
            ol.physical_origin_id,
            ol.gift_card_value,
            ol.oh_json_store,
            ol.shipped_date as ol_shipped_date,
            ol.is_backorderFlg,
            al.ship_from_location_id as al_ship_from_location_id,
            coalesce(rb.ship_from_location_id, al.ship_from_location_id, ol.physical_origin_id) as ship_from_loc_id,
            fd.fulfillment_detail_pk,
            fd.order_line_pk as fd_order_line_pk,
            fd.fulfillment_detail_id,
            fd.shipment_id,
            fd.fulfillment_id,
            fd.item_id as fd_item_id,
            fd.status_id,
            fd.package_id,
            fd.package_detail_id,
            fd.ship_via_id as fd_ship_via_id,
            fd.carrier_code as fd_carrier_code,
            fd.service_level_code as fd_service_level_cd,
            fd.gc_number,
            fd.gc_p_i_n,
            fd.gift_card_number,
            fd.serial_number,
            fd.sgtin,
            fd.tracking_number,
            fd.fulfillment_date,
            oi.invoice_id,
            fu.shipped_date as fu_shipped_date,
            ot.shipped_date as ot_shipped_date,
            st.description as status_description,
            cnl.cancel_reason_id,
            cnl.cancel_reason_desc
        from mao_ord_release_base rb
            join mao_ord_order_line ol on rb.org_id = ol.org_id and rb.order_id = ol.order_id and rb.order_line_id = ol.order_line_id
            left join mao_ord_allocation_v al on al.org_id = ol.org_id and al.order_line_pk = ol.pk and al.allocation_id = rb.allocation_id
            left join mao_ord_fulfillment_detail fd on rb.org_id = fd.org_id and rb.release_id = fd.release_id and rb.release_line_id = fd.release_line_id
            left join mao_ord_invoice oi on ol.org_id = oi.org_id and ol.order_pk = oi.order_pk
            left join mao_ful_fulflmnt fu on rb.release_id = fu.fulfillment_id and rb.release_line_id = fu.fulfillment_line_id
            left join mao_ord_order_tracking_info ot on rb.org_id = ot.org_id and fd.tracking_number = ot.tracking_number
            left join mao_ord_status st on fd.status_id::decimal(38, 0) = st.status
            left join cnl_reason cnl on rb.org_id = cnl.org_id and fd.order_line_pk = cnl.order_line_pk
    ),
    release_detail_stg as (
        select
            rj.org_id,
            rj.release_pk as rel_pk,
            rj.release_line_pk as rel_ln_pk,
            rj.fulfillment_detail_pk as fulflmnt_dtl_pk,
            rj.release_id as rel_id,
            rj.release_line_id as rel_ln_id,
            rj.order_id as ord_id,
            rj.order_line_id as ord_ln_id,
            rj.fulfillment_detail_id as fulflmnt_dtl_id,
            rj.shipment_id,
            rj.fulfillment_id as fulflmnt_id,
            rj.rel_item_id as item_id,
            coalesce(rj.status_id, '1000')::decimal(38, 0) as status_id,
            coalesce(rj.status_description, 'CREATED') as status_desc,
            rj.allocation_id as alloc_id,
            rj.package_id as pkg_id,
            rj.package_detail_id as pkg_dtl_id,
            rj.invoice_id as inv_id,
            rj.delivery_method_id as dlvry_method_id,
            rj.delivery_method_sub_type as dlvry_method_sub_type,
            coalesce(wh.wh_str_snum, rj.ship_from_loc_id) as ship_from_loc_id,
            coalesce(rj.fd_ship_via_id, rj.reh_ship_via_id) as ship_via_id,
            rj.gc_number as gift_card_no,
            rj.gc_p_i_n as gift_card_pin,
            rj.gift_card_value,
            coalesce(rj.fd_carrier_code, rj.reh_carrier_code) as carrier_cd,
            coalesce(rj.fd_service_level_cd, rj.service_level_cd) as service_level_cd,
            rj.tracking_number as tracking_num,
            rj.serial_number as serial_num,
            rj.sgtin,
            parse_json(rj.oh_json_store):"Fields":"extend::source"::string as channel,
            rj.quantity as ord_qty,
            rj.cancelled_quantity as cnl_qty,
            rj.fulfilled_quantity as fulfld_qty,
            case when (coalesce(rj.status_id, '1000')::decimal(38, 0)<=3500 and rj.cancelled_quantity > 0 and rj.cancelled_quantity = rj.quantity and (rj.is_backorderFlg = 1 or rj.cancel_reason_id in ('3000.000','8000.000','13000.000','NoLongerAvailable','CancelUnallocatedUnits'))) then 1 else 0 end as is_rejected,
            rj.cancel_reason_id,
            rj.cancelled_date as cnlled_dt,
            rj.fulfillment_date::timestamp_ntz as fulflmnt_dt,
            case when rj.status_id::decimal(38, 0) = 7000 then coalesce(rj.fu_shipped_date, rj.ol_shipped_date, rj.ot_shipped_date)::timestamp_ntz end as shpd_dt,
            case UPPER(loc.loc_type_id) when 'STORE' then 'Store' when 'SUPPLIER' then 'Dropship' when 'DC' then 'DC' end as fulflmnt_type,
            rj.rel_created_by as created_by,
            rj.rel_created_timestamp::timestamp_ntz as created_ts,
            rj.rel_updated_by as updated_by,
            rj.rel_updated_timestamp::timestamp_ntz as updated_ts,
            rj.src_load_ts
        from release_joined rj
            left join {{ source('src_dom_lkp_gold','lkp_mao_warehouses_t') }} wh on wh.org_id = rj.org_id and wh.wh_id = rj.ship_from_loc_id
            left join {{ ref('dim_mao_loc_t') }} loc on loc.active_flg = 'Y' and loc.loc_id = rj.ship_from_loc_id
    ),

    release_detail_main as (
        select
            src.*,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
        from release_detail_stg src
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
    release_detail_main as src
