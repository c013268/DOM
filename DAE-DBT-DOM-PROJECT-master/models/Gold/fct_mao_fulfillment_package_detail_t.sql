{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    ["org_id", "pkg_id", "pkg_dtl_id"]  %}
	
{% set target_context_typ2_column_list = 
    ["pkg_status","pkg_status_desc","ord_id","pkg_type_id","fulflmnt_id","fulflmnt_ln_id","pickup_fulflmnt_id","item_id","shipment_id","shipment_type","ship_from_loc_id","ship_from_loc_type",
	"short_reason_id","crnt_location_id","tracking_num","rtn_tracking_num","inv_id","inv_ln_id","inv_txn_ref_id","inv_type","inv_created_by","inv_version","task_id","task_type_id",
	"task_type_name","task_status_id","task_status_name","ship_to_addr_addr1","ship_to_addr_addr2","ship_to_addr_addr3","ship_to_addr_city","ship_to_addr_country","ship_to_addr_county",
	"ship_to_addr_email","ship_to_addr_firstname","ship_to_addr_lastname","ship_to_addr_phone","ship_to_addr_postal_cd","ship_to_addr_state","dlvry_type","dlvry_status","carrier_barcode",
	"service_level_cd","is_hazardous","volume_qty","item_desc","weight_uom","serial_num","short_qty_uom","gift_card_num","rcvd_qty_uom","country_of_origin","task_assigned_to",
	"task_updtd_by","task_picked_qty","task_qty","gross_weight_qty","gross_weight_uom","gross_volume_qty","gross_volume_uom","qty_uom","volume_uom","weight_qty","gift_card_pin","pymt_ccy_code",
	"qty","rcvd_qty","gift_card_value","short_qty","inv_total","inv_sub_total","inv_total_chrgs","inv_total_discs","invoice_failed_amt","task_status_ts","inv_fulflmnt_dt","inv_created_ts",
	"estimated_dlvry_dt","packed_dttm","shipped_dttm","rcvd_dttm","created_by","created_ts","updated_by","updated_ts"]  %}

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk","hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}"
                    ,"{{ fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_ord_fulflmnt_silver','mao_ful_package_detail_v'), this, 'pkg_dtl_pk' )  }}"
                    ,"{{ fl_utils.m_upd_typ2_status_hist_flg_records( this ,'hash_sk' ,'hash_seq_num', 'pkg_status') }}"
					,"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ), 'src_load_ts' ) }}"], 
    meta={'strategy': 'merge', 'update_condition': 'active_flg'}
) }}

with 
    mao_ful_packages as (
        select
            ph.pk,
            ph.org_id,
            ph.package_id,
            ph.package_type_id,
            ph.package_status,
            ph.order_id,
            ph.shipment_id,
            ph.shipment_type,
            ph.ship_from_location_id,
            ph.current_location_id,
            ph.tracking_number,
            ph.return_tracking_number,
            ph.pickup_fulfillment_id,
            ph.task_id,
            ph.ship_to_address_address1,
            ph.ship_to_address_address2,
            ph.ship_to_address_address3,
            ph.ship_to_address_city,
            ph.ship_to_address_country,
            ph.ship_to_address_county,
            ph.ship_to_address_email,
            ph.ship_to_address_firstname,
            ph.ship_to_address_lastname,
            ph.ship_to_address_phone,
            ph.ship_to_address_postalcode,
            ph.ship_to_address_state,
            ph.delivery_type,
            ph.delivery_status,
            ph.carrier_barcode,
            ph.service_level_code,
            ph.gross_weight_qty,
            ph.gross_weight_uom,
            ph.gross_volume_qty,
            ph.gross_volume_uom,
            ph.estimated_delivery_date,
            ph.packed_date_time,
            ph.shipped_date_time,
            ph.received_date_time,
            ph.created_by,
            ph.created_timestamp,
            ph.updated_by,
            ph.updated_timestamp,
            ph.src_load_ts
        from {{ ref('mao_ful_packages_t') }} ph
        {% if is_incremental() %}
            where
                ph.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
    ),
    mao_ful_package_detail as (
        select
            pd.pk,
            pd.packages_pk,
            pd.package_detail_id,
            pd.org_id,
            pd.fulfillment_id,
            pd.fulfillment_line_id,
            pd.item_id,
            pd.short_reason_id,
            pd.is_hazardous,
            pd.volume_qty,
            pd.item_description,
            pd.weight_uom,
            pd.serial_number,
            pd.short_quantity_uom,
            pd.gift_card_number,
            pd.received_quantity_uom,
            pd.country_of_origin,
            pd.quantity_uom,
            pd.volume_uom,
            pd.weight_qty,
            pd.gift_card_pin,
            pd.quantity_qty,
            pd.received_quantity_qty,
            pd.gift_card_value,
            pd.short_quantity_qty,
            pd.src_load_ts
        from {{ source('src_ord_fulflmnt_silver','mao_ful_package_detail_v')}} pd
        where exists (
            select 1 from mao_ful_packages ph
            where ph.org_id = pd.org_id and ph.pk = pd.packages_pk
        )
    ),
    ord_inv_stg as (
        select
            ivh.org_id,
            ivh.pk as ivh_pk,
            ivl.pk as ivl_pk,
            ivh.package_id as pkg_id,
            ivl.package_detail_id as pkg_dtl_id,
            ivl.item_id,
            ivh.invoice_id as inv_id,
            ivl.invoice_line_id as inv_ln_id,
            ivh.transaction_reference_id,
            ivh.invoice_type,
            ivh.created_by,
            ivh.version,
            ivh.invoice_total,
            ivh.invoice_sub_total,
            ivh.total_charges,
            ivh.total_discounts,
            ivh.failed_amount,
            ivh.fulfillment_date,
            ivh.created_timestamp
        from {{ source('src_ord_mgmt_silver','mao_ord_invoice_line_v') }} ivl
        join {{ source('src_ord_mgmt_silver','mao_ord_invoice_v') }} ivh
            on ivl.org_id = ivh.org_id and ivl.invoice_pk = ivh.pk
        qualify row_number() over (
            partition by ivh.org_id, ivh.package_id, ivl.package_detail_id
            order by ivl.updated_timestamp desc, ivh.updated_timestamp desc
        ) = 1
    ),
    ord_e_inv_stg as (
        select
            ivh.org_id,
            ivh.pk as ivh_pk,
            ivl.pk as ivl_pk,
            ivh.package_id as pkg_id,
            ivl.package_detail_id as pkg_dtl_id,
            ivl.item_id,
            ivh.invoice_id as inv_id,
            ivl.invoice_line_id as inv_ln_id,
            ivh.transaction_reference_id,
            ivh.invoice_type,
            ivh.created_by,
            ivh.version,
            ivh.invoice_total,
            ivh.invoice_sub_total,
            ivh.total_charges,
            ivh.total_discounts,
            ivh.fulfillment_date
        from {{ source('src_ord_mgmt_silver','mao_ord_e_invoice_line_v') }} ivl
        join {{ source('src_ord_mgmt_silver','mao_ord_e_invoice_v') }} ivh
            on ivl.org_id = ivh.org_id and ivl.e_invoice_pk = ivh.pk
        qualify row_number() over (
            partition by ivh.org_id, ivh.package_id, ivl.package_detail_id
            order by ivl.updated_timestamp desc, ivh.updated_timestamp desc
        ) = 1
    ),
    loc_stg as (
        select loc_id, loc_type_id
        from {{ ref('dim_mao_loc_t') }}
        where reporting_flg = 'Y'
    ),
    payment_stg as (
        select
            ph.org_id,
            ph.order_id,
            array_agg(distinct pm.payment_type)::string as payment_type,
            max(pm.currency_code) as ccy_code
        from {{ source('src_payment_silver','mao_pay_payment_header_v') }} ph
        join {{ source('src_payment_silver','mao_pay_payment_method_v') }} pm
            on pm.org_id = ph.org_id and pm.payment_header_pk = ph.pk
        group by all
    ),
    ful_task_detail as (
        select
            ts.org_id,
            ts.task_id,
            ts.task_type_id,
            ts.task_status_id,
            ts.assigned_to,
            ts.updated_by,
            ts.task_status_timestamp,
            tt.task_type_name,
            tst.task_status_name,
            pt.picked_qty,
            pt.quantity
        from {{ source('src_ord_fulflmnt_silver','mao_ful_task_v') }} ts
        left join {{ source('src_ord_fulflmnt_silver','mao_ful_task_type_v') }} tt
            on ts.task_type_id = tt.task_type_id
        left join {{ source('src_ord_fulflmnt_silver','mao_ful_task_status_v') }} tst
            on ts.task_status_id = tst.task_status_id
        left join {{ source('src_ord_fulflmnt_silver','mao_ful_pick_task_detail_v') }} pt
            on pt.org_id = ts.org_id and pt.task_id = ts.task_id
    ),
    pkg_t as (
        select
            pd.packages_pk as pkg_pk,
            pd.pk as pkg_dtl_pk,
            ph.package_id as pkg_id,
            ph.package_type_id as pkg_type_id,
            pd.package_detail_id as pkg_dtl_id,
            try_to_decimal(nullif(ph.package_status, ''), 38, 0) as pkg_status,
            ps.package_status_name as pkg_status_desc,
            pd.org_id as org_id,
            ph.order_id as ord_id,
            pd.fulfillment_id as fulflmnt_id,
            pd.fulfillment_line_id as fulflmnt_ln_id,
            ph.pickup_fulfillment_id as pickup_fulflmnt_id,
            pd.item_id as item_id,
            ph.shipment_id as shipment_id,
            ph.shipment_type as shipment_type,
            ph.ship_from_location_id as ship_from_loc_id,
            ltf.loc_type_id as ship_from_loc_type,
            pd.short_reason_id as short_reason_id,
            ph.current_location_id as crnt_location_id,
            ph.tracking_number as tracking_num,
            ph.return_tracking_number as rtn_tracking_num,
            coalesce(oi.inv_id, ei.inv_id) as inv_id,
            coalesce(oi.inv_ln_id, ei.inv_ln_id) as inv_ln_id,
            coalesce(oi.transaction_reference_id, ei.transaction_reference_id) as inv_txn_ref_id,
            coalesce(oi.invoice_type, ei.invoice_type) as inv_type,
            coalesce(oi.created_by, ei.created_by) as inv_created_by,
            coalesce(oi.version, ei.version) as inv_version,
            ph.task_id as task_id,
            ft.task_type_id as task_type_id,
            ft.task_type_name as task_type_name,
            ft.task_status_id as task_status_id,
            ft.task_status_name as task_status_name,
            ph.ship_to_address_address1 as ship_to_addr_addr1,
            ph.ship_to_address_address2 as ship_to_addr_addr2,
            ph.ship_to_address_address3 as ship_to_addr_addr3,
            ph.ship_to_address_city as ship_to_addr_city,
            ph.ship_to_address_country as ship_to_addr_country,
            ph.ship_to_address_county as ship_to_addr_county,
            ph.ship_to_address_email as ship_to_addr_email,
            ph.ship_to_address_firstname as ship_to_addr_firstname,
            ph.ship_to_address_lastname as ship_to_addr_lastname,
            ph.ship_to_address_phone as ship_to_addr_phone,
            ph.ship_to_address_postalcode as ship_to_addr_postal_cd,
            ph.ship_to_address_state as ship_to_addr_state,
            ph.delivery_type as dlvry_type,
            ph.delivery_status as dlvry_status,
            ph.carrier_barcode as carrier_barcode,
            ph.service_level_code as service_level_cd,
            pd.is_hazardous as is_hazardous,
            pd.volume_qty as volume_qty,
            pd.item_description as item_desc,
            pd.weight_uom as weight_uom,
            pd.serial_number as serial_num,
            pd.short_quantity_uom as short_qty_uom,
            pd.gift_card_number as gift_card_num,
            pd.received_quantity_uom as rcvd_qty_uom,
            pd.country_of_origin as country_of_origin,
            ft.assigned_to as task_assigned_to,
            ft.updated_by as task_updtd_by,
            ft.picked_qty as task_picked_qty,
            ft.quantity as task_qty,
            ph.gross_weight_qty as gross_weight_qty,
            ph.gross_weight_uom as gross_weight_uom,
            ph.gross_volume_qty as gross_volume_qty,
            ph.gross_volume_uom as gross_volume_uom,
            pd.quantity_uom as qty_uom,
            pd.volume_uom as volume_uom,
            pd.weight_qty as weight_qty,
            pd.gift_card_pin as gift_card_pin,
            pm.ccy_code as pymt_ccy_code,
            pd.quantity_qty as qty,
            pd.received_quantity_qty as rcvd_qty,
            pd.gift_card_value as gift_card_value,
            pd.short_quantity_qty as short_qty,
            coalesce(oi.invoice_total, ei.invoice_total) as inv_total,
            coalesce(oi.invoice_sub_total, ei.invoice_sub_total) as inv_sub_total,
            coalesce(oi.total_charges, ei.total_charges) as inv_total_chrgs,
            coalesce(oi.total_discounts, ei.total_discounts) as inv_total_discs,
            oi.failed_amount as invoice_failed_amt,
            ft.task_status_timestamp::timestamp_ntz as task_status_ts,
            coalesce(oi.fulfillment_date, ei.fulfillment_date)::timestamp_ntz as inv_fulflmnt_dt,
            coalesce(oi.created_timestamp, ei.fulfillment_date)::timestamp_ntz as inv_created_ts,
            ph.estimated_delivery_date::timestamp_ntz(6) as estimated_dlvry_dt,
            ph.packed_date_time::timestamp_ntz(6) as packed_dttm,
            ph.shipped_date_time::timestamp_ntz(6) as shipped_dttm,
            ph.received_date_time::timestamp_ntz(6) as rcvd_dttm,
            ph.created_by as created_by,
            ph.created_timestamp::timestamp_ntz as created_ts,
            ph.updated_by as updated_by,
            ph.updated_timestamp::timestamp_ntz as updated_ts,
            pd.src_load_ts
        from
            mao_ful_package_detail pd
            join mao_ful_packages ph on ph.org_id = pd.org_id and ph.pk = pd.packages_pk
            left join (
                select package_status_id, package_status_name
                from {{ source('src_ord_fulflmnt_silver','mao_ful_package_status_v') }}
            ) ps on case when ph.package_status != '' then ph.package_status end = ps.package_status_id
            left join ord_inv_stg oi on pd.org_id = oi.org_id and ph.package_id = oi.pkg_id and pd.package_detail_id = oi.pkg_dtl_id
            left join ord_e_inv_stg ei on pd.org_id = ei.org_id and ph.package_id = ei.pkg_id and pd.package_detail_id = ei.pkg_dtl_id
            left join ful_task_detail ft on ph.org_id = ft.org_id and ph.task_id = ft.task_id
            left join loc_stg ltf on ph.ship_from_location_id = ltf.loc_id
            left join payment_stg pm on ph.org_id = pm.org_id and ph.order_id = pm.order_id
    ),
    pkg_main as (
        select
            src.*,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
        from
            pkg_t src
    )
select
    src.*,
    current_timestamp()::timestamp_ntz as start_ts,
    null::timestamp as end_ts,
    'Y'::varchar(1) as active_flg,
    'Y'::varchar(1) as reporting_flg,
    'N'::varchar(1) as status_hist_flg,
    {{ v_batch_id }}::decimal(38, 0) as batch_id,
    current_timestamp()::timestamp as etl_load_ts,
    current_timestamp()::timestamp as etl_updt_ts
from pkg_main src