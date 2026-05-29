{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "org_id", "ord_id", "pymt_method_id", "pymt_txn_id", "pymt_txn_dtl_id", "inv_id", "inv_ln_id" ]  %}

{% set target_context_typ2_column_list = 
    [ "ord_ln_id","item_id","pkg_id","pkg_dtl_id","prnt_ord_id","prnt_pymt_grp_id","prnt_pymt_method_id","pymt_grp_id","pymt_status_id","pymt_status_desc","entry_type_id","transaction_ref_id",
	"is_copied","is_modifiable","is_suspended","is_voided","account_type","addr_firstname","addr_lastname","addr_addr1","addr_addr2","addr_addr3","addr_city","addr_state",
	"addr_postal_cd","addr_county","addr_country","addr_phone","addr_email","alt_ccy_code","alt_ccy_amt","bus_name","captured_src","charge_seq",
	"check_qty","ccy_code","pymt_catg","pymt_type", "txn_card_last4", "card_alias", "card_token", "pymt_card_type","auth_status", "auth_cd", "auth_txn_dt","auth_txn_id",
	"pymt_txn_auth_code","pymt_txn_req_id","pymt_txn_dt","pymt_txn_status_desc","attrib_card_last4","pymt_txn_type","attrib_card_type_display",
	"attrib_card_expiry_dt","attrib_cvv_response","attrib_avs_code","attrib_str_merch_id","attrib_str_inv_id","attrib_str_ord_req_id","attrib_str_terminal_id",
	"seller_protection_status", "conversion_rt","crnt_auth_amt","crnt_failed_amt","crnt_refund_amt","crnt_settled_amt",
	"chrgbk_amt","book_amt", "pymt_txn_proc_amt", "pymt_txn_req_amt", "req_refund_amt", "pymt_method_amt","pymt_txn_amt","pymt_txn_grp_amt","merchandise_amt",
	"orig_amt","chg_amt", "pymt_gateway_id", "pymt_provider", "pymt_txn_req_dt", "created_ts", "updated_ts",
    "ordered_item_id","inv_ln_qty","inv_ln_unit_price","inv_ln_sub_total","inv_ln_total",
    "inv_ln_total_taxes","inv_ln_total_discs","inv_line_total_charges","fulflmnt_ts","inv_pk","inv_status","inv_type",
    "inv_prnt_ord_id","cust_id","inv_sub_total","inv_total","inv_total_discs","inv_total_charges","inv_total_taxes",
    "inv_created_by","inv_created_ts","inv_updated_by","inv_updated_ts","inv_ln_pk","physical_origin_id",
    "ship_from_loc_id","ship_from_addr_id","ship_to_loc_id","ship_to_addr_id","is_refund_gift_card","uom","ordered_uom",
    "inv_ln_ord_qty","gift_card_value","inv_ln_created_by","inv_ln_created_ts","inv_ln_updated_by","inv_ln_updated_ts" ]  %}


{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook = ["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
					"{{  fl_utils.m_apply_typ1_deletion_on_tgt_model( source('src_payment_silver','mao_pay_payment_header_v'), this, 'ord_pymt_ln_pk')   }}",
					"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"],
    meta={'format': "iceberg", 'strategy': "merge", 'update_condition': "active_flg"}
) }}

with
    mao_pay_payment_header as (
        select
            ph.pk,
            ph.org_id,
            ph.order_id,
            ph.payment_group_id,
            ph.status,
            ph.src_load_ts
        from {{ source('src_payment_silver','mao_pay_payment_header_v') }} ph
    ),
        pymt_transaction_base as (
        select
            pth.org_id,
            pth.pk as pymt_txn_pk,
            pth.payment_method_pk as pymt_method_pk,
            pth.payment_transaction_id as pymt_txn_id,
            ptd.payment_transaction_detail_id as pymt_txn_dtl_id,
            pth.status as auth_status,
            pth.transaction_date as auth_txn_dt,
            pth.unique_transaction_id as auth_txn_id,
            pth.order_id as ord_id,
            null as pymt_txn_auth_code,
            pth.request_id as pymt_txn_req_id,
            pth.follow_on_id as pymt_txn_follow_on_id,
            pth.requested_date as pymt_txn_req_dt,
            pth.transaction_date as pymt_txn_dt,
            pth.payment_response_status as pymt_txn_status_desc,
            pth.transaction_type as pymt_txn_type,
            coalesce(ptd.amount, 0) as pymt_txn_amt,
            ptg.amount as pymt_txn_grp_amt,
            pth.processed_amount as pymt_txn_proc_amt,
            pth.requested_amount as pymt_txn_req_amt,
            pth.created_timestamp as created_ts,
            pth.updated_timestamp as updated_ts,
            inv.pk as inv_pk,
            inv.invoice_id as inv_id,
            inv.status as inv_status,
            inv.invoice_type as inv_type,
            inv.parent_order_id as prnt_ord_id,
            inv.customer_id as cust_id,
            inv.package_id as pkg_id,
            inv.invoice_sub_total as inv_sub_total,
            inv.invoice_total as inv_total,
            inv.total_discounts as inv_total_discs,
            inv.total_charges as inv_total_charges,
            inv.total_taxes as inv_total_taxes,
            inv.created_by as inv_created_by,
            inv.created_timestamp as inv_created_ts,
            inv.updated_by as inv_updated_by,
            inv.updated_timestamp as inv_updated_ts,
            invl.pk as inv_ln_pk,
            invl.invoice_line_id as inv_ln_id,
            invl.order_line_id as ord_ln_id,
            invl.package_detail_id as pkg_dtl_id,
            invl.physical_origin_id,
            invl.ship_from_location_id as ship_from_loc_id,
            invl.ship_from_address_id as ship_from_addr_id,
            invl.ship_to_location_id as ship_to_loc_id,
            invl.ship_to_address_id as ship_to_addr_id,
            invl.item_id,
            invl.ordered_item_id,
            invl.is_refund_gift_card,
            invl.uom,
            invl.ordered_uom,
            invl.quantity as inv_ln_qty,
            invl.ordered_quantity as inv_ln_ord_qty,
            invl.unit_price as inv_ln_unit_price,
            invl.gift_card_value,
            invl.invoice_line_sub_total as inv_ln_sub_total,
            invl.invoice_line_total as inv_ln_total,
            invl.total_discounts as inv_ln_total_discs,
            invl.total_charges as inv_line_total_charges,
            invl.total_taxes as inv_ln_total_taxes,
            invl.created_by as inv_ln_created_by,
            invl.created_timestamp as inv_ln_created_ts,
            invl.updated_by as inv_ln_updated_by,
            invl.updated_timestamp as inv_ln_updated_ts,
            invl.fulfillment_date as fulflmnt_ts,
            pth.src_load_ts
        from {{ source('src_payment_silver','mao_pay_payment_transaction_v') }} pth
            left join (
                select org_id, payment_transaction_pk, payment_transaction_detail_id, amount, updated_timestamp, reference_id
                from {{ source('src_payment_silver','mao_pay_payment_transaction_detail_v') }}
            ) ptd on pth.org_id = ptd.org_id and pth.pk = ptd.payment_transaction_pk
            left join (
                select org_id, payment_transaction_pk, amount
                from {{ source('src_payment_silver','mao_pay_payment_transaction_group_v') }}
            ) ptg on pth.org_id = ptg.org_id and pth.pk = ptg.payment_transaction_pk
            left join {{ source('src_ord_mgmt_silver','mao_ord_invoice_v') }} inv
                on ptd.org_id = inv.org_id and ptd.reference_id = inv.invoice_id
            left join {{ source('src_ord_mgmt_silver','mao_ord_invoice_line_v') }} invl
                on inv.org_id = invl.org_id and inv.pk = invl.invoice_pk
        {% if is_incremental() %}
            where
                pth.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
    ),
	pymt_order_ids as (
	select org_id,ord_id from pymt_transaction_base group by all
	),
    follow_on_lookup as (
        select 
            pth.org_id,
			pth.order_id as ord_id,
            pth.request_id as pymt_txn_req_id,
            inv.invoice_id as inv_id,
            invl.invoice_line_id as inv_ln_id,
            invl.order_line_id as ord_ln_id,
            invl.item_id,
            pth.follow_on_id as pymt_txn_follow_on_id
        from {{ source('src_payment_silver','mao_pay_payment_transaction_v') }} pth
            left join (
                select org_id, payment_transaction_pk, reference_id
                from {{ source('src_payment_silver','mao_pay_payment_transaction_detail_v') }}
            ) ptd on pth.org_id = ptd.org_id and pth.pk = ptd.payment_transaction_pk
            left join {{ source('src_ord_mgmt_silver','mao_ord_invoice_v') }} inv
                on ptd.org_id = inv.org_id and ptd.reference_id = inv.invoice_id
            left join {{ source('src_ord_mgmt_silver','mao_ord_invoice_line_v') }} invl
                on inv.org_id = invl.org_id and inv.pk = invl.invoice_pk
			join pymt_order_ids ord on (pth.org_id=ord.org_id and pth.order_id=ord.ord_id)
        where
            inv.invoice_id is not null
    ),
    pymt_transaction as (
        select
            base.org_id,
            base.pymt_txn_pk,
            base.pymt_method_pk,
            base.pymt_txn_id,
            base.pymt_txn_dtl_id,
            base.auth_status,
            base.auth_txn_dt,
            base.auth_txn_id,
            base.ord_id,
            base.pymt_txn_auth_code,
            base.pymt_txn_req_id,
            base.pymt_txn_follow_on_id,
            base.pymt_txn_req_dt,
            base.pymt_txn_dt,
            base.pymt_txn_status_desc,
            base.pymt_txn_type,
            base.pymt_txn_amt,
            base.pymt_txn_grp_amt,
            base.pymt_txn_proc_amt,
            base.pymt_txn_req_amt,
            base.created_ts,
            base.updated_ts,
            base.inv_pk,
            coalesce(base.inv_id, fol.inv_id) as inv_id,
            base.inv_status,
            base.inv_type,
            base.prnt_ord_id,
            base.cust_id,
            base.pkg_id,
            base.inv_sub_total,
            base.inv_total,
            base.inv_total_discs,
            base.inv_total_charges,
            base.inv_total_taxes,
            base.inv_created_by,
            base.inv_created_ts,
            base.inv_updated_by,
            base.inv_updated_ts,
            base.inv_ln_pk,
            coalesce(base.inv_ln_id, fol.inv_ln_id) as inv_ln_id,
            coalesce(base.ord_ln_id, fol.ord_ln_id) as ord_ln_id,
            base.pkg_dtl_id,
            base.physical_origin_id,
            base.ship_from_loc_id,
            base.ship_from_addr_id,
            base.ship_to_loc_id,
            base.ship_to_addr_id,
            coalesce(base.item_id, fol.item_id) as item_id,
            base.ordered_item_id,
            base.is_refund_gift_card,
            base.uom,
            base.ordered_uom,
            base.inv_ln_qty,
            base.inv_ln_ord_qty,
            base.inv_ln_unit_price,
            base.gift_card_value,
            base.inv_ln_sub_total,
            base.inv_ln_total,
            base.inv_ln_total_discs,
            base.inv_line_total_charges,
            base.inv_ln_total_taxes,
            base.inv_ln_created_by,
            base.inv_ln_created_ts,
            base.inv_ln_updated_by,
            base.inv_ln_updated_ts,
            base.fulflmnt_ts,
            base.src_load_ts
        from pymt_transaction_base base
            left join follow_on_lookup fol
                on (base.org_id=fol.org_id and base.ord_id=fol.ord_id and base.pymt_txn_follow_on_id = fol.pymt_txn_req_id and base.inv_id is null and lower(base.pymt_txn_type) not in ('authorization', 'authorization reversal','settlement'))
        QUALIFY ROW_NUMBER() OVER (PARTITION BY base.org_id, base.ord_id, base.ord_ln_id,base.pymt_method_pk,base.pymt_txn_id,base.pymt_txn_dtl_id,base.inv_id,base.inv_ln_id ORDER BY updated_ts DESC) = 1
    ),
    pymt_details as (
        select
            pt.pymt_txn_pk as ord_pymt_ln_pk,
            ph.org_id as org_id,
            pm.payment_method_id as pymt_method_id,
            pt.pymt_txn_id,
            pt.pymt_txn_dtl_id,
            pt.inv_id,
            pt.inv_ln_id,
            ph.order_id as ord_id,
            pt.ord_ln_id,
            pt.item_id,
            pt.pkg_id,
            pt.pkg_dtl_id,
            pm.parent_order_id as prnt_ord_id,
            pm.parent_payment_group_id as prnt_pymt_grp_id,
            pm.parent_payment_method_id as prnt_pymt_method_id,
            ph.payment_group_id as pymt_grp_id,
            ph.status::decimal(38, 0) as pymt_status_id,
            pstat.status_name as pymt_status_desc,
            pm.entry_type_id as entry_type_id,
            pm.transaction_reference_id as transaction_ref_id,
            pm.is_copied as is_copied,
            pm.is_modifiable as is_modifiable,
            pm.is_suspended as is_suspended,
            pm.is_voided as is_voided,
            pm.account_type as account_type,
            ba.address_firstname as addr_firstname,
            ba.address_lastname as addr_lastname,
            ba.address_address1 as addr_addr1,
            ba.address_address2 as addr_addr2,
            ba.address_address3 as addr_addr3,
            ba.address_city as addr_city,
            ba.address_state as addr_state,
            ba.address_postalcode as addr_postal_cd,
            ba.address_county as addr_county,
            ba.address_country as addr_country,
            ba.address_phone as addr_phone,
            ba.address_email as addr_email,
            pm.alternate_currency_code as alt_ccy_code,
            pm.alternate_currency_amount as alt_ccy_amt,
            pm.business_name as bus_name,
            pm.captured_source as captured_src,
            pm.seq as charge_seq,
            pm.check_quantity as check_qty,
            pm.currency_code as ccy_code,
            pm.payment_category as pymt_catg,
            pm.payment_type as pymt_type,
            pm.gateway_id as pymt_gateway_id,
            pm.ext_paymentprovider as pymt_provider,
            pm.account_display_number as txn_card_last4,
            pm.ext_card_alias as card_alias,
            pm.account_number as card_token,
            pm.card_type as pymt_card_type,
            pt.auth_status,
            parse_json(pm.json_store):"Fields":"extend::authcode"::string as auth_cd,
            pt.auth_txn_dt::timestamp_ntz as auth_txn_dt,
            pt.auth_txn_id,
            pt.pymt_txn_auth_code,
			pt.pymt_txn_req_id,
            pt.pymt_txn_dt::timestamp_ntz as pymt_txn_dt,
            pt.pymt_txn_status_desc,
            pm.account_display_number as attrib_card_last4,
            pt.pymt_txn_type,
            pm.payment_type as attrib_card_type_display,
            pm.card_expiry_year as attrib_card_expiry_dt,
            pa.attrib_cvv_response,
            pa.attrib_avs_code,
            oh.ext_storemerchantid as attrib_str_merch_id,
            oh.ext_storeinvoiceid as attrib_str_inv_id,
            oh.ext_storeorderrequestid as attrib_str_ord_req_id,
            oh.ext_terminalid as attrib_str_terminal_id,
            pm.ext_sellerprotectionstatus as seller_protection_status,
            pm.conversion_rate as conversion_rt,
            pm.current_auth_amount as crnt_auth_amt,
            pm.current_failed_amount as crnt_failed_amt,
            pm.current_refund_amount as crnt_refund_amt,
            pm.current_settled_amount as crnt_settled_amt,
            ps.chargeback_amount as chrgbk_amt,
            ps.book_amount as book_amt,
            pt.pymt_txn_proc_amt,
            pt.pymt_txn_req_amt,
            ps.requested_refund_amount as req_refund_amt,
            pm.amount as pymt_method_amt,
            pt.pymt_txn_amt,
            pt.pymt_txn_grp_amt,
            pm.merchandise_amount as merchandise_amt,
            pm.original_amount as orig_amt,
            pm.change_amount as chg_amt,
            pt.pymt_txn_req_dt::timestamp_ntz as pymt_txn_req_dt,
            pt.created_ts,
            pt.updated_ts,
            pt.ordered_item_id,
            pt.inv_ln_qty,
            pt.inv_ln_unit_price,
            pt.inv_ln_sub_total,
            pt.inv_ln_total,
            pt.inv_ln_total_taxes,
            pt.inv_ln_total_discs,
            pt.inv_line_total_charges,
            pt.fulflmnt_ts,
            pt.inv_pk,
            pt.inv_status,
            pt.inv_type,
            pt.prnt_ord_id as inv_prnt_ord_id,
            pt.cust_id,
            pt.inv_sub_total,
            pt.inv_total,
            pt.inv_total_discs,
            pt.inv_total_charges,
            pt.inv_total_taxes,
            pt.inv_created_by,
            pt.inv_created_ts,
            pt.inv_updated_by,
            pt.inv_updated_ts,
            pt.inv_ln_pk,
            pt.physical_origin_id,
            pt.ship_from_loc_id,
            pt.ship_from_addr_id,
            pt.ship_to_loc_id,
            pt.ship_to_addr_id,
            pt.is_refund_gift_card,
            pt.uom,
            pt.ordered_uom,
            pt.inv_ln_ord_qty,
            pt.gift_card_value,
            pt.inv_ln_created_by,
            pt.inv_ln_created_ts,
            pt.inv_ln_updated_by,
            pt.inv_ln_updated_ts,
            pt.src_load_ts
        from mao_pay_payment_header ph
            join (
                select org_id, order_id, ext_storemerchantid, ext_storeinvoiceid, ext_storeorderrequestid, ext_terminalid
                from {{ source('src_ord_mgmt_silver','mao_ord_order_v') }}
            ) oh on oh.org_id = ph.org_id and oh.order_id = ph.order_id
            join {{ source('src_payment_silver','mao_pay_payment_method_v') }} pm
                on pm.org_id = ph.org_id and pm.payment_header_pk = ph.pk
            left join (
                select org_id, payment_method_pk, address_firstname, address_lastname, address_address1, address_address2, address_address3,
                    address_city, address_state, address_postalcode, address_county, address_country, address_phone, address_email
                from {{ source('src_payment_silver','mao_pay_billing_address_v') }}
            ) ba on ba.org_id = pm.org_id and ba.payment_method_pk = pm.pk
            left join (
                select org_id, payment_group_id, order_id, invoice_id, chargeback_amount, book_amount, requested_refund_amount
                from {{ source('src_payment_silver','mao_pay_payment_summary_v') }}
            ) ps on ps.org_id = pm.org_id and ps.payment_group_id = ph.payment_group_id and ps.order_id = ph.order_id
            join pymt_transaction pt
                on pt.org_id = pm.org_id and pt.pymt_method_pk = pm.pk
            left join (
                select
                    org_id,
                    payment_method_pk,
                    max(case when lower(name) = 'cvvresultcode' then value end) as attrib_cvv_response,
                    max(case when lower(name) = 'avsresultcode' then value end) as attrib_avs_code
                from {{ source('src_payment_silver','mao_pay_payment_method_attribute_v') }}
                where lower(name) in ('cvvresultcode', 'avsresultcode')
                group by all
            ) pa on pa.org_id = pm.org_id and pa.payment_method_pk = pm.pk
            left join (
                select status_id, status_name
                from {{ source('src_payment_silver','mao_pay_payment_status_v') }}
            ) pstat on pstat.status_id = ph.status
    ),
    pymt_details_hash as (
        select
            src.*,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
        from
            pymt_details as src
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
    pymt_details_hash as src