{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "org_id", "ord_id", "pymt_method_id", "pymt_txn_id" ]  %}

{% set target_context_typ2_column_list = 
    [ "pymt_txn_detail_id","prnt_ord_id","prnt_pymt_grp_id","prnt_pymt_method_id","pymt_grp_id","pymt_status_id","pymt_status_desc","entry_type_id","transaction_ref_id", "inv_id",
	"is_copied","is_modifiable","is_suspended","is_voided","account_type","addr_firstname","addr_lastname","addr_addr1","addr_addr2","addr_addr3","addr_city","addr_state",
	"addr_postal_cd","addr_county","addr_country","addr_phone","addr_email","alt_ccy_code","alt_ccy_amt","bus_name","captured_src","charge_seq",
	"check_qty","ccy_code","pymt_catg","pymt_type", "txn_card_last4", "card_alias", "card_token", "pymt_card_type","auth_status", "auth_cd", "auth_txn_dt","auth_txn_id","auth_original_order_id",
	"pymt_txn_auth_code","pymt_txn_req_id","pymt_txn_dt","pymt_txn_status_desc","attrib_card_last4","pymt_txn_type","attrib_card_type_display",
	"attrib_card_expiry_dt","attrib_cvv_response","attrib_avs_code","attrib_str_merch_id","attrib_str_inv_id","attrib_str_ord_req_id","attrib_str_terminal_id",
	"seller_protection_status", "conversion_rt","crnt_auth_amt","crnt_failed_amt","crnt_refund_amt","crnt_settled_amt",
	"chrgbk_amt","book_amt", "pymt_txn_proc_amt", "pymt_txn_req_amt", "req_refund_amt", "pymt_method_amt","pymt_txn_amt","pymt_txn_group_amt","merchandise_amt",
	"orig_amt","chg_amt", "pymt_gateway_id", "pymt_provider", "pymt_txn_req_dt", "created_ts", "updated_ts" ]  %}


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
    pymt_transaction as (
        select
            pth.org_id,
			pth.pk as pymt_txn_pk,
            pth.payment_method_pk as pymt_method_pk,
            pth.payment_transaction_id as pymt_txn_id,
			max_by(ptd.payment_transaction_detail_id,ptd.updated_timestamp) as pymt_txn_detail_id,
            pth.status as auth_status,
            pth.transaction_date as auth_txn_dt,
            pth.unique_transaction_id as auth_txn_id,
            pth.order_id as auth_original_order_id,
            null as pymt_txn_auth_code,
            pth.request_id as pymt_txn_req_id,
            pth.requested_date as pymt_txn_req_dt,
            pth.transaction_date as pymt_txn_dt,
            pth.payment_response_status as pymt_txn_status_desc,
            pth.transaction_type as pymt_txn_type,
            sum(coalesce(ptd.amount,0)) as pymt_txn_amount,
            ptg.amount as pymt_txn_group_amount,
            pth.processed_amount as pymt_txn_proc_amt,
            pth.requested_amount as pymt_txn_req_amt,
			pth.created_timestamp as created_ts,
			pth.updated_timestamp 
            as updated_ts,
			pth.src_load_ts
        from {{ source('src_payment_silver','mao_pay_payment_transaction_v') }} pth
            left join (
                select org_id, payment_transaction_pk, payment_transaction_detail_id, amount, updated_timestamp
                from {{ source('src_payment_silver','mao_pay_payment_transaction_detail_v') }}
            ) ptd on pth.org_id = ptd.org_id and pth.pk = ptd.payment_transaction_pk
            left join (
                select org_id, payment_transaction_pk, amount
                from {{ source('src_payment_silver','mao_pay_payment_transaction_group_v') }}
            ) ptg on pth.org_id = ptg.org_id and pth.pk = ptg.payment_transaction_pk
		{% if is_incremental() %}
            where
                pth.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
		group by all
    ),
    pymt_details as (
        select
            pt.pymt_txn_pk as ord_pymt_ln_pk
            ,ph.org_id as org_id
            ,ph.order_id as ord_id
            ,pm.payment_method_id as pymt_method_id
            ,pt.pymt_txn_id
            ,pt.pymt_txn_detail_id
            ,pm.parent_order_id as prnt_ord_id
            ,pm.parent_payment_group_id as prnt_pymt_grp_id
            ,pm.parent_payment_method_id as prnt_pymt_method_id
            ,ph.payment_group_id as pymt_grp_id
            ,ph.status::decimal(38, 0) as pymt_status_id
            ,pstat.status_name as pymt_status_desc
            ,pm.entry_type_id as entry_type_id
            ,pm.transaction_reference_id as transaction_ref_id
            ,ps.invoice_id as inv_id
            ,pm.is_copied as is_copied
            ,pm.is_modifiable as is_modifiable
            ,pm.is_suspended as is_suspended
            ,pm.is_voided as is_voided
            ,pm.account_type as account_type
            ,ba.address_firstname as addr_firstname
            ,ba.address_lastname as addr_lastname
            ,ba.address_address1 as addr_addr1
            ,ba.address_address2 as addr_addr2
            ,ba.address_address3 as addr_addr3
            ,ba.address_city as addr_city
            ,ba.address_state as addr_state
            ,ba.address_postalcode as addr_postal_cd
            ,ba.address_county as addr_county
            ,ba.address_country as addr_country
            ,ba.address_phone as addr_phone
            ,ba.address_email as addr_email
            ,pm.alternate_currency_code as alt_ccy_code
            ,pm.alternate_currency_amount as alt_ccy_amt
            ,pm.business_name as bus_name
            ,pm.captured_source as captured_src
            ,pm.seq as charge_seq
            ,pm.check_quantity as check_qty
            ,pm.currency_code as ccy_code
            ,pm.payment_category as pymt_catg
            ,pm.payment_type as pymt_type
            ,pm.gateway_id as pymt_gateway_id
            ,pm.ext_paymentprovider as pymt_provider
            ,pm.account_display_number as txn_card_last4
            ,pm.ext_card_alias as card_alias
            ,pm.account_number as card_token
            ,pm.card_type as pymt_card_type
            ,pt.auth_status
            ,parse_json(pm.json_store):"Fields":"extend::authcode"::string as auth_cd
            ,pt.auth_txn_dt::timestamp_ntz as auth_txn_dt
            ,pt.auth_txn_id
            ,pt.auth_original_order_id
            ,pt.pymt_txn_auth_code
            ,pt.pymt_txn_req_id
            ,pt.pymt_txn_dt::timestamp_ntz as pymt_txn_dt
            ,pt.pymt_txn_status_desc
            ,pm.account_display_number as attrib_card_last4
            ,pt.pymt_txn_type
            ,pm.payment_type as attrib_card_type_display
            ,pm.card_expiry_year as attrib_card_expiry_dt
            ,pa.attrib_cvv_response
            ,pa.attrib_avs_code
            ,oh.ext_storemerchantid as attrib_str_merch_id
            ,oh.ext_storeinvoiceid as attrib_str_inv_id
            ,oh.ext_storeorderrequestid as attrib_str_ord_req_id
            ,oh.ext_terminalid as attrib_str_terminal_id
            ,pm.ext_sellerprotectionstatus as seller_protection_status
            ,pm.conversion_rate as conversion_rt
            ,pm.current_auth_amount as crnt_auth_amt
            ,pm.current_failed_amount as crnt_failed_amt
            ,pm.current_refund_amount as crnt_refund_amt
            ,pm.current_settled_amount as crnt_settled_amt
            ,ps.chargeback_amount as chrgbk_amt
            ,ps.book_amount as book_amt
            ,pt.pymt_txn_proc_amt as pymt_txn_proc_amt
            ,pt.pymt_txn_req_amt as pymt_txn_req_amt
            ,ps.requested_refund_amount as req_refund_amt
            ,pm.amount as pymt_method_amt
            ,pt.pymt_txn_amount as pymt_txn_amt
            ,pt.pymt_txn_group_amount as pymt_txn_group_amt
            ,pm.merchandise_amount as merchandise_amt
            ,pm.original_amount as orig_amt
            ,pm.change_amount as chg_amt
            ,pt.pymt_txn_req_dt::timestamp_ntz as pymt_txn_req_dt
			,pt.created_ts
			,pt.updated_ts
            ,pt.src_load_ts
        from mao_pay_payment_header ph
            join (
                select org_id, order_id, ext_storemerchantid, ext_storeinvoiceid, ext_storeorderrequestid, ext_terminalid
                from {{ source('src_ord_mgmt_silver','mao_ord_order_v') }}
            ) oh on oh.org_id = ph.org_id and oh.order_id = ph.order_id
            join {{ source('src_payment_silver','mao_pay_payment_method_v') }} pm on pm.org_id = ph.org_id and pm.payment_header_pk = ph.pk
            left join (
                select org_id, payment_method_pk, address_firstname, address_lastname, address_address1, address_address2, address_address3,
                    address_city, address_state, address_postalcode, address_county, address_country, address_phone, address_email
                from {{ source('src_payment_silver','mao_pay_billing_address_v') }}
            ) ba on ba.org_id = pm.org_id and ba.payment_method_pk = pm.pk
            left join (
                select org_id, payment_group_id, order_id, invoice_id, chargeback_amount, book_amount, requested_refund_amount
                from {{ source('src_payment_silver','mao_pay_payment_summary_v') }}
            ) ps on ps.org_id = pm.org_id and ps.payment_group_id = ph.payment_group_id and ps.order_id = ph.order_id
            join pymt_transaction pt on pt.org_id = pm.org_id and pt.pymt_method_pk = pm.pk
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
