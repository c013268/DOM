{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "org_id", "ord_id" ]  %}

{% set target_context_typ2_column_list = 
    [ "selling_loc_id","cust_id","pymt_status_id","pymt_status_desc","hold_status_id","resolve_reason_id","min_fulflmnt_status_id","min_fulflmnt_status_desc",
	"max_fulflmnt_status_id","max_fulflmnt_status_desc","cnl_reason_id","cnl_reason_sdesc","cnl_reason_desc","cnl_reason_type_id","ccy_cd","pymt_type",
	"pymt_ccy_code","cust_type_id","doc_type_id","ord_locale","tracking_num","rtn_tracking_num","rtn_status_id","rtn_status_desc","ord_type_id","inv_id",
	"prnt_resrv_req_id","is_cnlled","is_confirmed","is_on_hold","is_post_voided","is_ready_for_tender","is_tax_exempt","is_tax_overridden","is_launch_sku_flg",
	"is_prepaid","is_fraud_service_failed","vendor_id","ord_updtd_by","ord_created_by","ord_type_sdesc","ord_type_desc","hold_type","loyalty_num","cust_email",
	"cust_phone", "refund_pymt_method","Cart_Company_Num","browser_ip","flx_id","xstore_ord_id","shopping_cart_id","associate_num","usr_agnt","channel",
	"csa_ord_note","credits_info","liability_amt","cnlled_ord_promo_amt","cnlled_ord_coupon_amt","cnlled_ord_shipping_amt","cnlled_ord_shipping_tax_amt","cnlled_ord_sales_tax_amt",
	"cnlled_orig_ord_shipping_amt","cnlled_orig_ord_shipping_tax_amt","cnlled_orig_ord_sales_tax_amt","cnlled_ord_sub_total","cnlled_ord_total","cnlled_ord_total_charges",
	"cnlled_total_discs","cnlled_ord_total_taxes","ord_promo_amt","ord_coupon_amt","ord_shipping_amt","ord_shipping_tax_amt","ord_sales_tax_amt","orig_ord_shipping_amt",
	"orig_ord_shipping_tax_amt","orig_ord_sales_tax_amt","confirmed_ord_total","ord_sub_total","ord_total","ord_total_charges","ord_total_discs","ord_total_taxes",
	"rfnd_req_total_amt","rfnd_req_total_cc_amt","rfnd_req_total_paypal_amt","rfnd_act_total_amt","rfnd_act_total_cc_amt","rfnd_act_total_paypal_amt","rtn_act_total_amt","rtn_req_total_credits",
	"rtn_act_total_credits","rtn_act_adj_amt","rtn_req_adj_amt","rtn_act_exch_Credit_Amt","rtn_req_exch_Credit_Amt","archive_ts","business_dt","captured_ts",
	"confirmed_ts","created_ts","updated_ts" ]  %}

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
					"{{  fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_ord_mgmt_silver','mao_ord_order_v'), this, 'ord_pk' ) }}",
					"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge", 'update_condition': "active_flg"}
) }}

with
    mao_ord_order as (
        select
            oh.pk,
            oh.org_id,
            oh.order_id,
            oh.selling_location_id,
            oh.customer_id,
            oh.payment_status_id,
            oh.min_fulfillment_status_id,
            oh.max_fulfillment_status_id,
            oh.cancel_reason_id,
            oh.currency_code,
            oh.customer_type_id,
            oh.order_type_id,
            oh.max_return_status_id,
            oh.parent_reservation_request_id,
            oh.is_cancelled,
            oh.is_confirmed,
            oh.is_on_hold,
            oh.is_post_voided,
            oh.is_ready_for_tender,
            oh.is_tax_exempt,
            oh.is_tax_overridden,
            oh.ext_is_fraudservice_failed,
            oh.updated_by,
            oh.created_by,
            oh.customer_email,
            oh.customer_phone,
            oh.refund_payment_method,
            oh.liability_amount,
            oh.cancelled_order_sub_total,
            oh.cancelled_order_total,
            oh.cancelled_total_discounts,
            oh.confirmed_order_total,
            oh.order_sub_total,
            oh.order_total,
            oh.total_charges,
            oh.total_discounts,
            oh.total_taxes,
            oh.archive_date,
            oh.business_date,
            oh.captured_date,
            oh.confirmed_date,
            oh.created_timestamp,
            oh.updated_timestamp,
            oh.src_load_ts,
            parse_json(oh.json_store):"Fields" as json_fields
        from {{ source('src_ord_mgmt_silver','mao_ord_order_v') }} oh
        {% if is_incremental() %}
            where
                oh.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
    ),
    order_charge_detail_stg as (
        select
            org_id,
            order_pk,
            sum(case when lower(charge_type_id) = 'promotion' then coalesce(charge_total, requested_amount) end) as promo_charge_total,
            sum(case when lower(charge_type_id) = 'coupon' then coalesce(charge_total, requested_amount) end) as coupon_charge_total,
            sum(case when lower(charge_type_id) = 'shipping' and process not ilike 'woolin02riskifiedintegration%' then charge_total end) as shipping_charge_total,
			sum(case when lower(charge_type_id) = 'shipping' and process ilike 'woolin02riskifiedintegration%' then charge_total end) as orig_shipping_charge_total
        from {{ ref('mao_ord_order_charge_detail_hist_t') }}
        where lower(charge_type_id) in ('coupon', 'promotion', 'shipping')
            and coalesce(charge_total, requested_amount,0)  !=0
        group by all
    ),
    order_tax_detail_stg as (
		select org_id, order_pk,
            sum(case when coalesce(tax_detail_id,'') = 'Shipping_1' and process not ilike 'woolin02riskifiedintegration%' then tax_amount end) as shipping_tax_amount,
            sum(case when coalesce(tax_detail_id,'') != 'Shipping_1' and process not ilike 'woolin02riskifiedintegration%' then tax_amount end) as sales_tax_amount,
			sum(case when coalesce(tax_detail_id,'') = 'Shipping_1' and process ilike 'woolin02riskifiedintegration%' then tax_amount end) as orig_shipping_tax_amount,
            sum(case when coalesce(tax_detail_id,'') != 'Shipping_1' and process ilike 'woolin02riskifiedintegration%' then tax_amount end) as orig_sales_tax_amount
        from {{ ref('mao_ord_order_tax_detail_hist_t') }}
        where lower(tax_type_id) = 'sales'
            and coalesce(tax_amount,0)<>0
        group by all
    ),
    cancelled_charge_detail_stg as (
        select
            org_id,
            order_pk,
            sum(case when lower(charge_type_id) = 'promotion' then coalesce(charge_total, requested_amount) end) as promo_charge_total,
            sum(case when lower(charge_type_id) = 'coupon' then coalesce(charge_total, requested_amount) end) as coupon_charge_total,
            sum(case when lower(charge_type_id) = 'shipping' and process not ilike 'woolin02riskifiedintegration%' then charge_total end) as shipping_charge_total,
			sum(case when lower(charge_type_id) = 'shipping' and process ilike 'woolin02riskifiedintegration%' then charge_total end) as orig_shipping_charge_total
        from {{ ref('mao_ord_cancelled_charge_detail_hist_t') }}
        where lower(charge_type_id) in ('coupon', 'promotion', 'shipping')
            and coalesce(charge_total, requested_amount,0)  !=0
        group by all
    ),
    cancelled_order_tax_detail_stg as (
		select org_id, order_pk,
            sum(case when coalesce(tax_detail_id,'') = 'Shipping_1' and process not ilike 'woolin02riskifiedintegration%' then tax_amount end) as shipping_tax_amount,
            sum(case when coalesce(tax_detail_id,'') != 'Shipping_1' and process not ilike 'woolin02riskifiedintegration%' then tax_amount end) as sales_tax_amount,
			sum(case when coalesce(tax_detail_id,'') = 'Shipping_1' and process ilike 'woolin02riskifiedintegration%' then tax_amount end) as orig_shipping_tax_amount,
            sum(case when coalesce(tax_detail_id,'') != 'Shipping_1' and process ilike 'woolin02riskifiedintegration%' then tax_amount end) as orig_sales_tax_amount
        from {{ ref('mao_ord_cancelled_order_tax_detail_hist_t') }}
        where lower(tax_type_id) = 'sales'
            and coalesce(tax_amount,0)<>0        
        group by all
    ),
    status_definition_stg as (
        select
            status::decimal(38, 0) as status,
            lower(process_type_id) as process_type_id,
            description
        from {{ source('src_ord_mgmt_silver','mao_fw_status_definition_v') }}
        where lower(profile_id) = 'fl-us'
            and lower(process_type_id) in ('order_execution', 'return_execution')
    ),
    payment_stg as (
        select
            ph.org_id,
            ph.order_id,
            array_agg(distinct pm.payment_type) within group (order by pm.payment_type)::string as payment_type,
            max(pm.currency_code) as ccy_code,
            max(pm.ext_vendorid) as vendor_id,
            max(pt.is_prepaid) as is_prepaid
        from {{ source('src_payment_silver','mao_pay_payment_header_v') }} ph
        join (
            select org_id, payment_header_pk, payment_type, currency_code, ext_vendorid
            from {{ source('src_payment_silver','mao_pay_payment_method_v') }}
        ) pm on pm.org_id = ph.org_id and pm.payment_header_pk = ph.pk
        left join (
            select payment_type_id, is_prepaid
            from {{ source('src_payment_silver','mao_pay_payment_type_v') }}
            where lower(profile_id) = 'fl-us'
        ) pt on pm.payment_type = pt.payment_type_id
        group by all
    ),
    order_hold_stg as (
        select org_id, order_pk, status_id, resolve_reason_id, hold_type
        from {{ source('src_ord_mgmt_silver','mao_ord_order_hold_v') }}
        qualify row_number() over (partition by org_id, order_pk order by updated_timestamp desc) = 1
    ),
    order_ln_exch_stg as (
        select org_id, order_pk
        from {{ source('src_ord_mgmt_silver','mao_ord_order_line_v') }}
        group by all
        having sum(case when is_gift_card = 0 and is_return = 1 then 1 else 0 end) > 0
    ),
    rtn_rfnd_credits as (
        select
            mph.org_id,
            mph.order_id,
            sum(case when pms.payment_type in ('credit card', 'in store credit card') then ps.requested_refund_amount end) as rfnd_req_total_cc_amt,
            sum(case when pms.payment_type = 'paypal' then ps.requested_refund_amount end) as rfnd_req_total_paypal_amt,
            sum(case when pms.payment_type in ('credit card', 'in store credit card') then pms.current_refund_amount end) as rfnd_act_total_cc_amt,
            sum(case when pms.payment_type = 'paypal' then pms.current_refund_amount end) as rfnd_act_total_paypal_amt,
            sum(ps.requested_refund_amount) as rfnd_req_total_amt,
            sum(pms.current_refund_amount) as rfnd_act_total_amt,
            sum(ps.returned) as rtn_act_total_amt,
            sum(ps.rtn_req_total_credits) as rtn_req_total_credits,
            sum(ps.rtn_req_total_credits) as rtn_act_total_credits,
            array_agg(
                object_construct(
                    'adjustmentType', 101,
                    'adjustmentTypeDesc', 'NA',
                    'adjustmentTypeId', 'NA',
                    'amount', abs(ps.rtn_req_total_credits),
                    'override', 'NA'
                )
            ) within group (order by abs(ps.rtn_req_total_credits) desc)::string as credits_info
        from (
            select org_id, pk, order_id
            from {{ source('src_payment_silver','mao_pay_payment_header_v') }}
        ) mph
        left join (
            select org_id, payment_header_pk, lower(payment_type) as payment_type, current_refund_amount
            from {{ source('src_payment_silver','mao_pay_payment_method_v') }}
            where current_refund_amount > 0
        ) pms on mph.org_id = pms.org_id and mph.pk = pms.payment_header_pk
        left join (
            select
                org_id,
                order_id,
                sum(case when requested_refund_amount > 0 or returned <> 0 then requested_refund_amount end) as requested_refund_amount,
                sum(case when requested_refund_amount > 0 or returned <> 0 then returned end) as returned,
                sum(case when credit_amount > 0 and reference_type ilike 'return credit' then credit_amount end) as rtn_req_total_credits,
                sum(case when credit_in > 0 and reference_type ilike 'return credit' then credit_in end) as rtn_act_total_credits
            from {{ source('src_payment_silver','mao_pay_payment_summary_v') }}
            group by all
        ) ps on mph.org_id = ps.org_id and mph.order_id = ps.order_id
        group by all
    ),
    inv_stg as (
        select org_id, order_pk, invoice_id
        from {{ source('src_ord_mgmt_silver','mao_ord_invoice_v') }}
        qualify row_number() over (partition by org_id, order_pk order by updated_timestamp desc) = 1
    ),
    tracking_info as (
        select org_id, order_pk, tracking_number
        from {{ source('src_ord_mgmt_silver','mao_ord_order_tracking_info_v') }}
        qualify row_number() over (partition by org_id, order_pk order by updated_timestamp desc) = 1
    ),
    rtn_tracking_info as (
        select org_id, order_pk, return_tracking_number
        from {{ source('src_ord_mgmt_silver','mao_ord_return_tracking_detail_v') }}
        qualify row_number() over (partition by org_id, order_pk order by updated_timestamp desc) = 1
    ),
    ord_csaordernote as (
        select org_id, order_pk, note_text
        from {{ source('src_ord_mgmt_silver','mao_ord_order_note_v') }}
        where lower(note_type_id) = 'csaordernote'
        qualify row_number() over (partition by org_id, order_pk order by updated_timestamp desc) = 1
    ),
    ord_hdr_stg as (
        select
            oh.pk as ord_pk
            ,oh.pk
            ,oh.org_id as org_id
            ,oh.order_id as ord_id
            ,oh.selling_location_id as selling_loc_id
            ,oh.customer_id as cust_id
            ,oh.payment_status_id::decimal(38, 0) as pymt_status_id
            ,pstat.status_name as pymt_status_desc
            ,oo.status_id as hold_status_id
            ,oo.resolve_reason_id as resolve_reason_id
            ,oh.min_fulfillment_status_id::decimal(38, 0) as min_fulflmnt_status_id
            ,st2.description as min_fulflmnt_status_desc
            ,oh.max_fulfillment_status_id::decimal(38, 0) as max_fulflmnt_status_id
            ,st3.description as max_fulflmnt_status_desc
            ,oh.cancel_reason_id as cnl_reason_id
            ,re.short_description as cnl_reason_sdesc
            ,re.description as cnl_reason_desc
            ,re.reason_type_id as cnl_reason_type_id
            ,oh.currency_code as ccy_cd
            ,pm.payment_type as pymt_type
            ,pm.ccy_code as pymt_ccy_code
            ,oh.customer_type_id as cust_type_id
            ,ot.doc_type_id as doc_type_id
            ,oh.json_fields:"extend::OrderLocale"::string as ord_locale
            ,tr.tracking_number as tracking_num
            ,rt.return_tracking_number as rtn_tracking_num
            ,oh.max_return_status_id::decimal(38, 0) as rtn_status_id
            ,rs.description as rtn_status_desc
            ,oh.order_type_id as ord_type_id
            ,iv.invoice_id as inv_id
            ,oh.parent_reservation_request_id as prnt_resrv_req_id
            ,oh.is_cancelled::decimal(1, 0) as is_cnlled
            ,oh.is_confirmed::decimal(1, 0) as is_confirmed
            ,oh.is_on_hold::decimal(1, 0) as is_on_hold
            ,oh.is_post_voided::decimal(1, 0) as is_post_voided
            ,oh.is_ready_for_tender::decimal(1, 0) as is_ready_for_tender
            ,oh.is_tax_exempt::decimal(1, 0) as is_tax_exempt
            ,oh.is_tax_overridden::decimal(1, 0) as is_tax_overridden
            ,case when oh.json_fields:"extend::LaunchSKUFlag"::string != 'false' then 1 else 0 end as is_launch_sku_flg
            ,nvl(pm.is_prepaid, 0) as is_prepaid
            ,oh.ext_is_fraudservice_failed as is_fraud_service_failed
            ,pm.vendor_id as vendor_id
            ,oh.updated_by as ord_updtd_by
            ,oh.created_by as ord_created_by
            ,ot.short_description as ord_type_sdesc
            ,ot.description as ord_type_desc
            ,oo.hold_type as hold_type
            ,oh.json_fields:"extend::FLX-ID"::string as loyalty_num
            ,oh.customer_email as cust_email
            ,oh.customer_phone as cust_phone
            ,oh.refund_payment_method as refund_pymt_method
            ,oh.json_fields:"extend::browserIp"::string as browser_ip
            ,oh.json_fields:"extend::CartCompanyNumber"::string as Cart_Company_Num
            ,oh.json_fields:"extend::FLX-ID"::string as flx_id
            ,oh.json_fields:"extend::ReplacedOrder"::string as xstore_ord_id
            ,oh.json_fields:"extend::ShoppingCartId"::string as shopping_cart_id
            ,oh.json_fields:"extend::AssociateNumber"::string as associate_num
            ,oh.json_fields:"extend::userAgent"::string as usr_agnt
            ,oh.json_fields:"extend::source"::string as channel
            ,note.note_text as csa_ord_note
            ,rtrf.credits_info
            ,oh.liability_amount as liability_amt
            ,cnl.promo_charge_total as cnlled_ord_promo_amt
            ,cnl.coupon_charge_total as cnlled_ord_coupon_amt
            ,coalesce(cnl.orig_shipping_charge_total,cnl.shipping_charge_total) as cnlled_ord_shipping_amt
            ,coalesce(cst.orig_shipping_tax_amount,cst.shipping_tax_amount) as cnlled_ord_shipping_tax_amt
			,coalesce(cst.orig_sales_tax_amount,cst.sales_tax_amount) as cnlled_ord_sales_tax_amt
            ,cnl.orig_shipping_charge_total as cnlled_orig_ord_shipping_amt
            ,cst.orig_shipping_tax_amount as cnlled_orig_ord_shipping_tax_amt
			,cst.orig_sales_tax_amount as cnlled_orig_ord_sales_tax_amt
			,oh.cancelled_order_sub_total as cnlled_ord_sub_total
            ,oh.cancelled_order_total as cnlled_ord_total
            ,oh.json_fields:"CancelledTotalCharges"::string as cnlled_ord_total_charges
            ,oh.cancelled_total_discounts as cnlled_total_discs
            ,oh.json_fields:"CancelledTotalTaxes"::string as cnlled_ord_total_taxes
            ,ocd.promo_charge_total as ord_promo_amt
            ,ocd.coupon_charge_total as ord_coupon_amt
            ,coalesce(ocd.orig_shipping_charge_total,ocd.shipping_charge_total) as ord_shipping_amt
            ,coalesce(st.orig_shipping_tax_amount,st.shipping_tax_amount) as ord_shipping_tax_amt
			,coalesce(st.orig_sales_tax_amount,st.sales_tax_amount) as ord_sales_tax_amt
            ,ocd.orig_shipping_charge_total as orig_ord_shipping_amt
            ,st.orig_shipping_tax_amount as orig_ord_shipping_tax_amt
			,st.orig_sales_tax_amount as orig_ord_sales_tax_amt
            ,oh.confirmed_order_total as confirmed_ord_total
            ,oh.order_sub_total as ord_sub_total
            ,oh.order_total as ord_total
            ,oh.total_charges as ord_total_charges
            ,oh.total_discounts as ord_total_discs
            ,oh.total_taxes as ord_total_taxes
            ,rtrf.rfnd_req_total_amt
            ,rtrf.rfnd_req_total_cc_amt
            ,rtrf.rfnd_req_total_paypal_amt
            ,rtrf.rfnd_act_total_amt
            ,rtrf.rfnd_act_total_cc_amt
            ,rtrf.rfnd_act_total_paypal_amt
            ,rtrf.rtn_act_total_amt
            ,rtrf.rtn_req_total_credits
            ,rtrf.rtn_act_total_credits
            ,oh.order_total - rtrf.rfnd_act_total_amt as rtn_act_adj_amt
            ,oh.order_total - rtrf.rfnd_req_total_amt as rtn_req_adj_amt
            ,case when ex.order_pk is not null then rtrf.rtn_act_total_credits end as rtn_act_exch_Credit_Amt
            ,case when ex.order_pk is not null then rtrf.rtn_req_total_credits end as rtn_req_exch_Credit_Amt
            ,oh.archive_date::timestamp_ntz as archive_ts
            ,oh.business_date::timestamp_ntz as business_dt
            ,oh.captured_date::timestamp_ntz as captured_ts
            ,oh.confirmed_date::timestamp_ntz as confirmed_ts
            ,oh.created_timestamp::timestamp_ntz as created_ts
            ,oh.updated_timestamp::timestamp_ntz as updated_ts
            ,oh.src_load_ts as src_load_ts
        from mao_ord_order oh
            left join order_hold_stg oo on oh.org_id = oo.org_id and oh.pk = oo.order_pk
            left join (
                select order_type_id, doc_type_id, short_description, description
                from {{ source('src_ord_mgmt_silver','mao_ord_order_type_v') }}
                where lower(profile_id) = 'fl-us'
            ) ot on oh.order_type_id = ot.order_type_id
            left join (
                select reason_id, short_description, description, reason_type_id
                from {{ source('src_ord_mgmt_silver','mao_ord_reason_v') }}
                where lower(profile_id) = 'fl-us'
            ) re on oh.cancel_reason_id = re.reason_id
            left join (
                select status_id, status_name
                from {{ source('src_payment_silver','mao_pay_payment_status_v') }}
            ) pstat on pstat.status_id = oh.payment_status_id
            left join tracking_info tr on tr.org_id = oh.org_id and tr.order_pk = oh.pk
            left join rtn_tracking_info rt on rt.org_id = oh.org_id and rt.order_pk = oh.pk
            left join inv_stg iv on oh.org_id = iv.org_id and oh.pk = iv.order_pk
            left join order_ln_exch_stg ex on oh.org_id = ex.org_id and oh.pk = ex.order_pk
            left join status_definition_stg st2 on oh.min_fulfillment_status_id::decimal(38, 0) = st2.status and st2.process_type_id = 'order_execution'
            left join status_definition_stg st3 on oh.max_fulfillment_status_id::decimal(38, 0) = st3.status and st3.process_type_id = 'order_execution'
            left join status_definition_stg rs on oh.max_return_status_id = rs.status and rs.process_type_id = 'return_execution'
            left join payment_stg pm on oh.org_id = pm.org_id and oh.order_id = pm.order_id
            left join rtn_rfnd_credits rtrf on oh.org_id = rtrf.org_id and oh.order_id = rtrf.order_id
            left join ord_csaordernote note on note.org_id = oh.org_id and note.order_pk = oh.pk
            left join order_charge_detail_stg ocd on ocd.org_id = oh.org_id and ocd.order_pk = oh.pk
            left join order_tax_detail_stg st on st.org_id = oh.org_id and st.order_pk = oh.pk
            left join cancelled_charge_detail_stg cnl on cnl.org_id = oh.org_id and cnl.order_pk = oh.pk
            left join cancelled_order_tax_detail_stg cst on cst.org_id = oh.org_id and cst.order_pk = oh.pk
    ),
    ord_hdr_main as (
        select
            src.*,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
        from
            ord_hdr_stg as src
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
    ord_hdr_main as src
