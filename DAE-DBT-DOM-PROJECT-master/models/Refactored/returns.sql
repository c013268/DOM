{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}

{{ config(
    materialized="incremental",
    unique_key=["order_id", "return_id", "lines_linenumber", "return_status"],
    post_hook=["{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'load_time_adls' ) }}"],
    meta={'strategy': "merge"}
) }}

WITH base_ord_hdr_hist AS (
    SELECT *
    FROM {{ source('dom_gold', 'fct_mao_ord_hdr_hist_v') }}
    WHERE doc_type_id = 'CustomerOrder'
),
base_ord_hdr AS (
    SELECT *
    FROM base_ord_hdr_hist
    QUALIFY ROW_NUMBER() OVER (PARTITION BY org_id, ord_id ORDER BY updated_ts DESC) = 1
),
base_ord_line_hist AS (
    SELECT ol.*
    FROM {{ source('dom_gold', 'fct_mao_ord_line_hist_v') }} ol
    JOIN base_ord_hdr oh
        ON ol.org_id = oh.org_id AND ol.ord_id = oh.ord_id
),
base_ord_line AS (
    SELECT *
    FROM base_ord_line_hist
    QUALIFY ROW_NUMBER() OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY updated_ts DESC) = 1
),
product_master AS (
    SELECT
        pm.internal_product_number_flca,
        pm.internal_product_number,
        pm.legacy_size_desc,
        pm.online_us_sku,
        pm.online_ca_sku,
        pm.global_size_id,
        pm.banner_id,
        pm.desc_long_2,
        pm.global_brand_desc,
        pm.fob_desc,
        pm."DESC",
        pm.tax_code,
        pm.designator_id
    FROM {{ ref('product_master_v') }} pm
    WHERE pm.banner_id IN ('81', '98')
    GROUP BY ALL
),
product_master_div AS (
    SELECT
        internal_product_number_flca,
        internal_product_number,
        legacy_size_desc,
        online_us_sku,
        online_ca_sku,
        CONCAT(TRIM(legacy_sku), '-', TRIM(legacy_size_code)) AS legacy_sku_size,
        global_size_id,
        banner_id,
        CASE
            WHEN banner_id = '03' THEN 'FL-US'
            WHEN banner_id = '16' THEN 'KFL-US'
            WHEN banner_id = '18' THEN 'CH-US'
            WHEN banner_id = '76' THEN 'FL-CA'
            WHEN banner_id = '77' THEN 'CH-CA'
        END AS org_desc,
        global_brand_desc,
        fob_desc,
        desc_long_2,
        "DESC",
        designator_id,
        cost,
        size_default_established_cost,
        size_default_established_cost_flca,
        tax_code
    FROM {{ ref('product_master_v') }}
    WHERE banner_id IN ('03', '16', '18', '76', '77')
    GROUP BY ALL
),
fct_mao_ord_line_stg AS (
    SELECT
        ord_line.*,
        CASE
            WHEN COALESCE(ord_line.max_fulflmnt_status_id, ord_hdr.rtn_status_id) = '11000' THEN 'CREATED'
            WHEN COALESCE(ord_line.max_fulflmnt_status_id, ord_hdr.rtn_status_id) = '18000' THEN 'RETURN_COMPLETE'
            WHEN COALESCE(ord_line.max_fulflmnt_status_id, ord_hdr.rtn_status_id) = '19000' THEN 'CANCELLED'
            ELSE UPPER(TRIM(COALESCE(ord_line.max_fulflmnt_status_desc, ord_hdr.rtn_status_desc)))
        END AS return_status,
		FIRST_VALUE(ord_line.orig_unit_price) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_orig_unit_price,
        FIRST_VALUE(ord_line.unit_price) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_unit_price,
        FIRST_VALUE(ord_line.cnlled_total_disc) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_cnlled_total_disc,
        FIRST_VALUE(ord_line.total_disc) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_total_disc,
        FIRST_VALUE(ord_line.cnlled_ord_ln_total) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_cnlled_ord_ln_total,
        FIRST_VALUE(ord_line.ord_ln_total) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_ord_ln_total,
        FIRST_VALUE(ord_line.cnlled_total_charges) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_cnlled_total_charges,
        FIRST_VALUE(ord_line.total_charges) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_total_charges,
        FIRST_VALUE(ord_line.cnlled_total_taxes) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_cnlled_total_taxes,
        FIRST_VALUE(ord_line.total_taxes) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_total_taxes,
        FIRST_VALUE(ord_line.cnlled_orig_ord_shipping_tax_amt) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_cnlled_ord_shipping_tax_amt,
        FIRST_VALUE(ord_line.cnlled_orig_ord_shipping_amt) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_cnlled_ord_shipping_amt,
        FIRST_VALUE(ord_line.orig_ord_shipping_amt) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_ord_shipping_amt,
        FIRST_VALUE(ord_line.orig_ord_shipping_tax_amt) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_ord_shipping_tax_amt,
        FIRST_VALUE(ord_line.cnlled_ord_ln_sub_total) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_cnlled_ord_ln_sub_total,
        FIRST_VALUE(ord_line.ord_ln_sub_total) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_ord_ln_sub_total,
        FIRST_VALUE(ord_line.cnlled_orig_ord_sales_tax_amt) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_cnlled_ord_sales_tax_amt,
        FIRST_VALUE(ord_line.orig_ord_sales_tax_amt) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_ord_sales_tax_amt,
		FIRST_VALUE(ord_line.gift_card_value) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_gift_card_value,
        ROW_NUMBER() OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_rnk
    FROM base_ord_line_hist ord_line
    JOIN base_ord_hdr ord_hdr
        ON ord_line.org_id = ord_hdr.org_id AND ord_line.ord_id = ord_hdr.ord_id
    WHERE ord_hdr.doc_type_id = 'CustomerOrder'
        AND ord_line.max_fulflmnt_status_id IS NOT NULL
        AND ord_line.max_fulflmnt_status_id > '9000'
        {% if is_incremental() %}
            AND ord_line.etl_updt_ts >= {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL_DBIMART") | as_text }}'
        {% endif %}
),
fct_mao_ord_line AS (
    SELECT *
    FROM (
        SELECT
            ord_line.*,
            ROW_NUMBER() OVER (
                PARTITION BY org_id, ord_id, ord_ln_id, return_status
                ORDER BY updated_ts DESC
            ) AS ord_ln_status_rnk
        FROM fct_mao_ord_line_stg ord_line
    )
    WHERE ord_ln_status_rnk = 1
),
dim_location AS (
    SELECT *
    FROM (
        SELECT
            loc_snum,
            loc_num,
            ROW_NUMBER() OVER (PARTITION BY LPAD(loc_snum, 5, '0') ORDER BY loc_sk DESC, loc_seq_num DESC) AS loc_rnk
        FROM {{ source('location_gold', 'dim_location_v') }}
        WHERE UPPER(banner_geo) = 'NA'
    )
    WHERE loc_rnk = 1
),
fct_exchange_orders AS (
    SELECT DISTINCT
        ol.prnt_ord_id,
        ol.prnt_ord_ln_id,
        ol.org_id,
        ol.ord_id,
        ol.is_even_exchg
    FROM base_ord_line ol
    WHERE ol.is_even_exchg = 1
        AND ol.prnt_ord_id IS NOT NULL
        AND ol.max_fulflmnt_status_id IS NOT NULL
),
fct_original_orders AS (
    SELECT
        ol.org_id,
        ol.ord_id,
        ol.ord_ln_id,
        oh.xstore_ord_id,
        ol.is_refund_gift_card,
        ol.is_restockable,
        ol.ord_shipping_amt,
        ol.ord_shipping_tax_amt
    FROM base_ord_line ol
    JOIN base_ord_hdr oh
        ON ol.org_id = oh.org_id AND ol.ord_id = oh.ord_id
    WHERE ol.max_fulflmnt_status_id IS NOT NULL
        AND ol.prnt_ord_id IS NULL
),
fct_mao_ord_tax_agg as (
	select org_id ol_org_id,ord_id as ol_ord_id,ord_ln_id as ol_ord_ln_id,
		ABS(SUM(COALESCE(fv.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(fv.fv_ord_shipping_tax_amt, 0))) AS shippingtaxamount,
		ABS(SUM(COALESCE(fv.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(fv.fv_ord_sales_tax_amt, 0))) AS taxamount
	from 
		fct_mao_ord_line fv
    where fv_rnk = 1
	group by all
),
payment as (
    select 
        pymt.* exclude(pymt_txn_amt),
        case 
            when pymt.pymt_txn_cnt = 1 then coalesce(pymt.inv_ln_total, pymt.pymt_txn_amt, 0)
            else coalesce(pymt.pymt_txn_amt, 0)
        end as pymt_txn_amt
    from (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY org_id, ord_id, ord_ln_id, pymt_txn_id,pymt_txn_dtl_id, inv_id,inv_ln_id
                ORDER BY inv_ln_updated_ts desc, src_load_ts desc
            ) AS pymt_rnk,
            COUNT(DISTINCT pymt_txn_id || '~' || coalesce(pymt_txn_dtl_id, '')) OVER (
                PARTITION BY org_id, ord_id, ord_ln_id, inv_id,inv_ln_id
            ) AS pymt_txn_cnt
        FROM {{ source('dom_gold', 'fct_mao_ord_pymt_line_v') }}
        WHERE lower(pymt_txn_type) not in ('authorization', 'authorization reversal','refund')
    ) pymt 
    where pymt.pymt_rnk = 1
),
payment_grouped AS (
    SELECT
        org_id,
        ord_id,
		ord_ln_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'amount', CAST(ABS(COALESCE(pymt_txn_amt,0)) AS VARCHAR),
                'authCode', CAST(auth_cd AS VARCHAR),
                'cardLast4', CAST(txn_card_last4 AS VARCHAR),
                'cegrRefId', CAST(inv_id AS VARCHAR),
                'paymentTransactionId', CAST(pymt_txn_id AS VARCHAR),
                'paymentTransactionSubType', CAST(NULL AS VARCHAR),
                'paymentTransactionType', CAST(pymt_txn_type AS VARCHAR),
                'paymentType', CAST(CASE WHEN pymt_type = 'Gift Card' THEN 'GIFTCARD' WHEN pymt_type = 'Credit Card' THEN 'CREDITCARD' ELSE UPPER(pymt_type) END AS STRING),
                'creditCardType', CAST(pymt_card_type AS VARCHAR),
                'date', CAST(created_ts AS VARCHAR),
                'shippingTaxAmount', CAST(shippingtaxamount AS VARCHAR),
                'taxAmount', CAST(taxamount AS VARCHAR),
                'authorization', OBJECT_CONSTRUCT(
                    'attributes', OBJECT_CONSTRUCT(
                        'authResponse', CAST(NULL AS VARCHAR),
                        'avsCode', CAST(attrib_avs_code AS VARCHAR),
                        'cardAlias', CAST(card_alias AS VARCHAR),
                        'cardBin', CAST(pymt_grp_id AS VARCHAR),
                        'cardLast4', CAST(attrib_card_last4 AS VARCHAR),
                        'cardToken', CAST(card_token AS VARCHAR),
                        'cardType', CAST(attrib_card_type_display AS VARCHAR),
                        'confirmationCode', CAST(NULL AS VARCHAR),
                        'cvvResponse', CAST(attrib_cvv_response AS VARCHAR),
                        'email', CAST(addr_email AS VARCHAR),
                        'expirationDate', CAST(attrib_card_expiry_dt AS VARCHAR),
                        'giftCardNumber', CAST(CASE WHEN pymt_type = 'Gift Card' THEN attrib_card_last4 END AS VARCHAR),
                        'sellerProtection', CAST(CASE WHEN pymt_type = 'Gift Card' THEN SELLER_PROTECTION_STATUS END AS VARCHAR)
                    ),
                    'authAmount', CAST(ABS(COALESCE(pymt_txn_req_amt,0)) AS VARCHAR),
                    'authCode', CAST(auth_cd AS VARCHAR),
                    'errorMessage', CAST(NULL AS VARCHAR),
                    'id', CAST(NULL AS VARCHAR),
                    'originalOrderNumber', CAST(ord_id AS VARCHAR),
                    'paymentType', CAST(pymt_type AS VARCHAR),
                    'preSettled', CAST(NULL AS VARCHAR),
                    'transactionDate', CAST(COALESCE(pymt_txn_dt, pymt_txn_req_dt, created_ts) AS VARCHAR),
                    'transactionId', CAST(pymt_txn_id AS VARCHAR)
                ),
                'creditCard', CASE WHEN pymt_type = 'Credit Card' THEN
                    OBJECT_CONSTRUCT(
                        'authInfo', OBJECT_CONSTRUCT(
                            'authAmount', CAST(ABS(COALESCE(pymt_txn_req_amt,0)) AS VARCHAR),
                            'authCode', CAST(auth_cd AS VARCHAR),
                            'authResponse', CAST(NULL AS VARCHAR),
                            'authTime', CAST(COALESCE(pymt_txn_dt, pymt_txn_req_dt, created_ts) AS VARCHAR),
                            'avsCode', CAST(attrib_avs_code AS VARCHAR),
                            'cvvResponse', CAST(attrib_cvv_response AS VARCHAR),
                            'originalOrderNumber', CAST(ord_id AS VARCHAR),
                            'preSettled', CAST(NULL AS VARCHAR),
                            'referenceNumber', CAST(transaction_ref_id AS VARCHAR),
                            'transactionId', CAST(pymt_txn_id AS VARCHAR)
                        ),
                        'cardAlias', CAST(card_alias AS VARCHAR),
                        'cardBin', CAST(pymt_grp_id AS VARCHAR),
                        'cardLast4', CAST(txn_card_last4 AS VARCHAR),
                        'cardToken', CAST(card_token AS VARCHAR),
                        'expirationDate', CAST(attrib_card_expiry_dt AS VARCHAR),
                        'type', CAST(attrib_card_type_display AS VARCHAR)
                    )
                ELSE NULL END,
                'giftCard', CASE WHEN pymt_type = 'Gift Card' THEN
                    OBJECT_CONSTRUCT(
                        'amount', CAST(ABS(COALESCE(pymt_txn_amt,0)) AS VARCHAR),
                        'authCode', CAST(auth_cd AS VARCHAR),
                        'giftCardNumber', CAST(attrib_card_last4 AS VARCHAR),
                        'originalOrderNumber', CAST(ord_id AS VARCHAR),
                        'preSettled', CAST(NULL AS VARCHAR),
                        'transactionDate', CAST(COALESCE(pymt_txn_dt, pymt_txn_req_dt, created_ts) AS VARCHAR),
                        'transactionId', CAST(pymt_txn_id AS VARCHAR)
                    )
                ELSE NULL END,
                'paypal', CASE WHEN pymt_type = 'PayPal' THEN
                    OBJECT_CONSTRUCT(
                        'amount', CAST(ABS(COALESCE(pymt_txn_amt,0)) AS VARCHAR),
                        'authInfo', OBJECT_CONSTRUCT(
                            'authAmount', CAST(ABS(COALESCE(pymt_txn_req_amt,0)) AS VARCHAR),
                            'authCode', CAST(auth_cd AS VARCHAR),
                            'authResponse', CAST(NULL AS VARCHAR),
                            'authTime', CAST(COALESCE(pymt_txn_dt, pymt_txn_req_dt, created_ts) AS VARCHAR),
                            'avsCode', CAST(attrib_avs_code AS VARCHAR),
                            'cvvResponse', CAST(attrib_cvv_response AS VARCHAR),
                            'originalOrderNumber', CAST(ord_id AS VARCHAR),
                            'preSettled', CAST(NULL AS VARCHAR),
                            'referenceNumber', CAST(transaction_ref_id AS VARCHAR),
                            'transactionId', CAST(pymt_txn_id AS VARCHAR)
                        ),
                        'paypalEmailId', CAST(addr_email AS VARCHAR),
                        'transactionDate', CAST(COALESCE(pymt_txn_dt, pymt_txn_req_dt, created_ts) AS VARCHAR),
                        'transactionId', CAST(pymt_txn_id AS VARCHAR)
                    )
                ELSE NULL END
            )
        ) AS paymentsinfo
    FROM payment pymt left join fct_mao_ord_tax_agg ord on  (pymt.org_id=ord.ol_org_id and pymt.ord_id=ord.ol_ord_id and pymt.ord_ln_id=ord.ol_ord_ln_id)
    WHERE pymt.pymt_rnk = 1
    GROUP BY all
),
returns_landing AS (
    SELECT
        CAST(NVL(ord_line.prnt_ord_id, ord_line.ord_id) AS VARCHAR) AS order_id,
        ord_hdr.created_ts AS order_datetime,
        CAST(ord_line.ord_id AS VARCHAR) AS return_id,
        CAST(CASE TRIM(ord_line.org_id)
            WHEN 'FL-US' THEN '21'
            WHEN 'FL-CA' THEN '45'
            WHEN 'KFL-US' THEN '22'
            WHEN 'CH-CA' THEN '77'
            WHEN 'CH-US' THEN '20'
        END AS VARCHAR) AS company_number,
        ord_line.return_status AS return_status,
        CAST(ord_line.ord_id AS VARCHAR) AS return_number,
        CAST(CASE WHEN ord_line.is_refund_gift_card = 1 OR orig.is_refund_gift_card = 1 THEN 'E_GIFT_CARD' ELSE UPPER(ord_hdr.refund_pymt_method) END AS VARCHAR) AS refund_method,
        CAST(CASE
            WHEN UPPER(loc.loc_type_id) = 'STORE' THEN 'XSTORE'
            WHEN UPPER(loc.loc_type_id) = 'DC' AND UPPER(TRIM(loc.loc_addr_city)) = 'RENO' THEN 'RENO'
            WHEN UPPER(loc.loc_type_id) = 'DC' AND UPPER(TRIM(loc.loc_addr_city)) = 'CAMP HILL' THEN 'CAMPHILL'
            WHEN UPPER(loc.loc_type_id) = 'DC' AND UPPER(TRIM(loc.loc_addr_city)) = 'JUNCTION CITY' THEN 'JC'
            WHEN UPPER(loc.loc_type_id) = 'DC' AND UPPER(TRIM(loc.loc_addr_city)) = 'MILTON' THEN 'MILTON'
            WHEN UPPER(loc.loc_type_id) = 'DC' THEN UPPER(TRIM(loc.loc_addr_city))
        END AS VARCHAR) AS return_location,
        CAST(ord_exch.ord_id AS VARCHAR) AS exchangeordernumber,
        CAST(ord_exch.ord_id AS VARCHAR) AS exchangenumber,
        COALESCE(ord_hdr.confirmed_ts, ord_hdr.captured_ts) AS return_date,
        CAST(ord_line.updated_by AS VARCHAR) AS return_agent,
        CAST(ord_line.txn_ref_id AS VARCHAR) AS return_taxtransid,
        CAST(ABS(COALESCE(ord_hdr.rtn_act_adj_amt, 0)) AS VARCHAR) AS return_act_adjustmentamount,
        CAST(ABS(COALESCE(ord_hdr.rtn_act_total_credits, 0)) AS VARCHAR) AS return_act_creditsamount,
        CAST(ABS(SUM(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS return_act_discountamount,
        CAST(ABS(SUM((ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
					ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
					ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
					ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0))) -
					ABS(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0))) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS return_act_discountedtotalamount,
        CAST(ABS(COALESCE(ord_hdr.rfnd_act_total_amt, 0)) AS VARCHAR) AS return_act_refundtotalamount,
        CAST(ABS(COALESCE(ord_hdr.rtn_act_total_amt, 0)) AS VARCHAR) AS return_act_totalreturnamount,
        CAST(ABS(COALESCE(ord_hdr.rtn_act_exch_credit_amt, 0)) AS VARCHAR) AS return_act_exchangecreditamount,
        CAST(ABS(COALESCE(ord_hdr.rfnd_act_total_cc_amt, 0)) AS VARCHAR) AS return_act_creditcardrefundamount,
        CAST(ABS(COALESCE(ord_hdr.rfnd_act_total_paypal_amt, 0)) AS VARCHAR) AS return_act_paypalrefundamount,
        CAST(CASE WHEN ord_line.is_refund_gift_card = 1 THEN 'E_GIFT_CARD' ELSE ABS(COALESCE(ord_hdr.rfnd_act_total_amt, 0)) END AS VARCHAR) AS return_act_giftcardrefundamount,
        CAST(ABS(SUM(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS return_act_shippingamount,
        CAST(ABS(SUM(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS return_act_shippingtaxamount,
        CAST(ABS(SUM(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS return_act_subtotalamount,
        CAST(ABS(SUM(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS return_act_taxamount,
        CAST(ABS(COALESCE(ord_hdr.rtn_req_adj_amt, 0)) AS VARCHAR) AS req_tot_adjustmentamount,
        CAST(ABS(COALESCE(ord_hdr.rtn_req_total_credits, 0)) AS VARCHAR) AS req_tot_creditsamount,
        CAST(ABS(SUM(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS req_tot_discountamount,
        CAST(ABS(SUM((ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
					ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
					ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
					ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0))) -
					ABS(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0))) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS req_tot_discountedtotalamount,
        CAST(ABS(COALESCE(ord_hdr.rfnd_req_total_amt, 0)) AS VARCHAR) AS req_tot_refundtotalamount,
        CAST(ABS(COALESCE(ord_hdr.rtn_req_total_credits, 0)) AS VARCHAR) AS req_tot_totalreturnamount,
        CAST(ABS(COALESCE(ord_hdr.rtn_req_exch_credit_amt, 0)) AS VARCHAR) AS req_tot_exchangecreditamount,
        CAST(ABS(COALESCE(ord_hdr.rfnd_req_total_cc_amt, 0)) AS VARCHAR) AS req_tot_creditcardrefundamount,
        CAST(ABS(COALESCE(ord_hdr.rfnd_req_total_paypal_amt, 0)) AS VARCHAR) AS req_tot_paypalrefundamount,
        CAST(CASE WHEN ord_line.is_refund_gift_card = 1 THEN 'E_GIFT_CARD' ELSE ABS(COALESCE(ord_hdr.rfnd_act_total_amt, 0)) END AS VARCHAR) AS req_tot_giftcardrefundamount,
        CAST(ABS(SUM(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS req_tot_shippingamount,
        CAST(ABS(SUM(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS req_tot_shippingtaxamount,
        CAST(ABS(SUM(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS req_tot_subtotalamount,
        CAST(ABS(SUM(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS VARCHAR) AS req_tot_taxamount,
        CAST(CASE WHEN ord_hdr.pymt_status_desc IN ('Refunded', 'Awaiting Refund') THEN ord_line.ord_id END AS VARCHAR) AS lines_refundnum,
        CAST(NULL AS VARCHAR) AS lines_act_priceoverridereason,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0)) AS VARCHAR) AS lines_act_discountamount,
        CAST((ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0))) -
			ABS(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0)) AS VARCHAR) AS lines_act_discountedtotalamount,
        CAST(ABS(COALESCE(ord_line.fv_orig_unit_price, ord_line.fv_unit_price, 0)) AS VARCHAR) AS lines_act_originalretailprice,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) AS VARCHAR) AS lines_act_shippingamount,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) AS VARCHAR) AS lines_act_shippingtaxamount,
        CAST(NULL AS VARCHAR) AS lines_act_giftboxamount,
        CAST(NULL AS VARCHAR) AS lines_act_giftboxtaxamount,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) AS VARCHAR) AS lines_act_subtotalamount,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS VARCHAR) AS lines_act_taxamount,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS VARCHAR) AS lines_act_totalamount,
        CAST(ABS(COALESCE(ord_line.fv_unit_price, 0)) AS VARCHAR) AS lines_act_unitprice,
        CAST(CASE WHEN ord_line.is_backorderflg = 1 THEN 'true' WHEN ord_line.is_backorderflg = 0 THEN 'false' END AS VARCHAR) AS product_backorderflag,
        CAST(UPPER(CASE WHEN ord_line.is_gift_card = 1 THEN ord_line.itm_brand ELSE pm.global_brand_desc END) AS VARCHAR) AS product_brand,
        CAST(CASE WHEN ord_line.is_gift_card = 1 THEN ord_line.itm_dept_name ELSE pm.fob_desc END AS VARCHAR) AS product_category,
        CAST(CASE WHEN ord_line.is_gift_card = 1 THEN ord_line.itm_color_desc ELSE pm.desc_long_2 END AS VARCHAR) AS product_color,
        CAST(pm."DESC" AS VARCHAR) AS product_description,
        CAST(ord_line.small_image_u_r_i AS VARCHAR) AS product_image,
        CAST(CASE WHEN ord_hdr.is_prepaid = 0 THEN 'false' END AS VARCHAR) AS product_iscollectupfront,
        CAST(CASE WHEN ord_line.is_launch_sku_flg = 1 THEN 'true' WHEN ord_line.is_launch_sku_flg = 0 THEN 'false' END AS VARCHAR) AS product_launchskuflag,
        CAST(ord_line.itm_desc AS VARCHAR) AS product_name,
        CAST(CASE WHEN ord_line.is_gift_card = 1 THEN 'GFT' ELSE pm.designator_id END AS VARCHAR) AS productdesignator,
        CAST(TRIM(CASE
            WHEN ord_line.item_id = 'ECARD20' THEN '2138264'
            WHEN ord_line.item_id = 'ECARD21' THEN '2138265'
            WHEN ord_line.item_id = 'ECARD22' THEN '2138266'
            WHEN ord_line.item_id = 'ECARD45' THEN '20'
            WHEN ord_line.item_id = 'ECARD77' THEN '2000003'
            WHEN ord_line.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
            WHEN ord_line.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.internal_product_number
            ELSE ord_line.item_id
        END) AS VARCHAR) AS product_number,
        CAST(ord_line.product_type AS VARCHAR) AS product_type,
        CAST(CASE WHEN ord_line.is_gift_card = 1 THEN ord_line.itm_size ELSE pm.legacy_size_desc END AS VARCHAR) AS product_size,
        CAST(CASE
            WHEN ord_line.is_gift_card = 1 THEN ord_line.item_id
            WHEN ord_hdr.channel != 'XSTORE' AND ord_hdr.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.online_us_sku
            WHEN ord_hdr.channel != 'XSTORE' AND ord_hdr.org_id IN ('FL-CA', 'CH-CA') THEN pm.online_ca_sku
            WHEN ord_hdr.channel = 'XSTORE' THEN pm_div.legacy_sku_size
            ELSE itm.color
        END AS VARCHAR) AS product_sku,
        CAST(CASE WHEN ord_line.is_gift_card = 1 THEN ord_line.itm_tax_cd ELSE pm.tax_code END AS VARCHAR) AS product_taxcode,
        CAST(CASE WHEN ord_line.max_fulflmnt_status_id IN ('9000', '19000') THEN ord_line.orig_ord_qty ELSE ord_line.qty END AS VARCHAR) AS lines_qty,
        CASE
            WHEN TRIM(ord_line.org_id) IN ('CH-US', 'FL-US', 'KFL-US', 'FL-CA', 'CH-CA') THEN
                CASE
                    WHEN UPPER(ord_line.rtn_reason) = 'COLOR MISMATCH' THEN 'PC'
                    WHEN UPPER(ord_line.rtn_reason) = 'BOSS WRONG ITEM' THEN 'BW'
                    WHEN UPPER(ord_line.rtn_reason) = 'DROP SHIP WRONG ITEM' THEN 'DI'
                    WHEN UPPER(ord_line.rtn_reason) = 'EMBROIDERY QUALITY' THEN 'EQ'
                    WHEN UPPER(ord_line.rtn_reason) = 'EB4KIDS FIT GUARNTEE' THEN 'FG'
                    WHEN UPPER(ord_line.rtn_reason) = 'DO NOT USE' THEN 'LS'
                    WHEN UPPER(ord_line.rtn_reason) = 'WRONG ART GRAPHIC' THEN 'PA'
                    WHEN UPPER(ord_line.rtn_reason) = 'PERSONALIZED-BUYER' THEN 'PB'
                    WHEN UPPER(ord_line.rtn_reason) = 'WRONG COLOR' THEN 'PC'
                    WHEN UPPER(ord_line.rtn_reason) = 'PERSONALZD WRONG ITM' THEN 'PI'
                    WHEN UPPER(ord_line.rtn_reason) = 'PERSONALZD UNWANTED' THEN 'PN'
                    WHEN UPPER(ord_line.rtn_reason) = 'PRINTING QUALITY' THEN 'PQ'
                    WHEN UPPER(ord_line.rtn_reason) = 'MIS-SPELLING' THEN 'PS'
                    WHEN UPPER(ord_line.rtn_reason) = 'PERSONALIZED-AGENT' THEN 'PU'
                    WHEN UPPER(ord_line.rtn_reason) = 'DIDN''T HOLD UP' THEN 'PW'
                    WHEN UPPER(ord_line.rtn_reason) = 'STORE DID NOT REFUND' THEN 'SN'
                    WHEN UPPER(ord_line.rtn_reason) = 'STORE DID REFUND' THEN 'SY'
                    WHEN UPPER(ord_line.rtn_reason) = 'USED CLEAT' THEN 'UC'
                    WHEN UPPER(ord_line.rtn_reason) = 'USED SHOE NOT DEFECT' THEN 'US'
                    WHEN UPPER(ord_line.rtn_reason) = 'WRONG ITEM ORDERED' THEN 'WO'
                    WHEN UPPER(ord_line.rtn_reason) = 'DAMAGED IN TRANSIT (throw in trash)' THEN 'WT'
                    WHEN UPPER(ord_line.rtn_reason) = 'XSTORE RETURN' THEN 'XX'
                    WHEN UPPER(ord_line.rtn_reason) IN ('DAMAGED', 'DEFECTIVE ITEM', 'DEFECTIVE ITEM NO RETURN FEE', 'FLX DEFECTIVE ITEM', 'FLX DEFECTIVE ITEM NO RETURN FEE', '["DEFECTIVE ITEM"]') THEN 'DQ'
                    WHEN UPPER(ord_line.rtn_reason) IN ('WRONG ITEM', 'I ORDERED THE WRONG ITEM', 'FLX I ORDERED THE WRONG ITEM', 'WRONG ITEM SHIPPED', 'FLX WRONG ITEM SHIPPED', '["WRONG ITEM SHIPPED"]') THEN 'WI'
                    WHEN UPPER(ord_line.rtn_reason) IN ('ITEM NOT AS DESCRIBE', 'ITEM NOT AS DESCRIBED/ PICTURED', 'FLX ITEM NOT AS DESCRIBED/ PICTURED', '["ITEM NOT AS DESCRIBED/ PICTURED"]') THEN 'WD'
                    WHEN UPPER(ord_line.rtn_reason) IN ('TOO BIG / LONG', 'TOO BIG LONG', 'TOO BIG/ LONG', 'TOO BIG/ LONG NO RETURN FEE', 'TOO LONG', 'ITEM DOES NOT FIT', 'FLX TOO BIG/ LONG', 'FLX TOO BIG/ LONG NO RETURN FEE') THEN 'TB'
                    WHEN UPPER(ord_line.rtn_reason) IN ('TOO SMALL / SHORT', 'TOO SMALL/ SHORT', 'TOO SMALL/SHORT', 'TOO SMALL/SHORT NO RETURN FEE', 'FLX TOO SMALL/ SHORT', 'FLX TOO SMALL/SHORT', 'FLX TOO SMALL/ SHORT NO RETURN FEE') THEN 'TS'
                    WHEN UPPER(ord_line.rtn_reason) IN ('TOO NARROW', 'TOO NARROW NO RETURN FEE', 'FLX TOO NARROW', '["TOO NARROW"]') THEN 'TN'
                    WHEN UPPER(ord_line.rtn_reason) IN ('TOO WIDE', 'TOO WIDE NO RETURN FEE', 'FLX TOO WIDE', 'FLX TOO WIDE NO RETURN FEE', '["TOO WIDE"]') THEN 'TW'
                    WHEN UPPER(ord_line.rtn_reason) IN ('UNWANTED ITEM', 'UNWANTED / CHANGED MY MIND', 'UNWANTED/ CHANGED MY MIND', 'UNWANTED/ CHANGED MY MIND NO RETURN FEE', 'FLX UNWANTED/ CHANGED MY MIND') THEN 'U'
                    WHEN UPPER(ord_line.rtn_reason) = 'LOST PACKAGE' THEN 'LP'
                    WHEN UPPER(ord_line.rtn_reason) = 'NOT DELIVERABLE' THEN 'ND'
                    WHEN UPPER(ord_line.rtn_reason) IN ('RETURN', 'RETURNREASON', 'INSTORE CS RETURNS', 'STORERETURN RETURNS', 'DC RETURNS') THEN 'IS'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '501-%' THEN '501'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '502-%' THEN '502'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '503-%' THEN '503'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '504-%' THEN '504'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '505-%' THEN '505'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '506-%' THEN '506'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '507-%' THEN '507'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '508-%' THEN '508'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '509-%' THEN '509'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '510-%' THEN '510'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '511-%' THEN '511'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '512-%' THEN '512'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '513-%' THEN '513'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '514-%' THEN '514'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '515-%' THEN '515'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '521-%' THEN '521'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '522-%' THEN '522'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '523-%' THEN '523'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '525-%' THEN '525'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '526-%' THEN '526'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '527-%' THEN '527'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '529-%' THEN '529'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '534-%' THEN '534'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '539-%' THEN '539'
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '999-%' THEN '999'
                END
        END AS lines_reasoncode,
        CAST(CASE
            WHEN TRIM(ord_line.org_id) IN ('CH-US', 'FL-US', 'KFL-US', 'FL-CA', 'CH-CA') THEN
                CASE
                    WHEN UPPER(ord_line.rtn_reason) = 'BOSS WRONG ITEM' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 390 WHEN 'FL-US' THEN 294 WHEN 'KFL-US' THEN 262 WHEN 'FL-CA' THEN 326 WHEN 'CH-CA' THEN 548 END
                    WHEN UPPER(ord_line.rtn_reason) = 'DROP SHIP WRONG ITEM' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 391 WHEN 'FL-US' THEN 295 WHEN 'KFL-US' THEN 263 WHEN 'FL-CA' THEN 327 WHEN 'CH-CA' THEN 549 END
                    WHEN UPPER(ord_line.rtn_reason) = 'EMBROIDERY QUALITY' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 393 WHEN 'FL-US' THEN 297 WHEN 'KFL-US' THEN 265 WHEN 'FL-CA' THEN 329 WHEN 'CH-CA' THEN 551 END
                    WHEN UPPER(ord_line.rtn_reason) = 'WRONG ITEM' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 419 WHEN 'FL-US' THEN 323 WHEN 'KFL-US' THEN 291 WHEN 'FL-CA' THEN 355 WHEN 'CH-CA' THEN 577 END
                    WHEN UPPER(ord_line.rtn_reason) = 'ITEM NOT AS DESCRIBE' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 418 WHEN 'FL-US' THEN 322 WHEN 'KFL-US' THEN 290 WHEN 'FL-CA' THEN 354 WHEN 'CH-CA' THEN 576 END
                    WHEN UPPER(ord_line.rtn_reason) = 'UNWANTED ITEM' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 415 WHEN 'FL-US' THEN 319 WHEN 'KFL-US' THEN 287 WHEN 'FL-CA' THEN 351 WHEN 'CH-CA' THEN 573 END
                    WHEN UPPER(ord_line.rtn_reason) = 'INSTORE CS RETURNS' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 395 WHEN 'FL-US' THEN 299 WHEN 'KFL-US' THEN 267 WHEN 'FL-CA' THEN 331 WHEN 'CH-CA' THEN 553 END
                    WHEN UPPER(ord_line.rtn_reason) = 'XSTORE RETURN' THEN CASE TRIM(ord_line.org_id) WHEN 'FL-US' THEN 762 END
                    WHEN UPPER(ord_line.rtn_reason) = 'WRONG ART GRAPHIC' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 400 WHEN 'FL-US' THEN 304 WHEN 'KFL-US' THEN 272 WHEN 'FL-CA' THEN 336 WHEN 'CH-CA' THEN 558 END
                    WHEN UPPER(ord_line.rtn_reason) = 'PERSONALIZED-BUYER' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 401 WHEN 'FL-US' THEN 305 WHEN 'KFL-US' THEN 273 WHEN 'FL-CA' THEN 337 WHEN 'CH-CA' THEN 559 END
                    WHEN UPPER(ord_line.rtn_reason) = 'PERSONALZD WRONG ITM' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 403 WHEN 'FL-US' THEN 307 WHEN 'KFL-US' THEN 275 WHEN 'FL-CA' THEN 339 WHEN 'CH-CA' THEN 561 END
                    WHEN UPPER(ord_line.rtn_reason) = 'PERSONALZD UNWANTED' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 404 WHEN 'FL-US' THEN 308 WHEN 'KFL-US' THEN 276 WHEN 'FL-CA' THEN 340 WHEN 'CH-CA' THEN 562 END
                    WHEN UPPER(ord_line.rtn_reason) = 'PRINTING QUALITY' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 405 WHEN 'FL-US' THEN 309 WHEN 'KFL-US' THEN 277 WHEN 'FL-CA' THEN 341 WHEN 'CH-CA' THEN 563 END
                    WHEN UPPER(ord_line.rtn_reason) = 'MIS-SPELLING' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 406 WHEN 'FL-US' THEN 310 WHEN 'KFL-US' THEN 278 WHEN 'FL-CA' THEN 342 WHEN 'CH-CA' THEN 564 END
                    WHEN UPPER(ord_line.rtn_reason) = 'PERSONALIZED-AGENT' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 407 WHEN 'FL-US' THEN 311 WHEN 'KFL-US' THEN 279 WHEN 'FL-CA' THEN 343 WHEN 'CH-CA' THEN 565 END
                    WHEN UPPER(ord_line.rtn_reason) = 'DIDN''T HOLD UP' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 408 WHEN 'FL-US' THEN 312 WHEN 'KFL-US' THEN 280 WHEN 'FL-CA' THEN 344 WHEN 'CH-CA' THEN 566 END
                    WHEN UPPER(ord_line.rtn_reason) = 'STORE DID NOT REFUND' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 409 WHEN 'FL-US' THEN 313 WHEN 'KFL-US' THEN 281 WHEN 'FL-CA' THEN 345 WHEN 'CH-CA' THEN 567 END
                    WHEN UPPER(ord_line.rtn_reason) = 'STORE DID REFUND' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 410 WHEN 'FL-US' THEN 314 WHEN 'KFL-US' THEN 282 WHEN 'FL-CA' THEN 346 WHEN 'CH-CA' THEN 568 END
                    WHEN UPPER(ord_line.rtn_reason) = 'USED CLEAT' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 416 WHEN 'FL-US' THEN 320 WHEN 'KFL-US' THEN 288 WHEN 'FL-CA' THEN 352 WHEN 'CH-CA' THEN 574 END
                    WHEN UPPER(ord_line.rtn_reason) = 'USED SHOE NOT DEFECT' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 417 WHEN 'FL-US' THEN 321 WHEN 'KFL-US' THEN 289 WHEN 'FL-CA' THEN 353 WHEN 'CH-CA' THEN 575 END
                    WHEN UPPER(ord_line.rtn_reason) = 'WRONG ITEM ORDERED' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 420 WHEN 'FL-US' THEN 324 WHEN 'KFL-US' THEN 292 WHEN 'FL-CA' THEN 356 WHEN 'CH-CA' THEN 578 END
                    WHEN UPPER(ord_line.rtn_reason) = 'DAMAGED IN TRANSIT (throw in trash)' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 421 WHEN 'FL-US' THEN 325 WHEN 'KFL-US' THEN 293 WHEN 'FL-CA' THEN 357 WHEN 'CH-CA' THEN 579 END
                    WHEN UPPER(ord_line.rtn_reason) = 'DO NOT USE' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 397 WHEN 'FL-US' THEN 301 WHEN 'KFL-US' THEN 269 WHEN 'FL-CA' THEN 333 WHEN 'CH-CA' THEN 555 END
                    WHEN UPPER(ord_line.rtn_reason) = 'COLOR MISMATCH' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 402 WHEN 'FL-US' THEN 306 WHEN 'KFL-US' THEN 274 WHEN 'FL-CA' THEN 338 WHEN 'CH-CA' THEN 560 END
                    WHEN UPPER(ord_line.rtn_reason) IN ('DAMAGED', 'DEFECTIVE ITEM', 'DEFECTIVE ITEM NO RETURN FEE', 'FLX DEFECTIVE ITEM', 'FLX DEFECTIVE ITEM NO RETURN FEE', '["DEFECTIVE ITEM"]') THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 392 WHEN 'FL-US' THEN 296 WHEN 'KFL-US' THEN 264 WHEN 'FL-CA' THEN 328 WHEN 'CH-CA' THEN 550 END
                    WHEN UPPER(ord_line.rtn_reason) IN ('I ORDERED THE WRONG ITEM', 'FLX I ORDERED THE WRONG ITEM', 'WRONG ITEM SHIPPED', 'FLX WRONG ITEM SHIPPED', '["WRONG ITEM SHIPPED"]') THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 419 WHEN 'FL-US' THEN 323 WHEN 'KFL-US' THEN 291 WHEN 'FL-CA' THEN 355 WHEN 'CH-CA' THEN 577 END
                    WHEN UPPER(ord_line.rtn_reason) IN ('ITEM NOT AS DESCRIBED/ PICTURED', 'FLX ITEM NOT AS DESCRIBED/ PICTURED', '["ITEM NOT AS DESCRIBED/ PICTURED"]') THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 418 WHEN 'FL-US' THEN 322 WHEN 'KFL-US' THEN 290 WHEN 'FL-CA' THEN 354 WHEN 'CH-CA' THEN 576 END
                    WHEN UPPER(ord_line.rtn_reason) IN ('TOO BIG / LONG', 'TOO BIG LONG', 'TOO BIG/ LONG', 'TOO BIG/ LONG NO RETURN FEE', 'TOO LONG', 'ITEM DOES NOT FIT', 'FLX TOO BIG/ LONG', 'FLX TOO BIG/ LONG NO RETURN FEE') THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 411 WHEN 'FL-US' THEN 315 WHEN 'KFL-US' THEN 283 WHEN 'FL-CA' THEN 347 WHEN 'CH-CA' THEN 569 END
                    WHEN UPPER(ord_line.rtn_reason) IN ('TOO SMALL / SHORT', 'TOO SMALL/ SHORT', 'TOO SMALL/SHORT', 'TOO SMALL/SHORT NO RETURN FEE', 'FLX TOO SMALL/ SHORT', 'FLX TOO SMALL/SHORT', 'FLX TOO SMALL/ SHORT NO RETURN FEE') THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 413 WHEN 'FL-US' THEN 317 WHEN 'KFL-US' THEN 285 WHEN 'FL-CA' THEN 349 WHEN 'CH-CA' THEN 571 END
                    WHEN UPPER(ord_line.rtn_reason) IN ('TOO NARROW', 'TOO NARROW NO RETURN FEE', 'FLX TOO NARROW', '["TOO NARROW"]') THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 412 WHEN 'FL-US' THEN 316 WHEN 'KFL-US' THEN 284 WHEN 'FL-CA' THEN 348 WHEN 'CH-CA' THEN 570 END
                    WHEN UPPER(ord_line.rtn_reason) IN ('TOO WIDE', 'TOO WIDE NO RETURN FEE', 'FLX TOO WIDE', 'FLX TOO WIDE NO RETURN FEE', '["TOO WIDE"]') THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 414 WHEN 'FL-US' THEN 318 WHEN 'KFL-US' THEN 286 WHEN 'FL-CA' THEN 350 WHEN 'CH-CA' THEN 572 END
                    WHEN UPPER(ord_line.rtn_reason) IN ('UNWANTED / CHANGED MY MIND', 'UNWANTED/ CHANGED MY MIND', 'UNWANTED/ CHANGED MY MIND NO RETURN FEE', 'FLX UNWANTED/ CHANGED MY MIND') THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 415 WHEN 'FL-US' THEN 319 WHEN 'KFL-US' THEN 287 WHEN 'FL-CA' THEN 351 WHEN 'CH-CA' THEN 573 END
                    WHEN UPPER(ord_line.rtn_reason) = 'LOST PACKAGE' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 398 WHEN 'FL-US' THEN 302 WHEN 'KFL-US' THEN 270 WHEN 'FL-CA' THEN 334 WHEN 'CH-CA' THEN 556 END
                    WHEN UPPER(ord_line.rtn_reason) = 'NOT DELIVERABLE' THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 399 WHEN 'FL-US' THEN 303 WHEN 'KFL-US' THEN 271 WHEN 'FL-CA' THEN 335 WHEN 'CH-CA' THEN 557 END
                    WHEN UPPER(ord_line.rtn_reason) IN ('RETURN', 'RETURNREASON', 'STORERETURN RETURNS', 'DC RETURNS') THEN CASE TRIM(ord_line.org_id) WHEN 'CH-US' THEN 395 WHEN 'FL-US' THEN 299 WHEN 'KFL-US' THEN 267 WHEN 'FL-CA' THEN 331 WHEN 'CH-CA' THEN 553 END
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '501-%' THEN 763
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '502-%' THEN 764
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '503-%' THEN 765
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '504-%' THEN 766
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '505-%' THEN 767
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '506-%' THEN 768
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '507-%' THEN 769
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '508-%' THEN 770
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '509-%' THEN 771
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '510-%' THEN 772
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '511-%' THEN 773
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '512-%' THEN 774
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '513-%' THEN 775
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '514-%' THEN 776
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '515-%' THEN 777
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '521-%' THEN 778
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '522-%' THEN 779
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '523-%' THEN 780
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '525-%' THEN 781
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '526-%' THEN 782
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '527-%' THEN 783
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '529-%' THEN 784
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '534-%' THEN 785
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '539-%' THEN 786
                    WHEN TRIM(ord_line.org_id) = 'FL-US' AND UPPER(ord_line.rtn_reason) LIKE '999-%' THEN 787
                END
        END AS VARCHAR) AS lines_reasoncodeid,
        CAST(ord_hdr.ccy_cd AS VARCHAR) AS currencyiso,
        CAST(NULL AS VARCHAR) AS req_lines_priceoverridereason,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0)) AS VARCHAR) AS req_lines_discountamount,
        CAST((ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0))) -
			ABS(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0)) AS VARCHAR) AS req_lines_discountedtotalamount,
        CAST(ABS(COALESCE(ord_line.fv_orig_unit_price, ord_line.fv_unit_price, 0)) AS VARCHAR) AS req_lines_originalretailprice,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) AS VARCHAR) AS req_lines_shippingamount,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) AS VARCHAR) AS req_lines_shippingtaxamount,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) AS VARCHAR) AS req_lines_subtotalamount,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS VARCHAR) AS req_lines_taxamount,
        CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS VARCHAR) AS req_lines_totalamount,
        CAST(NULL AS VARCHAR) AS req_lines_giftboxamount,
        CAST(NULL AS VARCHAR) AS req_lines_giftboxtaxamount,
        CAST(ABS(COALESCE(ord_line.fv_unit_price, 0)) AS VARCHAR) AS req_lines_unitprice,
        CAST(CASE
            WHEN TRIM(ord_line.org_id) = 'FL-US' AND REGEXP_LIKE(UPPER(TRIM(REPLACE(REPLACE(rtn_reason, '["', ''), '"]', ''))), '^[0-9]{3}-') THEN 'false'
            WHEN UPPER(TRIM(rtn_reason)) IS NOT NULL THEN 'true'
            ELSE 'true'
        END AS VARCHAR) AS restockable,
        CAST(ord_hdr.inv_id AS VARCHAR) AS taxinvoicenum,
        TRY_PARSE_JSON(COALESCE(ord_hdr.credits_info, '[]')) AS credits,
        TRY_PARSE_JSON(COALESCE(ord_line.rtn_fee_info, '[]')) AS adjustments,
        pymt_line.paymentsinfo AS paymentsinfo,
        ord_line.updated_ts AS postedat,
        CAST(ord_line.updated_by AS VARCHAR) AS postedby,
        ord_line.updated_ts AS load_time_kafka,
        ord_line.etl_updt_ts AS load_time_adls,
        TO_DATE(ord_hdr.created_ts) AS order_date,
        ord_line.updated_ts AS updated_datetime,
        CAST(CASE
            WHEN UPPER(loc.loc_type_id) = 'DC' THEN NULL
            ELSE dim_loc.loc_num
        END AS VARCHAR) AS returningstore,
        CAST(CASE 
	        when lower(loc.loc_type_id) = 'store' and ord_line.ord_id like 'RET%' THEN trim(replace(ord_line.ord_id, 'RET', ''))
        END AS VARCHAR) AS xstoretransactionnumber,
        CAST(CASE WHEN ord_line.is_loyalty_disc = 1 THEN 'true' END AS VARCHAR) AS loyaltydiscount,
        COALESCE(ord_line.ord_coupons, '[]') AS req_tot_discounts,
        COALESCE(ord_line.ord_coupons, '[]') AS return_act_discounts,
        CAST(ord_line.ord_ln_id AS VARCHAR) AS lines_linenumber,
        'MAO' AS ref_source
    FROM fct_mao_ord_line ord_line
    JOIN base_ord_hdr ord_hdr
        ON ord_line.org_id = ord_hdr.org_id AND ord_line.ord_id = ord_hdr.ord_id
    LEFT JOIN payment_grouped pymt_line
        ON ord_line.org_id = pymt_line.org_id and ord_line.prnt_ord_id = pymt_line.ord_id and ord_line.prnt_ord_ln_id = pymt_line.ord_ln_id
    LEFT JOIN product_master pm
        ON ord_line.is_gift_card != 1
        AND TRIM(ord_line.item_id) = TRIM(pm.global_size_id)
        AND (CASE WHEN ord_hdr.org_id IN ('FL-CA', 'CH-CA') THEN '98' ELSE '81' END) = pm.banner_id
    LEFT JOIN product_master_div pm_div
        ON ord_line.is_gift_card != 1
        AND TRIM(ord_line.item_id) = TRIM(pm_div.global_size_id)
        AND ord_line.org_id = pm_div.org_desc
    LEFT JOIN {{ source('dom_gold', 'dim_mao_loc_v') }} loc
        ON COALESCE(ord_line.ship_to_loc_id, ord_line.physical_org_id) = loc.loc_id
    LEFT JOIN {{ source('dom_gold', 'dim_mao_item_v') }} itm
        ON TRIM(ord_line.item_id) = TRIM(itm.item_id)
    LEFT JOIN dim_location dim_loc
        ON LPAD(dim_loc.loc_snum, 5, '0') = LPAD(COALESCE(ord_line.ship_to_loc_id, ord_line.physical_org_id), 5, '0')
    LEFT JOIN fct_exchange_orders ord_exch
        ON ord_line.org_id = ord_exch.org_id AND ord_line.ord_id = ord_exch.prnt_ord_id AND ord_line.ord_ln_id = ord_exch.prnt_ord_ln_id
    LEFT JOIN fct_original_orders orig
        ON ord_line.org_id = orig.org_id AND ord_line.prnt_ord_id = orig.ord_id AND ord_line.prnt_ord_ln_id = orig.ord_ln_id
)
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(order_datetime AS TIMESTAMP) AS order_datetime,
    CAST(return_id AS VARCHAR(300)) AS return_id,
    CAST(company_number AS DECIMAL(38, 0)) AS company_number,
    CAST(return_status AS VARCHAR) AS return_status,
    CAST(return_number AS VARCHAR) AS return_number,
    CAST(refund_method AS VARCHAR) AS refund_method,
    CAST(return_location AS VARCHAR) AS return_location,
    CAST(exchangeordernumber AS VARCHAR) AS exchangeordernumber,
    CAST(exchangenumber AS VARCHAR) AS exchangenumber,
    CAST(return_date AS DATE) AS return_date,
    CAST(return_agent AS VARCHAR) AS return_agent,
    CAST(return_taxtransid AS VARCHAR) AS return_taxtransid,
    CAST(return_act_adjustmentamount AS DECIMAL(38, 2)) AS return_act_adjustmentamount,
    CAST(return_act_creditsamount AS DECIMAL(38, 2)) AS return_act_creditsamount,
    CAST(return_act_discountamount AS DECIMAL(38, 2)) AS return_act_discountamount,
    CAST(return_act_discountedtotalamount AS DECIMAL(38, 2)) AS return_act_discountedtotalamount,
    CAST(return_act_refundtotalamount AS DECIMAL(38, 2)) AS return_act_refundtotalamount,
    CAST(return_act_totalreturnamount AS DECIMAL(38, 2)) AS return_act_totalreturnamount,
    CAST(return_act_exchangecreditamount AS DECIMAL(38, 2)) AS return_act_exchangecreditamount,
    CAST(return_act_creditcardrefundamount AS DECIMAL(38, 2)) AS return_act_creditcardrefundamount,
    CAST(return_act_paypalrefundamount AS DECIMAL(38, 2)) AS return_act_paypalrefundamount,
    CAST(return_act_giftcardrefundamount AS DECIMAL(38, 2)) AS return_act_giftcardrefundamount,
    CAST(return_act_shippingamount AS DECIMAL(38, 2)) AS return_act_shippingamount,
    CAST(return_act_shippingtaxamount AS DECIMAL(38, 2)) AS return_act_shippingtaxamount,
    CAST(return_act_subtotalamount AS DECIMAL(38, 2)) AS return_act_subtotalamount,
    CAST(return_act_taxamount AS DECIMAL(38, 2)) AS return_act_taxamount,
    CAST(req_tot_adjustmentamount AS DECIMAL(38, 2)) AS req_tot_adjustmentamount,
    CAST(req_tot_creditsamount AS DECIMAL(38, 2)) AS req_tot_creditsamount,
    CAST(req_tot_discountamount AS DECIMAL(38, 2)) AS req_tot_discountamount,
    CAST(req_tot_discountedtotalamount AS DECIMAL(38, 2)) AS req_tot_discountedtotalamount,
    CAST(req_tot_refundtotalamount AS DECIMAL(38, 2)) AS req_tot_refundtotalamount,
    CAST(req_tot_totalreturnamount AS DECIMAL(38, 2)) AS req_tot_totalreturnamount,
    CAST(req_tot_exchangecreditamount AS DECIMAL(38, 2)) AS req_tot_exchangecreditamount,
    CAST(req_tot_creditcardrefundamount AS DECIMAL(38, 2)) AS req_tot_creditcardrefundamount,
    CAST(req_tot_paypalrefundamount AS DECIMAL(38, 2)) AS req_tot_paypalrefundamount,
    CAST(req_tot_giftcardrefundamount AS DECIMAL(38, 2)) AS req_tot_giftcardrefundamount,
    CAST(req_tot_shippingamount AS DECIMAL(38, 2)) AS req_tot_shippingamount,
    CAST(req_tot_shippingtaxamount AS DECIMAL(38, 2)) AS req_tot_shippingtaxamount,
    CAST(req_tot_subtotalamount AS DECIMAL(38, 2)) AS req_tot_subtotalamount,
    CAST(req_tot_taxamount AS DECIMAL(38, 2)) AS req_tot_taxamount,
    CAST(lines_refundnum AS VARCHAR) AS lines_refundnum,
    CAST(lines_act_priceoverridereason AS VARCHAR(50000)) AS lines_act_priceoverridereason,
    CAST(lines_act_discountamount AS DECIMAL(38, 2)) AS lines_act_discountamount,
    CAST(lines_act_discountedtotalamount AS DECIMAL(38, 2)) AS lines_act_discountedtotalamount,
    CAST(lines_act_originalretailprice AS DECIMAL(38, 2)) AS lines_act_originalretailprice,
    CAST(lines_act_shippingamount AS DECIMAL(38, 2)) AS lines_act_shippingamount,
    CAST(lines_act_shippingtaxamount AS DECIMAL(38, 2)) AS lines_act_shippingtaxamount,
    CAST(lines_act_giftboxamount AS DECIMAL(38, 2)) AS lines_act_giftboxamount,
    CAST(lines_act_giftboxtaxamount AS DECIMAL(38, 2)) AS lines_act_giftboxtaxamount,
    CAST(lines_act_subtotalamount AS DECIMAL(38, 2)) AS lines_act_subtotalamount,
    CAST(lines_act_taxamount AS DECIMAL(38, 2)) AS lines_act_taxamount,
    CAST(lines_act_totalamount AS DECIMAL(38, 2)) AS lines_act_totalamount,
    CAST(lines_act_unitprice AS DECIMAL(38, 2)) AS lines_act_unitprice,
    CAST(product_backorderflag AS VARCHAR) AS product_backorderflag,
    CAST(product_brand AS VARCHAR) AS product_brand,
    CAST(product_category AS VARCHAR) AS product_category,
    CAST(product_color AS VARCHAR) AS product_color,
    CAST(product_description AS VARCHAR) AS product_description,
    CAST(product_image AS VARCHAR) AS product_image,
    CAST(product_iscollectupfront AS VARCHAR) AS product_iscollectupfront,
    CAST(product_launchskuflag AS VARCHAR) AS product_launchskuflag,
    CAST(product_name AS VARCHAR) AS product_name,
    CAST(productdesignator AS VARCHAR) AS productdesignator,
    CAST(product_number AS VARCHAR) AS product_number,
    CAST(product_type AS VARCHAR) AS product_type,
    CAST(product_size AS VARCHAR) AS product_size,
    CAST(product_sku AS VARCHAR) AS product_sku,
    CAST(product_taxcode AS VARCHAR) AS product_taxcode,
    CAST(lines_qty AS DECIMAL(38, 2)) AS lines_qty,
    CAST(lines_reasoncode AS VARCHAR) AS lines_reasoncode,
    CAST(lines_reasoncodeid AS VARCHAR) AS lines_reasoncodeid,
    CAST(currencyiso AS VARCHAR) AS currencyiso,
    CAST(req_lines_priceoverridereason AS VARCHAR) AS req_lines_priceoverridereason,
    CAST(req_lines_discountamount AS DECIMAL(38, 2)) AS req_lines_discountamount,
    CAST(req_lines_discountedtotalamount AS DECIMAL(38, 2)) AS req_lines_discountedtotalamount,
    CAST(req_lines_originalretailprice AS DECIMAL(38, 2)) AS req_lines_originalretailprice,
    CAST(req_lines_shippingamount AS DECIMAL(38, 2)) AS req_lines_shippingamount,
    CAST(req_lines_shippingtaxamount AS DECIMAL(38, 2)) AS req_lines_shippingtaxamount,
    CAST(req_lines_subtotalamount AS DECIMAL(38, 2)) AS req_lines_subtotalamount,
    CAST(req_lines_taxamount AS DECIMAL(38, 2)) AS req_lines_taxamount,
    CAST(req_lines_totalamount AS DECIMAL(38, 2)) AS req_lines_totalamount,
    CAST(req_lines_giftboxamount AS DECIMAL(38, 2)) AS req_lines_giftboxamount,
    CAST(req_lines_giftboxtaxamount AS DECIMAL(38, 2)) AS req_lines_giftboxtaxamount,
    CAST(req_lines_unitprice AS DECIMAL(38, 2)) AS req_lines_unitprice,
    CAST(restockable AS VARCHAR) AS restockable,
    CAST(taxinvoicenum AS VARCHAR) AS taxinvoicenum,
    credits AS credits,
    adjustments AS adjustments,
    paymentsinfo AS paymentsinfo,
    CAST(postedat AS TIMESTAMP) AS postedat,
    CAST(postedby AS VARCHAR) AS postedby,
    CAST(load_time_kafka AS TIMESTAMP) AS load_time_kafka,
    CAST(load_time_adls AS TIMESTAMP) AS load_time_adls,
    CAST(order_date AS DATE) AS order_date,
    CAST(returningstore AS VARCHAR) AS returningstore,
    CAST(xstoretransactionnumber AS VARCHAR) AS xstoretransactionnumber,
    CAST(loyaltydiscount AS VARCHAR) AS loyaltydiscount,
    CAST(req_tot_discounts AS VARCHAR) AS req_tot_discounts,
    CAST(return_act_discounts AS VARCHAR) AS return_act_discounts,
    CAST(lines_linenumber AS VARCHAR) AS lines_linenumber,
    CAST(ref_source AS VARCHAR(50)) AS ref_source
FROM returns_landing
WHERE product_number IS NOT NULL