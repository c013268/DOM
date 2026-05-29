{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}

{{ config(
    materialized="incremental",
    unique_key=["companynumber", "order_id", "refundid", "linenumber", "refund_status"],
    post_hook=["{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'load_time_adls' ) }}"],
    meta={'strategy': "merge"}
) }}


WITH base_ord_hdr AS (
    SELECT *,
        MAX(updated_ts) OVER(PARTITION BY org_id,ord_id) AS max_updated_ts,
		MAX(ord_total) OVER(PARTITION BY org_id,ord_id) AS max_ord_total,
        MIN(ord_total) OVER(PARTITION BY org_id,ord_id) AS min_ord_total
    FROM {{ source('dom_gold', 'fct_mao_ord_hdr_v') }}
    WHERE doc_type_id = 'CustomerOrder'
),
base_ord_line_hist AS (
    SELECT *
    FROM {{ source('dom_gold', 'fct_mao_ord_line_hist_v') }}
    WHERE max_fulflmnt_status_id IS NOT NULL
        AND max_fulflmnt_status_id NOT IN (8000, 8500)
        {% if is_incremental() %}
            AND etl_updt_ts >= {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL_DBIMART") | as_text }}'
        {% endif %}
),
base_ord_line AS (
    SELECT *
    FROM (
        SELECT
            *,
            MAX(ord_ln_total) OVER (PARTITION BY org_id, ord_id, ord_ln_id) AS max_ln_total,
            MIN(ord_ln_total) OVER (PARTITION BY org_id, ord_id, ord_ln_id) AS min_ln_total,
            MIN(qty) OVER (PARTITION BY org_id, ord_id, ord_ln_id) AS min_ln_qty,
            MAX(orig_ord_qty) OVER (PARTITION BY org_id, ord_id, ord_ln_id) AS max_ln_orig_qty
       FROM base_ord_line_hist
    )
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
auth_reversal AS (
    SELECT ord_id, org_id
    FROM {{ source('dom_gold', 'fct_mao_ord_pymt_line_v') }}
    WHERE pymt_txn_type = 'Authorization Reversal'
),
legit_refund_payment AS (
    SELECT ord_id, org_id
    FROM {{ source('dom_gold', 'fct_mao_ord_pymt_line_v') }}
    WHERE PYMT_TXN_DTL_ID IS NOT NULL
        AND pymt_txn_type IN ('Refund', 'Return Credit')
),
refund_hist_details AS (
    SELECT
        fol.org_id,
        fol.ord_id,
        fol.ord_ln_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'refundstatus', CAST(CASE
            WHEN foh.pymt_status_id = '6000' THEN 'REFUND_PROCESSING'
            WHEN foh.pymt_status_id = '7000' THEN 'REFUNDED'
            WHEN foh.pymt_status_id = '5000' AND (fol.max_ln_total != fol.min_ln_total AND foh.max_ord_total != 0) THEN 'REFUNDED'
            ELSE UPPER(foh.pymt_status_desc)
        END AS VARCHAR),
                'timestamp', CAST(TO_VARCHAR(
                    TRY_TO_TIMESTAMP(CAST(fol.created_ts AS VARCHAR), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                    'YYYY-MM-DD"T"HH24:MI:SS.FF3"000000Z"'
                ) AS VARCHAR)
            )
        ) AS refund_status_history,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'amount', CAST(ABS(COALESCE(fol.ord_ln_total, 0)) AS VARCHAR),
                'type', CAST(UPPER(COALESCE(foh.refund_pymt_method, '')) AS VARCHAR)
            )
        ) AS refundmethods
    FROM (SELECT *,
               MAX(ord_ln_total) OVER(PARTITION BY org_id,ord_id,ord_ln_id) AS max_ln_total,
               MIN(ord_ln_total) OVER(PARTITION BY org_id,ord_id,ord_ln_id) AS min_ln_total
        FROM base_ord_line_hist
    ) fol
    JOIN base_ord_hdr foh
        ON NVL(fol.prnt_ord_id, fol.ord_id) = foh.ord_id AND fol.org_id = foh.org_id
    WHERE (foh.pymt_status_id in (6000,7000) or (foh.pymt_status_id =5000 and fol.max_ln_total != fol.min_ln_total AND foh.max_ord_total != 0))
    GROUP BY all
),
fct_mao_ord_line_fv AS (
    SELECT
        org_id,
        ord_id,
        ord_ln_id,
        FIRST_VALUE(orig_unit_price) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_orig_unit_price,
        FIRST_VALUE(unit_price) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_unit_price,
        FIRST_VALUE(cnlled_total_disc) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_cnlled_total_disc,
        FIRST_VALUE(total_disc) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_total_disc,
        FIRST_VALUE(cnlled_ord_ln_total) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_cnlled_ord_ln_total,
        FIRST_VALUE(ord_ln_total) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_ord_ln_total,
        FIRST_VALUE(cnlled_total_charges) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_cnlled_total_charges,
        FIRST_VALUE(total_charges) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_total_charges,
        FIRST_VALUE(cnlled_total_taxes) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_cnlled_total_taxes,
        FIRST_VALUE(total_taxes) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_total_taxes,
        FIRST_VALUE(case when is_rtn=1 then cnlled_ord_shipping_amt else cnlled_orig_ord_shipping_amt end) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_cnlled_ord_shipping_amt,
        FIRST_VALUE(case when is_rtn=1 then ord_shipping_amt else orig_ord_shipping_amt end) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_ord_shipping_amt,
        FIRST_VALUE(case when is_rtn=1 then cnlled_ord_shipping_tax_amt else cnlled_orig_ord_shipping_tax_amt end) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_cnlled_ord_shipping_tax_amt,
        FIRST_VALUE(case when is_rtn=1 then ord_shipping_tax_amt else orig_ord_shipping_tax_amt end) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_ord_shipping_tax_amt,
        FIRST_VALUE(cnlled_ord_ln_sub_total) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_cnlled_ord_ln_sub_total,
        FIRST_VALUE(ord_ln_sub_total) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_ord_ln_sub_total,
        FIRST_VALUE(case when is_rtn=1 then cnlled_ord_sales_tax_amt else cnlled_orig_ord_sales_tax_amt end) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_cnlled_ord_sales_tax_amt,
        FIRST_VALUE(case when is_rtn=1 then ord_sales_tax_amt else orig_ord_sales_tax_amt end) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_ord_sales_tax_amt,
        FIRST_VALUE(gift_card_value) OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_gift_card_value,
        ROW_NUMBER() OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY max_fulflmnt_status_id ASC, updated_ts ASC) AS fv_rnk
    FROM base_ord_line_hist
),
fct_mao_ord_tax_agg as (
	select org_id ol_org_id,ord_id as ol_ord_id,ord_ln_id as ol_ord_ln_id,
		ABS(SUM(COALESCE(fv.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(fv.fv_ord_shipping_tax_amt, 0))) AS shippingtaxamount,
		ABS(SUM(COALESCE(fv.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(fv.fv_ord_sales_tax_amt, 0))) AS taxamount
	from 
		fct_mao_ord_line_fv fv
    where 
        fv_rnk = 1
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
        WHERE lower(pymt_txn_type) = 'refund'
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
                'paymentTransactionId', CAST(PYMT_TXN_ID AS VARCHAR),
                'paymentTransactionSubType', CAST(NULL AS VARCHAR),
                'paymentTransactionType', CAST(PYMT_TXN_TYPE AS VARCHAR),
                'paymentType', CAST(CASE WHEN pymt_type = 'Gift Card' THEN 'GIFTCARD' WHEN pymt_type = 'Credit Card' THEN 'CREDITCARD' ELSE UPPER(pymt_type) END AS STRING),
                'creditCardType', CAST(PYMT_CARD_TYPE AS VARCHAR),
                'date', CAST(created_ts AS VARCHAR),
                'shippingTaxAmount', CAST(NULL AS VARCHAR),
                'taxAmount', CAST(NULL AS VARCHAR),
                'authorization', OBJECT_CONSTRUCT(
                    'attributes', OBJECT_CONSTRUCT(
                        'authResponse', CAST(pymt_txn_status_desc AS VARCHAR),
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
                            'authResponse', CAST(pymt_txn_status_desc AS VARCHAR),
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
                            'authResponse', CAST(pymt_txn_status_desc AS VARCHAR),
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
refunds AS (
    SELECT
        CAST(NVL(fol.prnt_ord_id, fol.ord_id) AS VARCHAR) AS order_id,
        CAST(foh.created_ts AS VARCHAR) AS order_datetime,
        fol.ord_id AS refundid,
        CAST(CASE TRIM(foh.org_id)
            WHEN 'FL-US' THEN '21'
            WHEN 'FL-CA' THEN '45'
            WHEN 'KFL-US' THEN '22'
            WHEN 'CH-CA' THEN '77'
            WHEN 'CH-US' THEN '20'
        END AS VARCHAR) AS companynumber,
        CAST(DATE(fol.updated_ts) AS VARCHAR) AS refund_header_date,
        CAST(CASE
            WHEN foh.pymt_status_id = '6000' THEN 'REFUND_PROCESSING'
            WHEN foh.pymt_status_id = '7000' THEN 'REFUNDED'
            WHEN foh.pymt_status_id = '5000' AND (fol.max_ln_total != fol.min_ln_total AND foh.min_ord_total != 0) THEN 'REFUNDED'
            ELSE UPPER(foh.pymt_status_desc)
        END AS VARCHAR) AS refund_status,
        fol.created_by AS refund_createdby,
        foh.cust_id AS refund_userid,
        CASE
            WHEN LOWER(foh.ord_type_id) IN ('web', 'return') THEN 'OMS_OBF_SVC'
            WHEN LOWER(foh.ord_type_id) = 'callcenter' THEN 'CUSTOMER_SVC'
            WHEN LOWER(foh.ord_type_id) = 'savethesale' THEN 'XSTORE'
        END AS source,
        fol.ord_id AS refundnum,
        CAST(CASE WHEN fol.max_fulflmnt_status_id IN ('9000', '19000') THEN fol.orig_ord_qty ELSE fol.qty END AS VARCHAR) AS quantity,
        CAST(fol.itm_desc AS VARCHAR) AS name,
        CAST(CASE
            WHEN fol.is_gift_card = 1 THEN fol.item_id
            WHEN foh.channel != 'XSTORE' AND foh.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.online_us_sku
            WHEN foh.channel != 'XSTORE' AND foh.org_id IN ('FL-CA', 'CH-CA') THEN pm.online_ca_sku
            WHEN foh.channel = 'XSTORE' THEN pm_div.legacy_sku_size
            ELSE itm.color
        END AS VARCHAR) AS sku,
        CAST(CASE WHEN fol.is_gift_card = 1 THEN fol.itm_size ELSE pm.legacy_size_desc END AS VARCHAR) AS size,
        CAST(CASE WHEN fol.is_gift_card = 1 THEN fol.itm_color_desc ELSE pm.desc_long_2 END AS VARCHAR) AS color,
        CAST(fol.small_image_u_r_i AS VARCHAR) AS image,
        CAST(UPPER(CASE WHEN fol.is_gift_card = 1 THEN fol.itm_brand ELSE pm.global_brand_desc END) AS VARCHAR) AS brand,
        CAST(CASE WHEN fol.is_gift_card = 1 THEN fol.itm_dept_name ELSE pm.fob_desc END AS VARCHAR) AS category,
        CAST(pm."DESC" AS VARCHAR) AS description,
        CASE WHEN foh.is_prepaid = 0 THEN 'false' END AS iscollectupfront,
        CASE WHEN fol.is_backorderflg = 1 THEN 'true' WHEN fol.is_backorderflg = 0 THEN 'false' END AS backorderflag,
        CASE WHEN fol.is_launch_sku_flg = 1 THEN 'true' WHEN fol.is_launch_sku_flg = 0 THEN 'false' END AS launchskuflag,
        CAST(CASE WHEN fol.is_gift_card = 1 THEN fol.itm_tax_cd ELSE pm.tax_code END AS VARCHAR) AS taxcode,
        CAST(CASE WHEN fol.is_gift_card = 1 THEN 'GFT' ELSE pm.designator_id END AS VARCHAR) AS productdesignator,
        CAST(TRIM(CASE
            WHEN fol.item_id = 'ECARD20' THEN '2138264'
            WHEN fol.item_id = 'ECARD21' THEN '2138265'
            WHEN fol.item_id = 'ECARD22' THEN '2138266'
            WHEN fol.item_id = 'ECARD45' THEN '20'
            WHEN fol.item_id = 'ECARD77' THEN '2000003'
            WHEN fol.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
            WHEN fol.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.internal_product_number
            ELSE fol.item_id
        END) AS VARCHAR) AS productnumber,
        CAST(fol.product_type AS VARCHAR) AS producttype,
        CAST(foh.inv_id AS VARCHAR) AS cegrrefid,
        CAST(ABS(COALESCE(fv.fv_orig_unit_price, fv.fv_unit_price, 0)) AS VARCHAR) AS originalretailprice,
        CAST(ABS(COALESCE(fv.fv_unit_price, 0)) AS VARCHAR) AS originalunitprice,
        CAST(ABS(COALESCE(fv.fv_cnlled_total_disc, 0) + COALESCE(fv.fv_total_disc, 0)) AS VARCHAR) AS originalunitdiscountamount,
        CAST(ABS(COALESCE(fv.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(fv.fv_ord_ln_sub_total, 0)) AS VARCHAR) AS linerefundsubtotal,
        CAST(ABS(COALESCE(fv.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(fv.fv_ord_sales_tax_amt, 0)) AS VARCHAR) AS taxamount,
        CAST(ABS(COALESCE(fv.fv_cnlled_ord_shipping_amt, 0) + COALESCE(fv.fv_ord_shipping_amt, 0)) AS VARCHAR) AS shippingamount,
        CAST(ABS(COALESCE(fv.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(fv.fv_ord_shipping_tax_amt, 0)) AS VARCHAR) AS shippingtaxamount,
        CAST(0 AS VARCHAR) AS inboundshippingamount,
        CAST(
            CASE
                WHEN foh.pymt_status_id = 5000
                    AND fol.is_cnlled=0
                    AND (fol.max_ln_total != fol.min_ln_total AND foh.min_ord_total != 0)
                    THEN ABS(COALESCE(fol.max_ln_total, 0)) - ABS(COALESCE(fol.min_ln_total, 0))
                WHEN fol.is_cnlled = 1
                    AND fol.is_rtn = 0
                    AND foh.pymt_status_id IN (6000, 7000)
                    AND fol.max_fulflmnt_status_id <= 9000
                    THEN ABS(COALESCE(fol.max_ln_total, 0)) - ABS(COALESCE(fol.min_ln_total, 0))
                ELSE
                    ABS(COALESCE(fv.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(fv.fv_ord_ln_sub_total, 0))
                    + ABS(COALESCE(fv.fv_cnlled_ord_shipping_amt, 0) + COALESCE(fv.fv_ord_shipping_amt, 0))
                    + ABS(COALESCE(fv.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(fv.fv_ord_shipping_tax_amt, 0))
                    + ABS(COALESCE(fv.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(fv.fv_ord_sales_tax_amt, 0))
            END AS VARCHAR
        ) AS totalamount,
       CAST(FALSE AS VARCHAR) AS returnexpected,
        CAST(CASE fol.is_rtn WHEN 1 THEN 'true' END AS VARCHAR) AS returned,
        CASE WHEN foh.ord_type_id = 'Return' THEN
            ARRAY_CONSTRUCT(
                OBJECT_CONSTRUCT(
                    'returnNumber', CAST(fol.ord_id AS VARCHAR),
                    'timestamp', CAST(TO_VARCHAR(
                        TRY_TO_TIMESTAMP(CAST(fol.created_ts AS VARCHAR), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                        'YYYY-MM-DD"T"HH24:MI:SS.FF3"000000Z"'
                    ) AS VARCHAR)
                )
            )
        END AS returnnumbers,
        CAST(CASE fol.is_rtn WHEN 1 THEN ABS(COALESCE(fol.qty, 0)) ELSE 0 END AS VARCHAR) AS quantityreturned,
        COALESCE(foh.confirmed_ts, foh.captured_ts) AS returndate,
        CAST(
            CASE
                WHEN fol.rtn_reason IS NOT NULL THEN fol.rtn_reason
                WHEN fol.appeasment_reason_cd IS NOT NULL THEN fol.appeasment_reason_cd
                WHEN fol.cnl_reason_desc IS NOT NULL THEN fol.cnl_reason_desc
            END AS VARCHAR
        ) AS reasoncode,
        CAST(0 AS VARCHAR) AS ih_inboundshippingamount,
        CAST(ABS(SUM(COALESCE(fv.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(fv.fv_ord_ln_sub_total, 0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS VARCHAR) AS ih_linerefundsubtotal,
        CAST(ABS(SUM(COALESCE(fv.fv_orig_unit_price, fv.fv_unit_price, 0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS VARCHAR) AS ih_originalretailprice,
        CAST(ABS(SUM(COALESCE(fv.fv_cnlled_total_disc, 0) + COALESCE(fv.fv_total_disc, 0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS VARCHAR) AS ih_originalunitdiscountamount,
        CAST(ABS(SUM(COALESCE(fv.fv_unit_price, 0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS VARCHAR) AS ih_originalunitprice,
        CAST(ABS(SUM(COALESCE(fv.fv_cnlled_ord_shipping_amt, 0) + COALESCE(fv.fv_ord_shipping_amt, 0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS VARCHAR) AS ih_shippingamount,
        CAST(ABS(SUM(COALESCE(fv.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(fv.fv_ord_shipping_tax_amt, 0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS VARCHAR) AS ih_shippingtaxamount,
        CAST(ABS(SUM(COALESCE(fv.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(fv.fv_ord_sales_tax_amt, 0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS VARCHAR) AS ih_taxamount,
         CAST(ABS(SUM(
            CASE
                WHEN foh.pymt_status_id = 5000
                    AND fol.is_cnlled=0
                    AND (fol.max_ln_total != fol.min_ln_total AND foh.min_ord_total != 0)
                    THEN ABS(COALESCE(fol.max_ln_total, 0)) - ABS(COALESCE(fol.min_ln_total, 0))
                WHEN fol.is_cnlled = 1
                    AND fol.is_rtn = 0
                    AND foh.pymt_status_id IN (6000, 7000)
                    AND fol.max_fulflmnt_status_id <= 9000
                    THEN ABS(COALESCE(fol.max_ln_total, 0)) - ABS(COALESCE(fol.min_ln_total, 0))
                ELSE
                    ABS(COALESCE(fv.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(fv.fv_ord_ln_sub_total, 0))
                    + ABS(COALESCE(fv.fv_cnlled_ord_shipping_amt, 0) + COALESCE(fv.fv_ord_shipping_amt, 0))
                    + ABS(COALESCE(fv.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(fv.fv_ord_shipping_tax_amt, 0))
                    + ABS(COALESCE(fv.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(fv.fv_ord_sales_tax_amt, 0))
            END
        ) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS VARCHAR) AS ih_totalamount,
        rtn.refundmethods AS refundmethods,
        pymt_line.paymentsinfo AS paymentsinfo,
        CAST(NULL AS VARCHAR) AS refund_notes,
        rtn.refund_status_history AS refunds_statushistory,
        fol.updated_ts AS load_time_kafka,
        fol.etl_updt_ts AS load_time_adls,
        foh.created_ts::DATE AS order_date,
        fol.updated_ts AS postedat,
        fol.updated_by AS postedby,
        fol.updated_ts AS updated_datetime,
        fol.ord_ln_id AS linenumber,
        'MAO' AS ref_source
    FROM base_ord_line fol
    JOIN base_ord_hdr foh
        ON NVL(fol.prnt_ord_id, fol.ord_id) = foh.ord_id AND fol.org_id = foh.org_id
    LEFT JOIN refund_hist_details rtn
        ON rtn.org_id = fol.org_id AND rtn.ord_id = fol.ord_id AND rtn.ord_ln_id = fol.ord_ln_id
    LEFT JOIN payment_grouped pymt_line
        ON fol.org_id = pymt_line.org_id and NVL(fol.prnt_ord_id, fol.ord_id) = pymt_line.ord_id and NVL(fol.prnt_ord_ln_id,fol.ord_ln_id) = pymt_line.ord_ln_id
    LEFT JOIN product_master pm
        ON fol.is_gift_card != 1
        AND TRIM(fol.item_id) = TRIM(pm.global_size_id)
        AND (CASE WHEN foh.org_id IN ('FL-CA', 'CH-CA') THEN '98' ELSE '81' END) = pm.banner_id
    LEFT JOIN product_master_div pm_div
        ON fol.is_gift_card != 1
        AND TRIM(fol.item_id) = TRIM(pm_div.global_size_id)
        AND fol.org_id = pm_div.org_desc
    LEFT JOIN fct_mao_ord_line_fv fv
        ON fol.org_id = fv.org_id AND fol.ord_id = fv.ord_id AND fol.ord_ln_id = fv.ord_ln_id AND fv.fv_rnk = 1
    LEFT JOIN {{ source('dom_gold', 'dim_mao_item_v') }} itm
        ON TRIM(fol.item_id) = TRIM(itm.item_id)
    WHERE fol.is_even_exchg = 0
        AND (
               (
                fol.is_rtn = 1
                AND foh.pymt_status_id IN (6000, 7000)
                AND fol.max_fulflmnt_status_id > 9000
                AND fol.max_fulflmnt_status_id != 19000
            )
            OR (
                foh.pymt_status_id IN (6000, 7000)
                AND fol.max_fulflmnt_status_id <= 9000
                AND fol.is_cnlled = 1
                AND fol.qty >= 0
                AND fol.is_rtn = 0
            )
            OR (
                foh.pymt_status_id = 5000
                AND fol.max_ln_total != fol.min_ln_total
                AND foh.max_ord_total != 0
                AND fol.is_cnlled = 0
            )
        )
        AND EXISTS (
                SELECT 1
                FROM legit_refund_payment lrp
                WHERE 
                   lrp.org_id = foh.org_id
                   AND lrp.ord_id = foh.ord_id
                )
        AND NOT EXISTS (
                SELECT 1
                FROM auth_reversal ar
                WHERE 
                    ar.org_id = fol.org_id
                    AND ar.ord_id = fol.ord_id
                )
)
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(order_datetime AS TIMESTAMP) AS order_datetime,
    CAST(refundid AS VARCHAR) AS refundid,
    CAST(companynumber AS DECIMAL(38, 0)) AS companynumber,
    CAST(refund_header_date AS DATE) AS refund_header_date,
    CAST(refund_status AS VARCHAR) AS refund_status,
    CAST(refund_createdby AS VARCHAR) AS refund_createdby,
    CAST(refund_userid AS VARCHAR) AS refund_userid,
    CAST(source AS VARCHAR) AS source,
    CAST(refundnum AS VARCHAR) AS refundnum,
    CAST(quantity AS DECIMAL(38, 0)) AS quantity,
    CAST(name AS VARCHAR) AS name,
    CAST(sku AS VARCHAR) AS sku,
    CAST(size AS VARCHAR) AS size,
    CAST(color AS VARCHAR) AS color,
    CAST(image AS VARCHAR) AS image,
    CAST(brand AS VARCHAR) AS brand,
    CAST(category AS VARCHAR) AS category,
    CAST(description AS VARCHAR) AS description,
    CAST(iscollectupfront AS VARCHAR) AS iscollectupfront,
    CAST(backorderflag AS VARCHAR) AS backorderflag,
    CAST(launchskuflag AS VARCHAR) AS launchskuflag,
    CAST(taxcode AS VARCHAR) AS taxcode,
    CAST(productdesignator AS VARCHAR) AS productdesignator,
    CAST(productnumber AS VARCHAR) AS productnumber,
    CAST(producttype AS VARCHAR) AS producttype,
    CAST(cegrrefid AS VARCHAR) AS cegrrefid,
    CAST(originalretailprice AS DECIMAL(38, 4)) AS originalretailprice,
    CAST(originalunitprice AS DECIMAL(38, 4)) AS originalunitprice,
    CAST(originalunitdiscountamount AS DECIMAL(38, 4)) AS originalunitdiscountamount,
    CAST(linerefundsubtotal AS DECIMAL(38, 4)) AS linerefundsubtotal,
    CAST(taxamount AS DECIMAL(38, 4)) AS taxamount,
    CAST(shippingamount AS DECIMAL(38, 4)) AS shippingamount,
    CAST(shippingtaxamount AS DECIMAL(38, 4)) AS shippingtaxamount,
    CAST(inboundshippingamount AS DECIMAL(38, 4)) AS inboundshippingamount,
    CAST(totalamount AS DECIMAL(38, 4)) AS totalamount,
    CAST(returnexpected AS VARCHAR) AS returnexpected,
    CAST(returned AS VARCHAR) AS returned,
    returnnumbers AS returnnumbers,
    CAST(quantityreturned AS DECIMAL(38, 0)) AS quantityreturned,
    CAST(returndate AS DATE) AS returndate,
    CAST(reasoncode AS VARCHAR) AS reasoncode,
    CAST(ih_inboundshippingamount AS DECIMAL(38, 4)) AS ih_inboundshippingamount,
    CAST(ih_linerefundsubtotal AS DECIMAL(38, 4)) AS ih_linerefundsubtotal,
    CAST(ih_originalretailprice AS DECIMAL(38, 4)) AS ih_originalretailprice,
    CAST(ih_originalunitdiscountamount AS DECIMAL(38, 4)) AS ih_originalunitdiscountamount,
    CAST(ih_originalunitprice AS DECIMAL(38, 4)) AS ih_originalunitprice,
    CAST(ih_shippingamount AS DECIMAL(38, 4)) AS ih_shippingamount,
    CAST(ih_shippingtaxamount AS DECIMAL(38, 4)) AS ih_shippingtaxamount,
    CAST(ih_taxamount AS DECIMAL(38, 4)) AS ih_taxamount,
    CAST(ih_totalamount AS DECIMAL(38, 4)) AS ih_totalamount,
    refundmethods AS refundmethods,
    paymentsinfo AS paymentsinfo,
    CAST(refund_notes AS VARCHAR) AS refund_notes,
    refunds_statushistory AS refunds_statushistory,
    CAST(load_time_kafka AS TIMESTAMP) AS load_time_kafka,
    CAST(load_time_adls AS TIMESTAMP) AS load_time_adls,
    CAST(order_date AS DATE) AS order_date,
    CAST(postedat AS TIMESTAMP) AS postedat,
    CAST(postedby AS VARCHAR) AS postedby,
    CAST(linenumber AS VARCHAR) AS linenumber,
    CAST(ref_source AS VARCHAR(50)) AS ref_source
FROM refunds
WHERE productnumber IS NOT NULL and order_id NOT LIKE 'R%'