{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}

{{ config(
    materialized="incremental",
    unique_key=["companynumber", "order_id", "exchangeid", "linenumber", "exchange_status"],
    post_hook=["{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'load_time_adls' ) }}"],
    meta={'strategy': "merge"}
) }}

WITH base_ord_hdr AS (
    SELECT *
    FROM {{ source('dom_gold', 'fct_mao_ord_hdr_v') }}
    WHERE doc_type_id = 'CustomerOrder'
),
base_ord_line_hist AS (
    SELECT *
    FROM {{ source('dom_gold', 'fct_mao_ord_line_hist_v') }}
    WHERE max_fulflmnt_status_id IS NOT NULL
        {% if is_incremental() %}
            AND etl_updt_ts >= {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL_DBIMART") | as_text }}'
        {% endif %}
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
        pm.fob_desc,
        pm.desc_long_2,
        pm.designator_id,
        pm.tax_code,
        pm.global_brand_desc,
        pm."DESC"
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
            WHEN ord_line.max_fulflmnt_status_id IN (1000, 1500) THEN 'EXCHANGE_ORDER_CREATED'
            ELSE 'EXCHANGE_INITIATED'
        END AS exchange_status,
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
    WHERE ord_line.is_gift_card = 0
        AND ord_line.is_even_exchg = 1
),
fct_mao_ord_line AS (
    SELECT *
    FROM (
        SELECT
            ord_line.*,
            ROW_NUMBER() OVER (
                PARTITION BY org_id, ord_id, ord_ln_id, exchange_status
                ORDER BY updated_ts DESC
            ) AS ord_ln_status_rnk
        FROM fct_mao_ord_line_stg ord_line
    )
    WHERE ord_ln_status_rnk = 1
),
fct_mao_ord_tax_agg as (
	select org_id ol_org_id,ord_id as ol_ord_id, ord_ln_id as ol_ord_ln_id,
		ABS(SUM(COALESCE(fv.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(fv.fv_ord_shipping_tax_amt, 0))) AS shippingtaxamount,
		ABS(SUM(COALESCE(fv.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(fv.fv_ord_sales_tax_amt, 0))) AS taxamount
	from 
		fct_mao_ord_line fv
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
                            'authTime', CAST(pymt_txn_req_dt AS VARCHAR),
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
        ) AS payments_info
    FROM payment pymt left join fct_mao_ord_tax_agg ord on  (pymt.org_id=ord.ol_org_id and pymt.ord_id=ord.ol_ord_id and pymt.ord_ln_id=ord.ol_ord_ln_id)
    WHERE pymt.pymt_rnk = 1
    GROUP BY all
),
exchanges AS (
SELECT
    CAST(ord_line.prnt_ord_id AS VARCHAR) AS order_id,
    CAST(ord_line.ord_id AS VARCHAR) AS exchangeid,
    CAST(ord_line.exchange_status AS VARCHAR) AS header_exchangestatus,
    CAST(CASE TRIM(ord_line.org_id)
        WHEN 'FL-US' THEN '21'
        WHEN 'FL-CA' THEN '45'
        WHEN 'KFL-US' THEN '22'
        WHEN 'CH-CA' THEN '77'
        WHEN 'CH-US' THEN '20'
    END AS VARCHAR) AS companynumber,
    CAST(ord_line.created_by AS VARCHAR) AS exchange_createdby,
    CAST(ord_line.created_by AS VARCHAR) AS userid,
    CAST(NULL AS VARCHAR) AS source,
    CAST(TO_VARCHAR(
        TRY_TO_TIMESTAMP(CAST(COALESCE(ord_hdr.confirmed_ts, ord_hdr.captured_ts) AS VARCHAR), 'YYYY-MM-DD HH24:MI:SS.FF3'),
        'YYYY-MM-DD"T"HH24:MI:SS.FF3"000000Z"'
    ) AS VARCHAR) AS exchange_header_date,
    CAST(ord_line.ord_id AS VARCHAR) AS exchangenum,
    CAST(ord_line.ord_id AS VARCHAR) AS returnnum,
    CAST(ord_line.ord_id AS VARCHAR) AS exchangeordernum,
    CAST(ord_line.exchange_status AS VARCHAR) AS exchange_status,
    CAST(CASE
        WHEN UPPER(ord_line.dlvry_method_id) = 'PICKUPATSTORE' THEN 'PICK'
        WHEN UPPER(ord_line.dlvry_method_id) = 'PICKUP_IN_STORE' THEN 'PICK'
        WHEN UPPER(ord_line.dlvry_method_id) = 'SHIPTORETURNCENTER' THEN 'PICK'
        WHEN UPPER(ord_line.dlvry_method_id) = 'SHIPTOADDRESS' THEN 'SHIP'
        WHEN UPPER(ord_line.dlvry_method_id) = 'SHIPTOSTORE' THEN 'SHIP'
        WHEN UPPER(ord_line.dlvry_method_id) = 'EMAIL' THEN 'ELECTRONIC'
        WHEN UPPER(ord_line.dlvry_method_id) = 'STORESALE' THEN 'XSTORE'
        ELSE ord_line.dlvry_method_id
    END AS VARCHAR) AS fullfillmenttype,
    CAST(ord_line.cart_shpmnt_method AS VARCHAR) AS shipmethod,
    CAST(ord_line.shpmnt_method AS VARCHAR) AS shipmethoddesc,
    CAST(ord_hdr.ccy_cd AS VARCHAR) AS shippingamount_currencyiso,
    CAST(ABS(COALESCE(ord_line.ord_shipping_amt, 0)) AS VARCHAR) AS shippingamount_value,
    CAST(NULL AS ARRAY) AS shippingline,
    CAST(ord_line.ord_ln_id AS VARCHAR) AS linenumber,
    CAST(NULL AS VARCHAR) AS line_salecode,
    CAST(ord_line.tax_cd AS VARCHAR) AS line_taxcode,
    CAST(NULL AS VARCHAR) AS line_,
    CAST(NULL AS VARCHAR) AS line_giftreceipientemail,
    CAST(NULL AS VARCHAR) AS line_giftfrom,
    CAST(NULL AS VARCHAR) AS line_giftto,
    CAST(NULL AS VARCHAR) AS line_giftcardnum,
    CAST(ord_line.cart_shpmnt_method AS VARCHAR) AS line_shipmethod,
    CAST(CASE
        WHEN ord_line.is_free_shipping = 1 THEN 'true'
        WHEN ord_line.is_free_shipping = 0 THEN 'false'
    END AS VARCHAR) AS line_freeshipping,
    CAST(ord_line.qty AS VARCHAR) AS line_quantity,
    CAST(CASE UPPER(loc.loc_type_id)
        WHEN 'DC' THEN 'WHSE'
        WHEN 'STORE' THEN 'STORE'
        WHEN 'SUPPLIER' THEN 'DROPSHIP'
    END AS VARCHAR) AS line_inventorylocation,
    CAST(ord_line.itm_desc AS VARCHAR) AS name,
    CAST(ord_line.small_image_u_r_i AS VARCHAR) AS image,
    CAST(CASE
        WHEN ord_line.is_gift_card = 1 THEN ord_line.item_id
        WHEN ord_hdr.channel != 'XSTORE' AND ord_line.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.online_us_sku
        WHEN ord_hdr.channel != 'XSTORE' AND ord_line.org_id IN ('FL-CA', 'CH-CA') THEN pm.online_ca_sku
        WHEN ord_hdr.channel = 'XSTORE' THEN pm_div.legacy_sku_size
        ELSE itm.color
    END AS VARCHAR) AS sku,
    CAST(pm.legacy_size_desc AS VARCHAR) AS size,
    CAST(pm.desc_long_2 AS VARCHAR) AS color,
    CAST(pm.global_brand_desc AS VARCHAR) AS brand,
    CAST(pm.fob_desc AS VARCHAR) AS category,
    CAST(pm."DESC" AS VARCHAR) AS description,
    CAST(CASE
        WHEN ord_hdr.is_prepaid = 1 THEN 'true'
        WHEN ord_hdr.is_prepaid = 0 THEN 'false'
    END AS VARCHAR) AS iscollectupfront,
    CAST(CASE
        WHEN ord_line.is_backorderflg = 1 THEN 'true'
        WHEN ord_line.is_backorderflg = 0 THEN 'false'
    END AS VARCHAR) AS backorderflag,
    CAST(CASE
        WHEN ord_line.is_launch_sku_flg = 1 THEN 'true'
        WHEN ord_line.is_launch_sku_flg = 0 THEN 'false'
    END AS VARCHAR) AS launchskuflag,
    CAST(ord_line.tax_cd AS VARCHAR) AS taxcode,
    CAST(pm.designator_id AS VARCHAR) AS productdesignator,
    CAST(TRIM(CASE
        WHEN ord_line.item_id = 'ECARD20' THEN '2138264'
        WHEN ord_line.item_id = 'ECARD21' THEN '2138265'
        WHEN ord_line.item_id = 'ECARD22' THEN '2138266'
        WHEN ord_line.item_id = 'ECARD45' THEN '20'
        WHEN ord_line.item_id = 'ECARD77' THEN '2000003'
        WHEN ord_line.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
        WHEN ord_line.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.internal_product_number
        ELSE ord_line.item_id
    END) AS VARCHAR) AS productnumber,
    CAST(ord_line.product_type AS VARCHAR) AS producttype,
    CAST(ord_hdr.ccy_cd AS VARCHAR) AS currencyiso,
    CAST(ord_line.price_override_reason AS VARCHAR) AS priceoverridereason,
    CAST(ABS(COALESCE(ord_line.fv_orig_unit_price, ord_line.fv_unit_price, 0)) AS VARCHAR) AS originalretailprice,
    CAST(ABS(COALESCE(ord_line.fv_unit_price, 0)) AS VARCHAR) AS unitprice,
    CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) AS VARCHAR) AS subtotalamount,
    CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS VARCHAR) AS taxamount,
    CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) AS VARCHAR) AS shippingamount,
    CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) AS VARCHAR) AS shippingtaxamount,
    CAST(NULL AS VARCHAR) AS giftboxamount,
    CAST(NULL AS VARCHAR) AS giftboxtaxamount,
    CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
		ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
		ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
		ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS VARCHAR) AS totalamount,
    CAST(ABS(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0)) AS VARCHAR) AS discountamount,
    CAST((ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
		ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
		ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
		ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0))) -
		ABS(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0)) AS VARCHAR) AS discountedtotalamount,
    OBJECT_CONSTRUCT(
        'companyNumber', CAST(CASE TRIM(ord_line.org_id)
            WHEN 'FL-US' THEN '21'
            WHEN 'FL-CA' THEN '45'
            WHEN 'KFL-US' THEN '22'
            WHEN 'CH-CA' THEN '77'
            WHEN 'CH-US' THEN '20'
        END AS VARCHAR),
        'orderId', CAST(ord_line.ord_id AS VARCHAR),
        'totalAmount', CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
							ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
							ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
							ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS VARCHAR),
        'totalrefundAmount', CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
							ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
							ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
							ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS VARCHAR)
    ) AS paymentrequest,
    pmt.payments_info AS paymentsinfo,
    ord_line.ship_to_addr_first_name AS alternateshipping_firstname,
    ord_line.ship_to_addr_last_name AS alternateshipping_lastname,
    ord_line.ship_to_addr_email AS alternateshipping_email,
    CAST(CASE TRIM(ord_line.org_id)
        WHEN 'FL-US' THEN 'footlocker'
        WHEN 'FL-CA' THEN 'footlocker'
        WHEN 'KFL-US' THEN 'kidsfootlocker'
        WHEN 'CH-CA' THEN 'champs'
        WHEN 'CH-US' THEN 'champs'
    END AS VARCHAR) AS alternateshipping_companyname,
    ord_line.ship_to_addr_phone AS alternateshipping_phonenumber,
    ord_line.ship_to_addr_addr1 AS alternateshipping_addressline1,
    ord_line.ship_to_addr_addr2 AS alternateshipping_addressline2,
    ord_line.ship_to_addr_city AS alternateshipping_city,
    ord_line.ship_to_addr_state AS alternateshipping_state,
    CAST(NULL AS VARCHAR) AS alternateshipping_statecode,
    ord_line.ship_to_addr_country AS alternateshipping_country,
    ord_line.ship_to_addr_country AS alternateshipping_countrycode,
    ord_line.ship_to_addr_postal_cd AS alternateshipping_postalcode,
    CAST(ord_hdr.created_ts AS VARCHAR) AS orderdatetime,
    CAST(ord_hdr.updated_ts AS VARCHAR) AS postedat,
    CAST(ord_line.updated_by AS VARCHAR) AS postedby,
    CAST(ord_line.updated_ts AS TIMESTAMP) AS load_time_kafka,
    CAST(ord_line.etl_updt_ts AS TIMESTAMP) AS load_time_adls,
    CAST(DATE(COALESCE(ord_hdr.confirmed_ts, ord_hdr.captured_ts)) AS DATE) AS orderdate,
    CAST('MAO' AS VARCHAR) AS ref_source
FROM fct_mao_ord_line ord_line
JOIN base_ord_hdr ord_hdr
    ON ord_line.org_id = ord_hdr.org_id AND ord_line.ord_id = ord_hdr.ord_id
LEFT JOIN payment_grouped pmt
    ON (ord_line.org_id = pmt.org_id and ord_line.prnt_ord_id = pmt.ord_id and ord_line.prnt_ord_ln_id = pmt.ord_ln_id)
LEFT JOIN {{ source('dom_gold', 'dim_mao_employee_v') }} emp
    ON ord_line.created_by = emp.user_id
LEFT JOIN product_master pm
    ON ord_line.is_gift_card != 1
    AND TRIM(ord_line.item_id) = TRIM(pm.global_size_id)
    AND (CASE WHEN ord_line.org_id IN ('FL-CA', 'CH-CA') THEN '98' ELSE '81' END) = pm.banner_id
LEFT JOIN product_master_div pm_div
    ON ord_line.is_gift_card != 1
    AND TRIM(ord_line.item_id) = TRIM(pm_div.global_size_id)
    AND ord_line.org_id = pm_div.org_desc
LEFT JOIN {{ source('dom_gold', 'dim_mao_item_v') }} itm
    ON TRIM(ord_line.item_id) = TRIM(itm.item_id)
LEFT JOIN {{ source('dom_gold', 'dim_mao_loc_v') }} loc
    ON COALESCE(ord_line.ship_from_loc_id, ord_line.physical_org_id) = loc.loc_id
WHERE COALESCE(pm.online_us_sku, ord_line.item_id, pm.online_ca_sku) IS NOT NULL
)
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(exchangeid AS VARCHAR) AS exchangeid,
    CAST(header_exchangestatus AS VARCHAR) AS header_exchangestatus,
    CAST(companynumber AS VARCHAR) AS companynumber,
    CAST(exchange_createdby AS VARCHAR) AS exchange_createdby,
    CAST(userid AS VARCHAR) AS userid,
    CAST(source AS VARCHAR) AS source,
    CAST(exchange_header_date AS VARCHAR) AS exchange_header_date,
    CAST(exchangenum AS VARCHAR) AS exchangenum,
    CAST(returnnum AS VARCHAR) AS returnnum,
    CAST(exchangeordernum AS VARCHAR) AS exchangeordernum,
    CAST(exchange_status AS VARCHAR) AS exchange_status,
    CAST(fullfillmenttype AS VARCHAR) AS fullfillmenttype,
    CAST(shipmethod AS VARCHAR) AS shipmethod,
    CAST(shipmethoddesc AS VARCHAR) AS shipmethoddesc,
    CAST(shippingamount_currencyiso AS VARCHAR) AS shippingamount_currencyiso,
    CAST(shippingamount_value AS VARCHAR) AS shippingamount_value,
    shippingline AS shippingline,
    CAST(linenumber AS VARCHAR) AS linenumber,
    CAST(line_salecode AS VARCHAR) AS line_salecode,
    CAST(line_taxcode AS VARCHAR) AS line_taxcode,
    CAST(line_ AS VARCHAR) AS line_,
    CAST(line_giftreceipientemail AS VARCHAR) AS line_giftreceipientemail,
    CAST(line_giftfrom AS VARCHAR) AS line_giftfrom,
    CAST(line_giftto AS VARCHAR) AS line_giftto,
    CAST(line_giftcardnum AS VARCHAR) AS line_giftcardnum,
    CAST(line_shipmethod AS VARCHAR) AS line_shipmethod,
    CAST(line_freeshipping AS VARCHAR) AS line_freeshipping,
    CAST(line_quantity AS VARCHAR) AS line_quantity,
    CAST(line_inventorylocation AS VARCHAR) AS line_inventorylocation,
    CAST(name AS VARCHAR) AS name,
    CAST(image AS VARCHAR) AS image,
    CAST(sku AS VARCHAR) AS sku,
    CAST(size AS VARCHAR) AS size,
    CAST(color AS VARCHAR) AS color,
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
    CAST(currencyiso AS VARCHAR) AS currencyiso,
    CAST(priceoverridereason AS VARCHAR) AS priceoverridereason,
    CAST(originalretailprice AS DECIMAL(15,4)) AS originalretailprice,
    CAST(unitprice AS DECIMAL(15,4)) AS unitprice,
    CAST(subtotalamount AS DECIMAL(15,4)) AS subtotalamount,
    CAST(taxamount AS DECIMAL(15,4)) AS taxamount,
    CAST(shippingamount AS DECIMAL(15,4)) AS shippingamount,
    CAST(shippingtaxamount AS DECIMAL(15,4)) AS shippingtaxamount,
    CAST(giftboxamount AS DECIMAL(15,4)) AS giftboxamount,
    CAST(giftboxtaxamount AS DECIMAL(15,4)) AS giftboxtaxamount,
    CAST(totalamount AS DECIMAL(15,4)) AS totalamount,
    CAST(discountamount AS DECIMAL(15,4)) AS discountamount,
    CAST(discountedtotalamount AS DECIMAL(15,4)) AS discountedtotalamount,
    paymentrequest AS paymentrequest,
    paymentsinfo AS paymentsinfo,
    CAST(alternateshipping_firstname AS VARCHAR) AS alternateshipping_firstname,
    CAST(alternateshipping_lastname AS VARCHAR) AS alternateshipping_lastname,
    CAST(alternateshipping_email AS VARCHAR) AS alternateshipping_email,
    CAST(alternateshipping_companyname AS VARCHAR) AS alternateshipping_companyname,
    CAST(alternateshipping_phonenumber AS VARCHAR) AS alternateshipping_phonenumber,
    CAST(alternateshipping_addressline1 AS VARCHAR) AS alternateshipping_addressline1,
    CAST(alternateshipping_addressline2 AS VARCHAR) AS alternateshipping_addressline2,
    CAST(alternateshipping_city AS VARCHAR) AS alternateshipping_city,
    CAST(alternateshipping_state AS VARCHAR) AS alternateshipping_state,
    CAST(alternateshipping_statecode AS VARCHAR) AS alternateshipping_statecode,
    CAST(alternateshipping_country AS VARCHAR) AS alternateshipping_country,
    CAST(alternateshipping_countrycode AS VARCHAR) AS alternateshipping_countrycode,
    CAST(alternateshipping_postalcode AS VARCHAR) AS alternateshipping_postalcode,
    CAST(orderdatetime AS TIMESTAMP) AS orderdatetime,
    CAST(postedat AS TIMESTAMP) AS postedat,
    CAST(postedby AS VARCHAR) AS postedby,
    CAST(load_time_kafka AS TIMESTAMP) AS load_time_kafka,
    CAST(load_time_adls AS TIMESTAMP) AS load_time_adls,
    CAST(orderdate AS DATE) AS orderdate,
    CAST(ref_source AS VARCHAR(50)) AS ref_source
FROM exchanges
WHERE productnumber IS NOT NULL
