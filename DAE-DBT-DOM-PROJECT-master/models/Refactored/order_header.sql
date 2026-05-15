{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}

{{ config(
    materialized="incremental",
    unique_key=["company_number", "order_id", "order_status"],
    post_hook=["{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'load_time_adls' ) }}"],
    meta={'strategy': "merge"}
) }}


WITH fct_mao_ord_pymt_line AS (
    SELECT *
    FROM (
        SELECT
            org_id,
            ord_id,
            addr_firstname AS bill_addr_firstname,
            addr_lastname AS bill_addr_lastname,
            addr_addr1 AS bill_addr_addr1,
            addr_addr2 AS bill_addr_addr2,
            addr_addr3 AS bill_addr_addr3,
            addr_city AS bill_addr_city,
            addr_postal_cd AS bill_addr_postal_cd,
            addr_state AS bill_addr_state,
            addr_country AS bill_addr_country,
            addr_email AS bill_addr_email,
            addr_phone AS bill_addr_phone,
            ROW_NUMBER() OVER (PARTITION BY org_id, ord_id ORDER BY src_load_ts DESC) AS rnk,
            pymt_gateway_id
        FROM {{ source('dom_gold', 'fct_mao_ord_pymt_line_v') }}
        WHERE ord_id IS NOT NULL
    )
    WHERE rnk = 1
),
fct_mao_ord_line_with_fv AS (
    SELECT
        ord_line.ord_id,
        ord_line.org_id,
        ord_line.ord_ln_id,
        ord_line.pkg_cnt,
        ord_line.ord_coupons,
        ord_line.IS_APPEASEMENT,
        ord_line.is_gift_card,
        cnl_reason.oms_cancel_code AS cnl_reason_id,
        ord_line.cnl_reason_desc AS cnl_reason_desc,
        ord_line.physical_org_id AS physical_org_id,
        ord_line.is_gift AS is_gift,
        ord_line.is_base_shipping_charged AS is_base_shipping_charged,
        ord_line.cart_shpmnt_method AS cart_shpmnt_method,
        ord_line.shpmnt_method AS shpmnt_method,
        ord_line.appeasment_reason_cd AS appeasment_reason_cd,
        ord_line.relate_ord_num_csa AS relate_ord_num_csa,
        ord_line.etl_updt_ts AS etl_updt_ts,
        ord_line.loyalty_reward_id AS loyalty_reward_id,
        ord_line.is_loyalty_disc AS is_loyalty_disc,
        ord_line.RUSH_FLG AS RUSH_FLG,
        ord_line.SHIP_FROM_LOC_ID AS SHIP_FROM_LOC_ID,
        ord_line.is_refund_gift_card AS is_refund_gift_card,
        ord_line.is_even_exchg,
        ord_line.ship_to_addr_addr1 AS ship_to_addr_addr1,
        ord_line.ship_to_addr_addr2 AS ship_to_addr_addr2,
        ord_line.ship_to_addr_city AS ship_to_addr_city,
        ord_line.ship_to_addr_country AS ship_to_addr_country,
        ord_line.ship_to_addr_state AS ship_to_addr_state,
        ord_line.ship_to_addr_postal_cd AS ship_to_addr_postal_cd,
        ord_line.ship_to_addr_email AS ship_to_addr_email,
        ord_line.ship_to_addr_first_name AS ship_to_addr_first_name,
        ord_line.ship_to_addr_last_name AS ship_to_addr_last_name,
        ord_line.ship_to_addr_phone AS ship_to_addr_phone,
        ord_line.prnt_ord_id,
        ord_line.updated_by AS updated_by,
        ord_line.created_by AS created_by,
        ord_line.updated_ts,
		ord_line.cnlled_ord_coupon_amt AS fv_cnlled_ord_coupon_amt,
		ord_line.ord_coupon_amt AS fv_ord_coupon_amt,
        ord_line.cnlled_orig_ord_shipping_amt AS fv_cnlled_ord_shipping_amt,
        ord_line.orig_ord_shipping_amt AS fv_ord_shipping_amt,
        ord_line.cnlled_orig_ord_sales_tax_amt AS fv_cnlled_ord_sales_tax_amt,
        ord_line.orig_ord_sales_tax_amt AS fv_ord_sales_tax_amt,
        ord_line.cnlled_orig_ord_shipping_tax_amt AS fv_cnlled_ord_shipping_tax_amt,
        ord_line.orig_ord_shipping_tax_amt AS fv_ord_shipping_tax_amt,
        ord_line.gift_card_value AS fv_gift_card_value,
        ord_line.cnlled_total_disc AS fv_cnlled_total_disc,
        ord_line.total_disc AS fv_total_disc,
        ord_line.cnlled_ord_ln_total AS fv_cnlled_ord_ln_total,
        ord_line.ord_ln_total AS fv_ord_ln_total,
        ord_line.cnlled_ord_ln_sub_total AS fv_cnlled_ord_ln_sub_total,
        ord_line.ord_ln_sub_total AS fv_ord_ln_sub_total,
        ord_line.cnlled_total_charges AS fv_cnlled_total_charges,
        ord_line.total_charges AS fv_total_charges,
        ord_line.cnlled_total_taxes AS fv_cnlled_total_taxes,
        ord_line.total_taxes AS fv_total_taxes
    FROM {{ source('dom_gold', 'fct_mao_ord_line_hist_v') }} ord_line
    JOIN {{ source('dom_gold', 'fct_mao_ord_hdr_v') }} ord_hdr
        ON ord_line.org_id = ord_hdr.org_id AND ord_line.ord_id = ord_hdr.ord_id
    LEFT JOIN {{ source('dom_gold', 'lkp_cancel_code_reason_v') }} cnl_reason
        ON ord_line.cnl_reason_id = cnl_reason.cancel_reason_id
    WHERE ord_hdr.doc_type_id = 'CustomerOrder'
        AND NOT (ord_line.max_fulflmnt_status_id IS NULL OR ord_line.max_fulflmnt_status_id > 9000)
        AND ord_line.prnt_ord_id IS NULL
        AND ord_line.is_even_exchg = 0
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) = 1
),
fct_mao_ord_line AS (
    SELECT
        ord_id,
        org_id,
        MAX_BY(cnl_reason_id, updated_ts) AS cnl_reason_id,
        MAX_BY(cnl_reason_desc, updated_ts) AS cnl_reason_desc,
        MAX_BY(physical_org_id, updated_ts) AS physical_org_id,
        MAX_BY(is_gift, updated_ts) AS is_gift,
        MAX_BY(is_base_shipping_charged, updated_ts) AS is_base_shipping_charged,
        SUM(COALESCE(pkg_cnt, 0)) AS pkg_cnt,
        MAX_BY(cart_shpmnt_method, updated_ts) AS cart_shpmnt_method,
        MAX_BY(shpmnt_method, updated_ts) AS shpmnt_method,
        MAX_BY(appeasment_reason_cd, updated_ts) AS appeasment_reason_cd,
        MAX_BY(relate_ord_num_csa, updated_ts) AS relate_ord_num_csa,
        MAX_BY(etl_updt_ts, updated_ts) AS etl_updt_ts,
        MAX_BY(loyalty_reward_id, updated_ts) AS loyalty_reward_id,
        MAX_BY(is_loyalty_disc, updated_ts) AS is_loyalty_disc,
        MAX_BY(RUSH_FLG, updated_ts) AS RUSH_FLG,
        ARRAY_UNION_AGG(parse_json(ord_coupons)) AS ord_coupons,
        MAX_BY(SHIP_FROM_LOC_ID, updated_ts) AS SHIP_FROM_LOC_ID,
        MAX(COALESCE(IS_APPEASEMENT, 0)) AS IS_APPEASEMENT,
        MAX(is_gift_card) AS is_gift_card,
        MAX_BY(is_refund_gift_card, updated_ts) AS is_refund_gift_card,
        MAX_BY(is_even_exchg, updated_ts) AS is_even_exchg,
        MAX_BY(ship_to_addr_addr1, updated_ts) AS ship_to_addr_addr1,
        MAX_BY(ship_to_addr_addr2, updated_ts) AS ship_to_addr_addr2,
        MAX_BY(ship_to_addr_city, updated_ts) AS ship_to_addr_city,
        MAX_BY(ship_to_addr_country, updated_ts) AS ship_to_addr_country,
        MAX_BY(ship_to_addr_state, updated_ts) AS ship_to_addr_state,
        MAX_BY(ship_to_addr_postal_cd, updated_ts) AS ship_to_addr_postal_cd,
        MAX_BY(ship_to_addr_email, updated_ts) AS ship_to_addr_email,
        MAX_BY(ship_to_addr_first_name, updated_ts) AS ship_to_addr_first_name,
        MAX_BY(ship_to_addr_last_name, updated_ts) AS ship_to_addr_last_name,
        MAX_BY(ship_to_addr_phone, updated_ts) AS ship_to_addr_phone,
        MAX_BY(prnt_ord_id, updated_ts) AS prnt_ord_id,
        MAX_BY(updated_by, updated_ts) AS updated_by,
        MAX_BY(created_by, updated_ts) AS created_by,
		SUM(fv_cnlled_ord_coupon_amt) AS ol_cnlled_ord_coupon_amt,
		SUM(fv_ord_coupon_amt) AS ol_ord_coupon_amt,
        SUM(fv_cnlled_ord_sales_tax_amt) AS ol_cnlled_ord_sales_tax_amt,
        SUM(fv_ord_sales_tax_amt) AS ol_ord_sales_tax_amt,
        SUM(fv_cnlled_ord_shipping_tax_amt) AS ol_cnlled_ord_shipping_tax_amt,
        SUM(fv_ord_shipping_tax_amt) AS ol_ord_shipping_tax_amt,
        SUM(fv_gift_card_value) AS ol_gift_card_value,
        SUM(fv_cnlled_total_disc) AS ol_cnlled_total_disc,
        SUM(fv_total_disc) AS ol_total_disc,
        SUM(fv_cnlled_ord_ln_total) AS ol_cnlled_ord_ln_total,
        SUM(fv_ord_ln_total) AS ol_ord_ln_total,
        SUM(fv_cnlled_ord_ln_sub_total) AS ol_cnlled_ord_ln_sub_total,
        SUM(fv_ord_ln_sub_total) AS ol_ord_ln_sub_total,
        SUM(fv_cnlled_total_charges) AS ol_cnlled_total_charges,
        SUM(fv_total_charges) AS ol_total_charges,
        SUM(fv_cnlled_total_taxes) AS ol_cnlled_total_taxes,
        SUM(fv_total_taxes) AS ol_total_taxes,
        SUM(fv_cnlled_ord_shipping_amt) AS ol_cnlled_ord_shipping_amt,
        SUM(fv_ord_shipping_amt) AS ol_ord_shipping_amt
    FROM fct_mao_ord_line_with_fv
    GROUP BY ord_id, org_id
),
fct_mao_ord_hdr AS (
    SELECT
        ord_hdr.*,
        ROW_NUMBER() OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id, ord_hdr.order_status ORDER BY ord_hdr.updated_ts DESC) AS ord_status_rnk
    FROM (
        SELECT
            ord_hdr.org_id,
            ord_hdr.ord_id,
            CASE
                WHEN ord_hdr.max_fulflmnt_status_id = '1000' THEN 'SUBMITTED'
                WHEN ord_hdr.max_fulflmnt_status_id = '1500' THEN 'SUBMITTED'
                WHEN ord_hdr.is_fraud_service_failed = 1 THEN 'FRAUD_CHECK_FAILED'
                WHEN ord_hdr.max_fulflmnt_status_id = '1600' THEN 'FULFILMENT_PROCESSING'
                WHEN ord_hdr.max_fulflmnt_status_id = '2000' THEN 'FULFILMENT_PROCESSING'
                WHEN ord_hdr.max_fulflmnt_status_id = '3000' THEN 'FULFILMENT_PROCESSING'
                WHEN ord_hdr.max_fulflmnt_status_id = '3500' THEN 'FULFILMENT_PROCESSING'
                WHEN ord_hdr.max_fulflmnt_status_id = '3600' THEN 'FULFILMENT_PROCESSING'
                WHEN ord_hdr.max_fulflmnt_status_id = '3700' THEN 'FULFILMENT_PROCESSING'
                WHEN ord_hdr.max_fulflmnt_status_id = '7000' THEN 'FULFILMENT_COMPLETE'
                WHEN ord_hdr.max_fulflmnt_status_id = '7500' THEN 'FULFILMENT_COMPLETE'
                WHEN ord_hdr.max_fulflmnt_status_id = '8000' THEN 'FULFILMENT_COMPLETE'
                WHEN ord_hdr.max_fulflmnt_status_id = '8500' THEN 'FULFILMENT_COMPLETE'
                WHEN ord_hdr.max_fulflmnt_status_id = '9000' THEN 'CANCELLED'
                WHEN ord_hdr.max_fulflmnt_status_id = '13000' THEN 'WAIT_FRAUD_SYSTEM_CHECK'
                ELSE UPPER(ord_hdr.max_fulflmnt_status_desc)
            END AS order_status,
            ord_hdr.ORD_LOCALE,
            ord_hdr.confirmed_ts,
            ord_hdr.captured_ts,
            ord_hdr.CCY_CD,
            ord_hdr.CUST_EMAIL,
            ord_hdr.cust_id,
            ord_hdr.cust_type_id,
            ord_hdr.FLX_ID,
            ord_hdr.prnt_resrv_req_id,
            ord_hdr.browser_ip,
            ord_hdr.VENDOR_ID,
            ord_hdr.channel,
            ord_hdr.ord_type_id,
            FIRST_VALUE(ord_hdr.cnlled_ord_shipping_amt) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS cnlled_ord_shipping_amt,
            FIRST_VALUE(ord_hdr.cnlled_ord_coupon_amt) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS cnlled_ord_coupon_amt,
            FIRST_VALUE(ord_hdr.cnlled_ord_total) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS cnlled_ord_total,
            FIRST_VALUE(ord_hdr.cnlled_total_discs) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS cnlled_total_discs,
            FIRST_VALUE(ord_hdr.cnlled_ord_shipping_tax_amt) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS cnlled_ord_shipping_tax_amt,
            FIRST_VALUE(ord_hdr.cnlled_ord_total_charges) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS cnlled_ord_total_charges,
            FIRST_VALUE(ord_hdr.cnlled_ord_sub_total) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS cnlled_ord_sub_total,
            FIRST_VALUE(ord_hdr.cnlled_ord_total_taxes) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS cnlled_ord_total_taxes,
            FIRST_VALUE(ord_hdr.ord_shipping_amt) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS ord_shipping_amt,
            FIRST_VALUE(ord_hdr.ord_coupon_amt) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS ord_coupon_amt,
            FIRST_VALUE(ord_hdr.ord_total) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS ord_total,
            FIRST_VALUE(ord_hdr.ord_total_discs) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS ord_total_discs,
            FIRST_VALUE(ord_hdr.ord_shipping_tax_amt) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS ord_shipping_tax_amt,
            FIRST_VALUE(ord_hdr.ord_total_charges) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS ord_total_charges,
            FIRST_VALUE(ord_hdr.ord_sub_total) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS ord_sub_total,
            FIRST_VALUE(ord_hdr.ord_total_taxes) OVER (PARTITION BY ord_hdr.org_id, ord_hdr.ord_id ORDER BY ord_hdr.max_fulflmnt_status_id ASC, ord_hdr.updated_ts ASC) AS ord_total_taxes,
            ord_hdr.associate_num,
            ord_hdr.csa_ord_note,
            ord_hdr.pymt_type,
            ord_hdr.doc_type_id,
            ord_hdr.updated_ts,
            ord_hdr.created_ts,
            ord_hdr.etl_updt_ts
        FROM {{ source('dom_gold', 'fct_mao_ord_hdr_hist_v') }} ord_hdr
        WHERE ord_hdr.doc_type_id = 'CustomerOrder'
            AND NOT (ord_hdr.max_fulflmnt_status_id IS NULL OR ord_hdr.max_fulflmnt_status_id > 9000)
            {% if is_incremental() %}
                AND ord_hdr.etl_updt_ts >= {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL_DBIMART") | as_text }}'
            {% endif %}
    ) ord_hdr
),
fct_exchange_orders AS (
    SELECT ol.org_id, ol.prnt_ord_id, ol.is_even_exchg
    FROM {{ source('dom_gold', 'fct_mao_ord_line_v') }} ol
    JOIN {{ source('dom_gold', 'fct_mao_ord_hdr_v') }} oh
        ON ol.org_id = oh.org_id AND ol.ord_id = oh.ord_id
    WHERE oh.doc_type_id = 'CustomerOrder'
        AND ol.is_even_exchg = 1
        AND ol.prnt_ord_id IS NOT NULL
        AND ol.max_fulflmnt_status_id IS NOT NULL
    GROUP BY ALL
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
payment AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY org_id, ord_id, pymt_method_id, pymt_txn_id
            ORDER BY NVL(PYMT_TXN_DETAIL_ID, '') DESC
        ) AS rnk
    FROM {{ source('dom_gold', 'fct_mao_ord_pymt_line_v') }}
),
authorizations_agg AS (
    SELECT
        org_id,
        ord_id,
        CAST(ccy_code AS VARCHAR) AS currencyIso,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'amount', CAST(pymt_method_amt AS VARCHAR),
                'authCode', CAST(auth_cd AS VARCHAR),
                'cardLast4', CAST(txn_card_last4 AS VARCHAR),
                'cegrRefId', CAST(inv_id AS VARCHAR),
                'paymentTransactionSubType', CAST(NULL AS VARCHAR),
                'paymentTransactionType', CAST(PYMT_TXN_TYPE AS VARCHAR),
                'paymentType', CAST(CASE WHEN pymt_type = 'Gift Card' THEN 'GIFTCARD' ELSE UPPER(pymt_type) END AS VARCHAR),
                'creditCardType', CAST(PYMT_CARD_TYPE AS VARCHAR),
                'date', CAST(COALESCE(PYMT_TXN_DT, pymt_txn_req_dt) AS VARCHAR),
                'shippingTaxAmount', CAST(NULL AS VARCHAR),
                'taxAmount', CAST(NULL AS VARCHAR),
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
                    'giftCardNumber', CAST(CASE WHEN pymt_type = 'Gift Card' THEN attrib_card_last4 ELSE NULL END AS VARCHAR),
                    'sellerProtection', CAST(CASE WHEN pymt_type = 'Gift Card' THEN SELLER_PROTECTION_STATUS ELSE NULL END AS VARCHAR)
                ),
                'authAmount', CAST(pymt_txn_req_amt AS VARCHAR),
                'errorMessage', CAST(NULL AS VARCHAR),
                'id', CAST(NULL AS VARCHAR),
                'originalOrderNumber', CAST(ord_id AS VARCHAR),
                'preSettled', CAST(NULL AS VARCHAR),
                'transactionDate', CAST(COALESCE(PYMT_TXN_DT, pymt_txn_req_dt) AS VARCHAR),
                'transactionId', CAST(pymt_txn_id AS VARCHAR),
                'creditCard', CASE WHEN pymt_type = 'Credit Card' THEN
                    OBJECT_CONSTRUCT(
                        'authInfo', OBJECT_CONSTRUCT(
                            'authAmount', CAST(pymt_txn_req_amt AS VARCHAR),
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
                        'amount', CAST(pymt_method_amt AS VARCHAR),
                        'authCode', CAST(auth_cd AS VARCHAR),
                        'giftCardNumber', CAST(attrib_card_last4 AS VARCHAR),
                        'originalOrderNumber', CAST(ord_id AS VARCHAR),
                        'preSettled', CAST(NULL AS VARCHAR),
                        'transactionDate', CAST(COALESCE(PYMT_TXN_DT, pymt_txn_req_dt) AS VARCHAR),
                        'transactionId', CAST(pymt_txn_id AS VARCHAR)
                    )
                ELSE NULL END,
                'paypal', CASE WHEN pymt_type = 'PayPal' THEN
                    OBJECT_CONSTRUCT(
                        'amount', CAST(pymt_method_amt AS VARCHAR),
                        'authInfo', OBJECT_CONSTRUCT(
                            'authAmount', CAST(crnt_auth_amt AS VARCHAR),
                            'authCode', CAST(auth_cd AS VARCHAR),
                            'authResponse', CAST(NULL AS VARCHAR),
                            'authTime', CAST(pymt_txn_req_dt AS VARCHAR),
                            'avsCode', CAST(attrib_avs_code AS VARCHAR),
                            'cvvResponse', CAST(attrib_cvv_response AS VARCHAR),
                            'originalOrderNumber', CAST(ord_id AS VARCHAR),
                            'preSettled', CAST(NULL AS VARCHAR),
                            'referenceNumber', CAST(transaction_ref_id AS VARCHAR),
                            'transactionId', CAST(pymt_txn_id AS VARCHAR)
                        ),
                        'paypalEmailId', CAST(addr_email AS VARCHAR),
                        'transactionDate', CAST(COALESCE(PYMT_TXN_DT, pymt_txn_req_dt) AS VARCHAR),
                        'transactionId', CAST(pymt_txn_id AS VARCHAR)
                    )
                ELSE NULL END
            )
        ) AS Payments_Info
    FROM payment
    WHERE rnk = 1
    GROUP BY org_id, ord_id, currencyIso
),
payment_grouped AS (
    SELECT
        org_id,
        ord_id,
        OBJECT_CONSTRUCT('authorizations', Payments_Info, 'currencyIso', currencyIso) AS paymentsinfo
    FROM authorizations_agg
),
order_header AS (
SELECT
    ord_hdr.ord_id AS order_id,
    CAST(CASE TRIM(ord_hdr.org_id)
        WHEN 'FL-US' THEN '21'
        WHEN 'FL-CA' THEN '45'
        WHEN 'KFL-US' THEN '22'
        WHEN 'CH-CA' THEN '77'
        WHEN 'CH-US' THEN '20'
    END AS VARCHAR) AS company_number,
    CAST(ord_hdr.order_status AS VARCHAR) AS order_status,
    CAST(ord_line.cnl_reason_id AS VARCHAR) AS cancel_code,
    CAST(ord_line.cnl_reason_desc AS VARCHAR) AS cancelreason,
    CAST(CASE WHEN exch.is_even_exchg IS NOT NULL THEN 'true' WHEN ord_line.is_even_exchg = 1 THEN 'true' END AS VARCHAR) AS exchangeorder_flag,
    pymt.bill_addr_addr1 AS billing_address_line1,
    pymt.bill_addr_addr2 AS billing_address_line2,
    pymt.bill_addr_city AS billing_city,
    pymt.bill_addr_country AS billing_country,
    pymt.bill_addr_email AS billing_email,
    pymt.bill_addr_firstname AS billing_first_name,
    pymt.bill_addr_lastname AS billing_last_name,
    pymt.bill_addr_postal_cd AS billing_postal_code,
    pymt.bill_addr_state AS billing_postal_state,
    pymt.bill_addr_phone AS billing_phonenumber,
    CAST('false' AS VARCHAR) AS order_giftbox_flag,
    CAST(CASE ord_line.IS_GIFT
        WHEN 1 THEN 'true'
        WHEN 0 THEN 'false'
    END AS VARCHAR) AS order_giftorder_flag,
    CAST(ord_hdr.ORD_LOCALE AS VARCHAR) AS order_langid,
    CASE
        WHEN ord_line.ord_coupons IS NOT NULL THEN
            TRANSFORM(
                ord_line.ord_coupons,
                x -> OBJECT_CONSTRUCT(
                    'amount', x:amount,
                    'couponCode', x:couponCode,
                    'promoCode', x:promoCode,
                    'DiscountType', 'FIXED',
                    'promoGroup', x:promoGroup
                )
            )
        ELSE PARSE_JSON('[]')
    END AS coupons,
    CAST(ord_hdr.channel AS VARCHAR) AS channel,
    ord_hdr.CREATED_TS AS order_datetime,
    CAST(CASE WHEN ord_line.is_base_shipping_charged = 1 THEN 'true' ELSE 'false' END AS VARCHAR) AS baseshippingcharged,
    CAST(ord_line.pkg_cnt AS VARCHAR) AS shippableconsignmentcount,
    CAST(NULL AS VARCHAR) AS referralsite,
    ABS(COALESCE(ord_line.ol_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.ol_ord_shipping_amt, 0)) AS oh_base_shipping_amount,
    ord_hdr.CCY_CD AS oh_currency,
    ABS(COALESCE(ord_line.ol_cnlled_ord_coupon_amt, 0) + COALESCE(ord_line.ol_ord_coupon_amt, 0)) AS oh_couponamount,
    CAST(NULL AS INT) AS oh_giftboxamount,
    CAST(NULL AS INT) AS oh_giftboxtaxamount,
    ABS(COALESCE(ord_line.ol_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.ol_ord_shipping_tax_amt, 0)) AS oh_baseshippingtaxamount,
    CAST(NULL AS INT) AS oh_settledamount,
    ABS(COALESCE(ord_line.ol_cnlled_total_disc, 0) + COALESCE(ord_line.ol_total_disc, 0)) AS oh_discounted_amount,
    CASE
        WHEN 
			ABS(COALESCE(ord_line.ol_ord_ln_total, 0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(COALESCE(ord_line.ol_gift_card_value,0))
        ELSE 
			(ABS(COALESCE(ord_line.ol_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.ol_ord_ln_sub_total, 0)) + 
			ABS(COALESCE(ord_line.ol_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.ol_ord_shipping_amt, 0)) +
			ABS(COALESCE(ord_line.ol_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.ol_ord_shipping_tax_amt, 0)) +
			ABS(COALESCE(ord_line.ol_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.ol_ord_sales_tax_amt, 0))) -
			ABS(COALESCE(ord_line.ol_cnlled_total_disc, 0) + COALESCE(ord_line.ol_total_disc, 0))
    END AS oh_discounted_total_amount,
    CASE WHEN pymt.PYMT_GATEWAY_ID = 'FootLockerPaymentGateway' THEN 'INTERNAL' ELSE pymt.PYMT_GATEWAY_ID END AS oh_gateway,
    ABS(COALESCE(ord_line.ol_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.ol_ord_shipping_amt, 0)) + ABS(COALESCE(ord_line.ol_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.ol_ord_shipping_tax_amt, 0)) AS oh_shipping_amount,
    CASE
        WHEN 
			ABS(COALESCE(ord_line.ol_ord_ln_sub_total, 0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(COALESCE(ord_line.ol_gift_card_value,0))
        ELSE 
			ABS(COALESCE(ord_line.ol_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.ol_ord_ln_sub_total, 0))
    END AS oh_subtotal_amount,
    ABS(COALESCE(ord_line.ol_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.ol_ord_sales_tax_amt, 0)) AS oh_tax_amount,
    CASE
        WHEN 
			ABS(COALESCE(ord_line.ol_ord_ln_total, 0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(COALESCE(ord_line.ol_gift_card_value,0))
        ELSE 
			ABS(COALESCE(ord_line.ol_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.ol_ord_ln_sub_total, 0)) + 
			ABS(COALESCE(ord_line.ol_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.ol_ord_shipping_amt, 0)) +
			ABS(COALESCE(ord_line.ol_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.ol_ord_shipping_tax_amt, 0)) +
			ABS(COALESCE(ord_line.ol_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.ol_ord_sales_tax_amt, 0))
    END AS oh_total_amount,
    ord_hdr.CUST_EMAIL AS email,
    ord_line.ship_to_addr_first_name AS first_name,
    ord_line.ship_to_addr_last_name AS last_name,
    LOWER(ord_hdr.cust_id) AS user_id,
    LOWER(ord_hdr.cust_type_id) AS user_type,
    CAST(NULL AS VARCHAR) AS controllercustomerid,
    CAST(NULL AS VARCHAR) AS relatecustomerid,
    ord_hdr.FLX_ID AS flxid,
    ord_line.ship_to_addr_phone AS order_phonenumber,
    CAST(NULL AS VARCHAR) AS order_affiliateidtime,
    CAST(NULL AS VARCHAR) AS order_affiliate_id,
    CAST(ord_hdr.prnt_resrv_req_id AS VARCHAR) AS order_flrequest_id,
    CAST(ord_hdr.prnt_resrv_req_id AS VARCHAR) AS order_request_id,
    ord_hdr.captured_ts::timestamp_ntz AS order_request_date,
    CAST(NULL AS VARCHAR) AS order_request_type,
    CAST(ord_hdr.cust_id AS VARCHAR) AS order_requester,
    CAST(CASE WHEN ord_line.RUSH_FLG = 1 THEN 'true' ELSE 'false' END AS VARCHAR) AS rush_flag,
    ord_hdr.ASSOCIATE_NUM AS sales_personid,
    LOWER(ord_line.cart_shpmnt_method) AS ship_method,
    ord_line.shpmnt_method AS ship_method_desc,
    CAST(ord_hdr.browser_ip AS VARCHAR) AS user_ip_address,
    ord_hdr.VENDOR_ID AS vendorid,
    ord_hdr.ord_id AS web_order_number,
    CAST(NULL AS VARCHAR) AS migratedorder,
    ord_line.appeasment_reason_cd AS order_overridereasoncd,
    CASE
        WHEN LOWER(ord_hdr.ord_type_id) in ('web','return') THEN 'CAS'
        WHEN LOWER(ord_hdr.ord_type_id) = 'callcenter' THEN 'CUSTOMER_SVC'
        WHEN LOWER(ord_hdr.ord_type_id) = 'savethesale' THEN 'XSTORE'
        WHEN LOWER(ord_hdr.ord_type_id) = 'launch' THEN 'LAUNCH_RESERVATION'
        ELSE UPPER(ord_hdr.ord_type_id)
    END as order_source,
    ord_line.RELATE_ORD_NUM_CSA AS relatedordernumber_csa,
    CAST(NULL AS VARCHAR) AS controllerordernumber,
    ord_line.ship_to_addr_addr1 AS shipping_addressline1,
    ord_line.ship_to_addr_addr2 AS shipping_addressline2,
    ord_line.ship_to_addr_city AS shipping_city,
    ord_line.ship_to_addr_country AS shipping_country,
    ord_line.ship_to_addr_state AS shipping_state,
    ord_line.ship_to_addr_postal_cd AS shipping_postal_code,
    ord_line.ship_to_addr_email AS shipping_email,
    ord_line.ship_to_addr_first_name AS shipping_first_name,
    ord_line.ship_to_addr_last_name AS shipping_last_name,
    ord_line.ship_to_addr_phone AS shipping_phonenumber,
    CAST(CASE WHEN ord_hdr.channel = 'customer_service' THEN ord_hdr.associate_num ELSE NULL END AS VARCHAR) AS csaagentid,
    CAST(ord_hdr.csa_ord_note AS VARCHAR) AS csaordernote,
    CAST(NULL AS VARCHAR) AS order_division,
    CAST(NULL AS VARCHAR) AS omsorderid,
    CAST(ord_line.UPDATED_BY AS VARCHAR) AS postedby,
    ord_hdr.UPDATED_TS AS postedat,
    CAST(ord_hdr.UPDATED_TS AS TIMESTAMP) AS load_time_kafka,
    CAST(ord_hdr.etl_updt_ts AS TIMESTAMP) AS load_time_adls,
    CAST(CASE WHEN ord_line.IS_APPEASEMENT = 1 THEN 'true' END AS VARCHAR) AS appeasementorder,
    CAST(CASE WHEN ord_hdr.ord_total = 0 AND ord_hdr.ord_type_id = 'CallCenter' AND ord_line.is_gift_card != 1 THEN 'true' ELSE 'false' END AS VARCHAR) AS nochargeorder,
    TRY_PARSE_JSON(UPPER(ord_hdr.pymt_type)) AS payment_type,
    CAST(CASE WHEN ord_line.cnl_reason_id = 'FRAUD' THEN 'false' ELSE 'true' END AS BOOLEAN) AS obforder,
    pymt_line.paymentsinfo AS payment,
    OBJECT_CONSTRUCT(
        'addressLine1', loc.loc_addr1,
        'addressLine2', loc.loc_addr2,
        'city', loc.loc_addr_city,
        'companyName', loc.PRNT_ORG_ID,
        'country', loc.loc_addr_country,
        'countryCode', loc.loc_addr_country,
        'email', loc.loc_addr_email,
        'firstName', loc.loc_addr_first_name,
        'lastName', loc.loc_addr_last_name,
        'phoneNumber', loc.loc_addr_phone_no,
        'postalCode', loc.loc_addr_postal_cd,
        'state', loc.loc_addr_state
    ) AS storeaddress,
    CAST(CASE ord_line.is_loyalty_disc
        WHEN 1 THEN 'true'
        ELSE NULL
    END AS VARCHAR) AS loyaltydiscount,
    CAST(NULL AS VARCHAR) AS loyaltypointsredeemed,
    ord_line.loyalty_reward_id AS loyaltyrewardid,
    CAST(NULL AS VARCHAR) AS loyaltyredemptionid,
    'MAO' AS ref_source
FROM fct_mao_ord_hdr ord_hdr
JOIN fct_mao_ord_line ord_line
    ON ord_line.ORG_ID = ord_hdr.ORG_ID AND ord_line.ORD_ID = ord_hdr.ORD_ID
LEFT JOIN fct_mao_ord_pymt_line pymt
    ON pymt.ORG_ID = ord_hdr.ORG_ID AND pymt.ORD_ID = ord_hdr.ORD_ID
LEFT JOIN payment_grouped pymt_line
    ON ord_hdr.ORG_ID = pymt_line.ORG_ID AND ord_hdr.ord_id = pymt_line.ord_id
LEFT JOIN {{ source('dom_gold', 'dim_mao_employee_v') }} emp
    ON ord_line.created_by = emp.user_id
LEFT JOIN dim_location dim_loc
    ON LPAD(dim_loc.loc_snum, 5, '0') = LPAD(COALESCE(ord_line.ship_from_loc_id, ord_line.physical_org_id), 5, '0')
LEFT JOIN {{ source('dom_gold', 'dim_mao_loc_v') }} loc
    ON COALESCE(ord_line.ship_from_loc_id, ord_line.physical_org_id) = loc.loc_id
LEFT JOIN fct_exchange_orders exch
    ON ord_hdr.ORG_ID = exch.ORG_ID AND ord_hdr.ORD_ID = exch.PRNT_ORD_ID
WHERE ord_hdr.ord_status_rnk = 1
)
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(company_number AS VARCHAR) AS company_number,
    CAST(order_status AS VARCHAR) AS order_status,
    CAST(cancel_code AS VARCHAR) AS cancel_code,
    CAST(cancelreason AS VARCHAR) AS cancelreason,
    CAST(exchangeorder_flag AS VARCHAR) AS exchangeorder_flag,
    CAST(billing_address_line1 AS VARCHAR) AS billing_address_line1,
    CAST(billing_address_line2 AS VARCHAR) AS billing_address_line2,
    CAST(billing_city AS VARCHAR) AS billing_city,
    CAST(billing_country AS VARCHAR) AS billing_country,
    CAST(billing_email AS VARCHAR) AS billing_email,
    CAST(billing_first_name AS VARCHAR) AS billing_first_name,
    CAST(billing_last_name AS VARCHAR) AS billing_last_name,
    CAST(billing_postal_code AS VARCHAR) AS billing_postal_code,
    CAST(billing_postal_state AS VARCHAR) AS billing_postal_state,
    CAST(billing_phonenumber AS VARCHAR) AS billing_phonenumber,
    CAST(order_giftbox_flag AS VARCHAR) AS order_giftbox_flag,
    CAST(order_giftorder_flag AS VARCHAR) AS order_giftorder_flag,
    CAST(order_langid AS VARCHAR) AS order_langid,
    coupons AS coupons,
    CAST(channel AS VARCHAR) AS channel,
    CAST(order_datetime AS TIMESTAMP) AS order_datetime,
    CAST(baseshippingcharged AS VARCHAR) AS baseshippingcharged,
    CAST(shippableconsignmentcount AS VARCHAR) AS shippableconsignmentcount,
    CAST(referralsite AS VARCHAR) AS referralsite,
    CAST(oh_base_shipping_amount AS DECIMAL(15,4)) AS oh_base_shipping_amount,
    CAST(oh_currency AS VARCHAR) AS oh_currency,
    CAST(oh_couponamount AS DECIMAL(15,4)) AS oh_couponamount,
    CAST(oh_giftboxamount AS DECIMAL(15,4)) AS oh_giftboxamount,
    CAST(oh_giftboxtaxamount AS DECIMAL(15,4)) AS oh_giftboxtaxamount,
    CAST(oh_baseshippingtaxamount AS DECIMAL(15,4)) AS oh_baseshippingtaxamount,
    CAST(oh_settledamount AS DECIMAL(15,4)) AS oh_settledamount,
    CAST(oh_discounted_amount AS DECIMAL(15,4)) AS oh_discounted_amount,
    CAST(oh_discounted_total_amount AS DECIMAL(15,4)) AS oh_discounted_total_amount,
    CAST(oh_gateway AS VARCHAR) AS oh_gateway,
    CAST(oh_shipping_amount AS DECIMAL(15,4)) AS oh_shipping_amount,
    CAST(oh_subtotal_amount AS DECIMAL(15,4)) AS oh_subtotal_amount,
    CAST(oh_tax_amount AS DECIMAL(15,4)) AS oh_tax_amount,
    CAST(oh_total_amount AS DECIMAL(15,4)) AS oh_total_amount,
    CAST(email AS VARCHAR) AS email,
    CAST(first_name AS VARCHAR) AS first_name,
    CAST(last_name AS VARCHAR) AS last_name,
    CAST(user_id AS VARCHAR) AS user_id,
    CAST(user_type AS VARCHAR) AS user_type,
    CAST(controllercustomerid AS VARCHAR) AS controllercustomerid,
    CAST(relatecustomerid AS VARCHAR) AS relatecustomerid,
    CAST(flxid AS VARCHAR) AS flxid,
    CAST(order_phonenumber AS VARCHAR) AS order_phonenumber,
    CAST(order_affiliateidtime AS VARCHAR) AS order_affiliateidtime,
    CAST(order_affiliate_id AS VARCHAR) AS order_affiliate_id,
    CAST(order_flrequest_id AS VARCHAR) AS order_flrequest_id,
    CAST(order_request_id AS VARCHAR) AS order_request_id,
    CAST(order_request_date AS VARCHAR) AS order_request_date,
    CAST(order_request_type AS VARCHAR) AS order_request_type,
    CAST(order_requester AS VARCHAR) AS order_requester,
    CAST(rush_flag AS VARCHAR) AS rush_flag,
    CAST(sales_personid AS VARCHAR) AS sales_personid,
    CAST(ship_method AS VARCHAR) AS ship_method,
    CAST(ship_method_desc AS VARCHAR) AS ship_method_desc,
    CAST(user_ip_address AS VARCHAR) AS user_ip_address,
    CAST(vendorid AS VARCHAR) AS vendorid,
    CAST(web_order_number AS VARCHAR) AS web_order_number,
    CAST(migratedorder AS VARCHAR) AS migratedorder,
    CAST(order_overridereasoncd AS VARCHAR) AS order_overridereasoncd,
    CAST(order_source AS VARCHAR) AS order_source,
    CAST(relatedordernumber_csa AS VARCHAR) AS relatedordernumber_csa,
    CAST(controllerordernumber AS VARCHAR) AS controllerordernumber,
    CAST(shipping_addressline1 AS VARCHAR) AS shipping_addressline1,
    CAST(shipping_addressline2 AS VARCHAR) AS shipping_addressline2,
    CAST(shipping_city AS VARCHAR) AS shipping_city,
    CAST(shipping_country AS VARCHAR) AS shipping_country,
    CAST(shipping_state AS VARCHAR) AS shipping_state,
    CAST(shipping_postal_code AS VARCHAR) AS shipping_postal_code,
    CAST(shipping_email AS VARCHAR) AS shipping_email,
    CAST(shipping_first_name AS VARCHAR) AS shipping_first_name,
    CAST(shipping_last_name AS VARCHAR) AS shipping_last_name,
    CAST(shipping_phonenumber AS VARCHAR) AS shipping_phonenumber,
    CAST(csaagentid AS VARCHAR) AS csaagentid,
    CAST(csaordernote AS VARCHAR) AS csaordernote,
    CAST(order_division AS VARCHAR) AS order_division,
    CAST(omsorderid AS VARCHAR) AS omsorderid,
    CAST(postedby AS VARCHAR) AS postedby,
    CAST(postedat AS TIMESTAMP) AS postedat,
    CAST(load_time_kafka AS TIMESTAMP) AS load_time_kafka,
    CAST(load_time_adls AS TIMESTAMP) AS load_time_adls,
    CAST(appeasementorder AS VARCHAR) AS appeasementorder,
    CAST(nochargeorder AS VARCHAR) AS nochargeorder,
    payment_type AS payment_type,
    CAST(obforder AS BOOLEAN) AS obforder,
    payment AS payment,
    storeaddress AS storeaddress,
    CAST(loyaltydiscount AS VARCHAR) AS loyaltydiscount,
    CAST(loyaltypointsredeemed AS VARCHAR) AS loyaltypointsredeemed,
    CAST(loyaltyrewardid AS VARCHAR) AS loyaltyrewardid,
    CAST(loyaltyredemptionid AS VARCHAR) AS loyaltyredemptionid,
    CAST(ref_source AS VARCHAR(50)) AS ref_source
FROM order_header
