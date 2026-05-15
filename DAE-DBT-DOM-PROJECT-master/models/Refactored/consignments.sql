{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}

{{ config(
    materialized="incremental",
    unique_key=["company_number", "order_id", "consignment_id", "consignment_status"],
    post_hook=["{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'load_time_adls' ) }}"],
    meta={'strategy': "merge"}
) }}


WITH
product_master AS (
    SELECT
        pm.internal_product_number_flca,
        pm.internal_product_number,
        pm.legacy_size_desc,
        pm.online_us_sku,
        pm.online_ca_sku,
        pm.global_size_id,
        pm.banner_id
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
payment AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY org_id, ord_id, pymt_method_id, pymt_txn_id
            ORDER BY NVL(PYMT_TXN_DETAIL_ID, '') DESC
        ) AS rnk
    FROM {{ source('dom_gold', 'fct_mao_ord_pymt_line_v') }}
),
payment_grouped AS (
    SELECT
        org_id,
        ord_id,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'amount', CAST(pymt_method_amt AS STRING),
                'authCode', CAST(auth_cd AS STRING),
                'cardLast4', CAST(txn_card_last4 AS STRING),
                'cegrRefId', CAST(inv_id AS STRING),
                'paymentTransactionId', CAST(PYMT_TXN_ID AS STRING),
                'paymentTransactionSubType', NULL,
                'paymentTransactionType', CAST(PYMT_TXN_TYPE AS STRING),
                'paymentType', CAST(CASE WHEN pymt_type = 'Gift Card' THEN 'GIFTCARD' WHEN pymt_type = 'Credit Card' THEN 'CREDITCARD' ELSE UPPER(pymt_type) END AS STRING),
                'creditCardType', CAST(PYMT_CARD_TYPE AS STRING),
                'date', CAST(COALESCE(PYMT_TXN_DT, pymt_txn_req_dt) AS STRING),
                'shippingTaxAmount', NULL,
                'taxAmount', NULL,
                'authorization', OBJECT_CONSTRUCT(
                    'attributes', OBJECT_CONSTRUCT(
                        'authResponse', CAST(pymt_txn_status_desc AS STRING),
                        'avsCode', CAST(attrib_avs_code AS STRING),
                        'cardAlias', CAST(card_alias AS STRING),
                        'cardBin', CAST(pymt_grp_id AS STRING),
                        'cardLast4', CAST(attrib_card_last4 AS STRING),
                        'cardToken', CAST(card_token AS STRING),
                        'cardType', CAST(attrib_card_type_display AS STRING),
                        'confirmationCode', NULL,
                        'cvvResponse', CAST(attrib_cvv_response AS STRING),
                        'email', CAST(addr_email AS STRING),
                        'expirationDate', CAST(attrib_card_expiry_dt AS STRING),
                        'giftCardNumber', CAST(CASE WHEN pymt_type = 'Gift Card' THEN attrib_card_last4 END AS STRING),
                        'sellerProtection', CAST(CASE WHEN pymt_type = 'Gift Card' THEN SELLER_PROTECTION_STATUS END AS STRING)
                    ),
                    'authAmount', CAST(pymt_txn_req_amt AS STRING),
                    'authCode', CAST(auth_cd AS STRING),
                    'errorMessage', NULL,
                    'id', NULL,
                    'originalOrderNumber', CAST(ord_id AS STRING),
                    'paymentType', CAST(pymt_type AS STRING),
                    'preSettled', NULL,
                    'transactionDate', CAST(COALESCE(PYMT_TXN_DT, pymt_txn_req_dt) AS STRING),
                    'transactionId', CAST(pymt_txn_id AS STRING)
                ),
                'creditCard', CASE WHEN pymt_type = 'Credit Card' THEN
                    OBJECT_CONSTRUCT(
                        'authInfo', OBJECT_CONSTRUCT(
                            'authAmount', CAST(pymt_txn_req_amt AS STRING),
                            'authCode', CAST(auth_cd AS STRING),
                            'authResponse', CAST(pymt_txn_status_desc AS STRING),
                            'authTime', CAST(pymt_txn_req_dt AS STRING),
                            'avsCode', CAST(attrib_avs_code AS STRING),
                            'cvvResponse', CAST(attrib_cvv_response AS STRING),
                            'originalOrderNumber', CAST(ord_id AS STRING),
                            'preSettled', NULL,
                            'referenceNumber', CAST(transaction_ref_id AS STRING),
                            'transactionId', CAST(pymt_txn_id AS STRING)
                        ),
                        'cardAlias', CAST(card_alias AS STRING),
                        'cardBin', CAST(pymt_grp_id AS STRING),
                        'cardLast4', CAST(txn_card_last4 AS STRING),
                        'cardToken', CAST(card_token AS STRING),
                        'expirationDate', CAST(attrib_card_expiry_dt AS STRING),
                        'type', CAST(attrib_card_type_display AS STRING)
                    )
                ELSE NULL END,
                'giftCard', CASE WHEN pymt_type = 'Gift Card' THEN
                    OBJECT_CONSTRUCT(
                        'amount', CAST(pymt_method_amt AS STRING),
                        'authCode', CAST(auth_cd AS STRING),
                        'giftCardNumber', CAST(attrib_card_last4 AS STRING),
                        'originalOrderNumber', CAST(ord_id AS STRING),
                        'preSettled', NULL,
                        'transactionDate', CAST(COALESCE(PYMT_TXN_DT, pymt_txn_req_dt) AS STRING),
                        'transactionId', CAST(pymt_txn_id AS STRING)
                    )
                ELSE NULL END,
                'paypal', CASE WHEN pymt_type = 'PayPal' THEN
                    OBJECT_CONSTRUCT(
                        'amount', CAST(pymt_method_amt AS STRING),
                        'authInfo', OBJECT_CONSTRUCT(
                            'authAmount', CAST(crnt_auth_amt AS STRING),
                            'authCode', CAST(auth_cd AS STRING),
                            'authResponse', CAST(pymt_txn_status_desc AS STRING),
                            'authTime', CAST(pymt_txn_req_dt AS STRING),
                            'avsCode', CAST(attrib_avs_code AS STRING),
                            'cvvResponse', CAST(attrib_cvv_response AS STRING),
                            'originalOrderNumber', CAST(ord_id AS STRING),
                            'preSettled', NULL,
                            'referenceNumber', CAST(transaction_ref_id AS STRING),
                            'transactionId', CAST(pymt_txn_id AS STRING)
                        ),
                        'paypalEmailId', CAST(addr_email AS STRING),
                        'transactionDate', CAST(COALESCE(PYMT_TXN_DT, pymt_txn_req_dt) AS STRING),
                        'transactionId', CAST(pymt_txn_id AS STRING)
                    )
                ELSE NULL END
            )
        ) AS payments_info,
        ARRAY_AGG(pymt_txn_type) AS pymt_txn_types
    FROM payment
    WHERE rnk = 1
    GROUP BY org_id, ord_id
),
pymt_txn_status AS (
    SELECT *
    FROM (
        SELECT
            org_id,
            ord_id,
            pymt_txn_status_desc,
            ROW_NUMBER() OVER (PARTITION BY org_id, ord_id ORDER BY src_load_ts DESC) AS rnk
        FROM {{ source('dom_gold', 'fct_mao_ord_pymt_line_v') }}
    )
    WHERE rnk = 1
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
bill_address_dtl AS (
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
            ROW_NUMBER() OVER (
                PARTITION BY org_id, ord_id
                ORDER BY src_load_ts DESC
            ) AS rnk
        FROM {{ source('dom_gold', 'fct_mao_ord_pymt_line_v') }}
        WHERE ord_id IS NOT NULL
    )
    WHERE rnk = 1
),
mao_ord_fulfillment_detail AS (
    SELECT *
    FROM (
        SELECT
            fd.*,
            ROW_NUMBER() OVER (
                PARTITION BY fd.org_id, fd.ord_id, fd.ord_ln_id, fd.rel_id, fd.rel_ln_id
                ORDER BY fd.updated_ts DESC
            ) AS ful_det_rnk
        FROM {{ source('dom_gold', 'fct_mao_ord_fulfillment_dtl_v') }} fd
    )
    WHERE ful_det_rnk = 1
        {% if is_incremental() %}
            AND etl_updt_ts >= {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL_DBIMART") | as_text }}'
        {% endif %}
),
fct_mao_ord_line_fv AS (
    SELECT
        ord_line.org_id,
        ord_line.ord_id,
        ord_line.ord_ln_id,
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
    FROM {{ source('dom_gold', 'fct_mao_ord_line_hist_v') }} ord_line
),
fct_mao_ord_line AS (
    SELECT
        oh.doc_type_id,
        oh.ccy_cd,
        oh.ord_locale,
        oh.cust_id,
        oh.flx_id,
        oh.pymt_status_id,
        oh.ord_total,
        oh.confirmed_ts,
        oh.captured_ts,
        oh.created_ts AS hdr_created_ts,
        ol.org_id,
        ol.ord_id,
        ol.ord_ln_id,
        ol.item_id,
        ol.qty,
        ol.unit_price,
        ol.orig_unit_price,
        ol.ord_ln_total,
        ol.ord_ln_sub_total,
        ol.total_disc,
        ol.total_disc_on_item,
        ol.total_taxes,
        ol.ord_shipping_amt,
        ol.ord_shipping_tax_amt,
        ol.ord_sales_tax_amt,
        ol.total_charges,
        ol.cnlled_total_disc,
        ol.cnlled_ord_ln_total,
        ol.cnlled_ord_shipping_amt,
        ol.cnlled_ord_shipping_tax_amt,
        ol.cnlled_ord_ln_sub_total,
        ol.cnlled_total_taxes,
        ol.cnlled_total_charges,
        ol.cnlled_ord_sales_tax_amt,
        ol.max_fulflmnt_status_id,
        ol.max_fulflmnt_status_desc,
        ol.cnl_reason_id,
        ol.cnl_reason_desc,
        ol.product_type,
        ol.is_gift_card,
        ol.is_pre_sale,
        ol.is_base_shipping_charged,
        ol.is_even_exchg,
        ol.prnt_ord_id,
        ol.physical_org_id,
        ol.ship_from_loc_id,
        ol.ship_to_addr_first_name,
        ol.ship_to_addr_last_name,
        ol.ship_to_addr_email,
        ol.ship_to_addr_phone,
        ol.ship_to_addr_addr1,
        ol.ship_to_addr_addr2,
        ol.ship_to_addr_city,
        ol.ship_to_addr_country,
        ol.ship_to_addr_state,
        ol.ship_to_addr_postal_cd,
        ol.cart_shpmnt_method,
        ol.shpmnt_method,
        ol.itm_size,
        ol.ord_note,
        ol.dlvry_method_id,
        ol.created_ts,
        ol.created_by,
        ol.updated_ts,
        ol.updated_by,
        ol.etl_updt_ts,
		fv.fv_orig_unit_price,
		fv.fv_unit_price,
        fv.fv_cnlled_total_disc,
        fv.fv_total_disc,
        fv.fv_cnlled_ord_ln_total,
        fv.fv_ord_ln_total,
        fv.fv_cnlled_total_charges,
        fv.fv_total_charges,
		fv.fv_cnlled_total_taxes,
		fv.fv_total_taxes,
		fv.fv_cnlled_ord_shipping_amt,
		fv.fv_ord_shipping_amt,
        fv.fv_cnlled_ord_shipping_tax_amt,
        fv.fv_ord_shipping_tax_amt,
        fv.fv_cnlled_ord_ln_sub_total,
        fv.fv_ord_ln_sub_total,
        fv.fv_cnlled_ord_sales_tax_amt,
        fv.fv_ord_sales_tax_amt,
        ROW_NUMBER() OVER (
            PARTITION BY ol.org_id, ol.ord_id, ol.ord_ln_id
            ORDER BY ol.updated_ts DESC
        ) AS ord_ln_rnk
    FROM {{ source('dom_gold', 'fct_mao_ord_line_hist_v') }} ol
    JOIN {{ source('dom_gold', 'fct_mao_ord_hdr_v') }} oh
        ON oh.org_id = ol.org_id AND oh.ord_id = ol.ord_id
    LEFT JOIN fct_mao_ord_line_fv fv
        ON ol.org_id = fv.org_id AND ol.ord_id = fv.ord_id AND ol.ord_ln_id = fv.ord_ln_id AND fv.fv_rnk = 1
    WHERE oh.doc_type_id = 'CustomerOrder'
        AND NOT (ol.max_fulflmnt_status_id IS NULL OR ol.max_fulflmnt_status_id > 9000)
        AND ol.prnt_ord_id IS NULL
        AND ol.is_even_exchg = 0
        AND ol.max_fulflmnt_status_id NOT IN ('8000', '8500')
),
fct_mao_ful_line AS (
    SELECT
        ol.doc_type_id,
        ol.ccy_cd,
        ol.ord_locale,
        ol.cust_id,
        ol.flx_id,
        ol.pymt_status_id,
        ol.ord_total,
        ol.confirmed_ts,
        ol.captured_ts,
        ol.hdr_created_ts,
        ol.is_base_shipping_charged,
        ol.ord_note,
        ol.total_disc_on_item,
        ol.total_disc,
        ol.orig_unit_price,
        ol.ord_shipping_amt,
        ol.ord_shipping_tax_amt,
        ol.ord_ln_sub_total,
        ol.total_taxes,
        ol.cnlled_total_disc,
        ol.cnlled_ord_ln_total,
        ol.cnlled_ord_shipping_amt,
        ol.cnlled_ord_shipping_tax_amt,
        ol.cnlled_ord_ln_sub_total,
        ol.cnlled_total_taxes,
        ol.physical_org_id,
        ol.ord_ln_total,
        ol.unit_price,
        ol.product_type,
        ol.qty,
        ol.is_gift_card,
        ol.is_pre_sale,
        ol.ship_to_addr_first_name,
        ol.ship_to_addr_last_name,
        ol.ship_to_addr_email,
        ol.ship_to_addr_phone,
        ol.ship_to_addr_addr1,
        ol.ship_to_addr_addr2,
        ol.ship_to_addr_city,
        ol.ship_to_addr_country,
        ol.ship_to_addr_state,
        ol.ship_to_addr_postal_cd,
        ol.cart_shpmnt_method,
        ol.shpmnt_method,
        ol.itm_size,
        ol.total_charges,
        ol.cnlled_total_charges,
        ol.ord_sales_tax_amt,
        ol.cnlled_ord_sales_tax_amt,
		ol.fv_orig_unit_price,
		ol.fv_unit_price,
        ol.fv_cnlled_total_disc,
        ol.fv_total_disc,
        ol.fv_cnlled_ord_ln_total,
        ol.fv_ord_ln_total,
        ol.fv_cnlled_total_charges,
        ol.fv_total_charges,
		ol.fv_cnlled_total_taxes,
		ol.fv_total_taxes,
		ol.fv_cnlled_ord_shipping_amt,
		ol.fv_ord_shipping_amt,
        ol.fv_cnlled_ord_shipping_tax_amt,
        ol.fv_ord_shipping_tax_amt,
        ol.fv_cnlled_ord_ln_sub_total,
        ol.fv_ord_ln_sub_total,
        ol.fv_cnlled_ord_sales_tax_amt,
        ol.fv_ord_sales_tax_amt,
        fl.org_id AS fl_org_id,
        fl.fulflmnt_id,
        fl.fulflmnt_ln_id,
        fl.ord_id,
        fl.ord_ln_id,
        fl.rel_id,
        fl.rel_ln_id,
        fl.item_id,
        fl.fulflmnt_ln_status_id,
        fl.fulflmnt_ln_status_desc,
        fl.cnl_reason_id,
        fl.cnl_reason_desc,
        fl.item_unit_price,
        fl.odrd_qty,
        fl.picked_qty,
        fl.pked_qty,
        fl.shipped_qty,
        fl.cnlled_qty,
        ol.hdr_created_ts AS created_ts,
        fl.created_by,
        fl.updated_ts,
        fl.updated_by,
        fl.etl_updt_ts,
        fl.short_reason_id,
        fl.rejected_flg
    FROM {{ source('dom_gold', 'fct_mao_fulfillment_line_hist_v') }} fl
    JOIN {{ source('dom_gold', 'fct_mao_fulfillment_hdr_v') }} fh
        ON fh.org_id = fl.org_id AND fh.fulflmnt_id = fl.fulflmnt_id
    JOIN fct_mao_ord_line ol
        ON fl.ord_id = ol.ord_id AND fl.ord_ln_id = ol.ord_ln_id AND ol.ord_ln_rnk = 1       
),
store_fulflmnts AS (
    SELECT rel_id, rel_ln_id
    FROM fct_mao_ful_line
    GROUP BY all
),
mao_consignments AS (
    SELECT
        'DCFulfillment' AS fulflmnt_type,
        fd.org_id,
        fd.fulflmnt_dtl_pk,
        fd.fulflmnt_dtl_id,
        fd.ord_id,
        fd.ord_ln_id,
        fd.rel_id AS fulflmnt_id,
        fd.rel_ln_id AS fulflmnt_ln_id,
        fd.rel_id,
        fd.rel_ln_id,
        fd.shipment_id,
        fd.item_id,
        fd.pkg_id,
        fd.pkg_dtl_id,
        fd.inv_id,
        fd.dlvry_method_id,
        fd.dlvry_method_sub_type,
        fd.ship_via_id,
        fd.gift_card_no,
        fd.gift_card_pin,
        fd.gift_card_value,
        fd.carrier_cd,
        fd.tracking_num,
        fd.serial_num,
        fd.sgtin,
        fd.channel,
        ol.max_fulflmnt_status_id AS fulflmnt_ln_status_id,
        ol.max_fulflmnt_status_desc AS fulflmnt_ln_status_desc,
        ol.cnl_reason_id,
        ol.cnl_reason_desc,
        ol.unit_price AS item_unit_price,
        fd.ord_qty AS odrd_qty,
        CAST(NULL AS NUMBER) AS picked_qty,
        CAST(NULL AS NUMBER) AS pked_qty,
        CAST(NULL AS NUMBER) AS shipped_qty,
        fd.cnl_qty AS cnlled_qty,
        fd.fulfld_qty,
        fd.fulflmnt_dt,
        fd.shpd_dt,
        ol.hdr_created_ts AS created_ts,
        ol.created_by,
        ol.updated_ts,
        ol.updated_by,
        ol.is_base_shipping_charged,
        ol.ord_note,
        ol.ccy_cd,
        ol.total_disc_on_item,
        ol.total_disc,
        ol.orig_unit_price,
        ol.ord_shipping_amt,
        ol.ord_shipping_tax_amt,
        ol.ord_ln_sub_total,
        ol.total_taxes,
        ol.cnlled_total_disc,
        ol.cnlled_ord_ln_total,
        ol.cnlled_ord_shipping_amt,
        ol.cnlled_ord_shipping_tax_amt,
        ol.cnlled_ord_ln_sub_total,
        ol.cnlled_total_taxes,
        ol.ord_ln_total,
        ol.unit_price,
        CONCAT(fd.rel_id, fd.rel_ln_id) AS consignment_id,
        ol.product_type,
        ol.qty,
        ol.is_gift_card,
        ol.is_pre_sale,
        ol.physical_org_id,
        fd.ship_from_loc_id,
        ol.doc_type_id,
        ol.confirmed_ts,
        ol.captured_ts,
        fd.etl_updt_ts,
        ol.ship_to_addr_first_name,
        ol.ship_to_addr_last_name,
        ol.ship_to_addr_email,
        ol.ship_to_addr_phone,
        ol.ship_to_addr_addr1,
        ol.ship_to_addr_addr2,
        ol.ship_to_addr_city,
        ol.ship_to_addr_country,
        ol.ship_to_addr_state,
        ol.ship_to_addr_postal_cd,
        ol.cust_id,
        ol.flx_id,
        ol.ord_total,
        ol.cart_shpmnt_method,
        ol.shpmnt_method,
        ol.ord_locale,
        ol.itm_size,
        fd.fulflmnt_dtl_id AS entryid,
        ol.pymt_status_id,
        ol.total_charges,
        ol.cnlled_total_charges,
        ol.ord_sales_tax_amt,
        ol.cnlled_ord_sales_tax_amt,
        fd.is_rejected,
        NULL::STRING AS short_reason_id,
        NULL::STRING AS rejected_flg,
        COALESCE(fd.rel_created_ts, fd.created_ts) AS rel_created_ts,
		ol.fv_orig_unit_price,
		ol.fv_unit_price,
        ol.fv_cnlled_total_disc,
        ol.fv_total_disc,
        ol.fv_cnlled_ord_ln_total,
        ol.fv_ord_ln_total,
        ol.fv_cnlled_total_charges,
        ol.fv_total_charges,
		ol.fv_cnlled_total_taxes,
		ol.fv_total_taxes,
		ol.fv_cnlled_ord_shipping_amt,
		ol.fv_ord_shipping_amt,
        ol.fv_cnlled_ord_shipping_tax_amt,
        ol.fv_ord_shipping_tax_amt,
        ol.fv_cnlled_ord_ln_sub_total,
        ol.fv_ord_ln_sub_total,
        ol.fv_cnlled_ord_sales_tax_amt,
        ol.fv_ord_sales_tax_amt
    FROM mao_ord_fulfillment_detail fd
    JOIN fct_mao_ord_line ol
        ON fd.org_id = ol.org_id AND fd.ord_id = ol.ord_id AND fd.ord_ln_id = ol.ord_ln_id
    WHERE NOT EXISTS (SELECT 'x' FROM store_fulflmnts sf WHERE fd.rel_id = sf.rel_id AND fd.rel_ln_id = sf.rel_ln_id)
    UNION ALL
    SELECT
        'StoreFulfillment' AS fulflmnt_type,
        fd.org_id,
        fd.fulflmnt_dtl_pk,
        fd.fulflmnt_dtl_id,
        fd.ord_id,
        fd.ord_ln_id,
        fd.rel_id AS fulflmnt_id,
        fd.rel_ln_id AS fulflmnt_ln_id,
        fd.rel_id,
        fd.rel_ln_id,
        fd.shipment_id,
        fd.item_id,
        fd.pkg_id,
        fd.pkg_dtl_id,
        fd.inv_id,
        fd.dlvry_method_id,
        fd.dlvry_method_sub_type,
        fd.ship_via_id,
        fd.gift_card_no,
        fd.gift_card_pin,
        fd.gift_card_value,
        fd.carrier_cd,
        fd.tracking_num,
        fd.serial_num,
        fd.sgtin,
        fd.channel,
        fl.fulflmnt_ln_status_id,
        fl.fulflmnt_ln_status_desc,
        fl.cnl_reason_id,
        fl.cnl_reason_desc,
        fl.item_unit_price,
        fl.odrd_qty,
        fl.picked_qty,
        fl.pked_qty,
        fl.shipped_qty,
        fl.cnlled_qty,
        fd.fulfld_qty,
        fd.fulflmnt_dt,
        fd.shpd_dt,
        fl.created_ts,
        fl.created_by,
        fl.updated_ts,
        fl.updated_by,
        fl.is_base_shipping_charged,
        fl.ord_note,
        fl.ccy_cd,
        fl.total_disc_on_item,
        fl.total_disc,
        fl.orig_unit_price,
        fl.ord_shipping_amt,
        fl.ord_shipping_tax_amt,
        fl.ord_ln_sub_total,
        fl.total_taxes,
        fl.cnlled_total_disc,
        fl.cnlled_ord_ln_total,
        fl.cnlled_ord_shipping_amt,
        fl.cnlled_ord_shipping_tax_amt,
        fl.cnlled_ord_ln_sub_total,
        fl.cnlled_total_taxes,
        fl.ord_ln_total,
        fl.unit_price,
        CONCAT(fd.rel_id, fd.rel_ln_id) AS consignment_id,
        fl.product_type,
        fl.qty,
        fl.is_gift_card,
        fl.is_pre_sale,
        fl.physical_org_id,
        fd.ship_from_loc_id,
        fl.doc_type_id,
        fl.confirmed_ts,
        fl.captured_ts,
        fd.etl_updt_ts,
        fl.ship_to_addr_first_name,
        fl.ship_to_addr_last_name,
        fl.ship_to_addr_email,
        fl.ship_to_addr_phone,
        fl.ship_to_addr_addr1,
        fl.ship_to_addr_addr2,
        fl.ship_to_addr_city,
        fl.ship_to_addr_country,
        fl.ship_to_addr_state,
        fl.ship_to_addr_postal_cd,
        fl.cust_id,
        fl.flx_id,
        fl.ord_total,
        fl.cart_shpmnt_method,
        fl.shpmnt_method,
        fl.ord_locale,
        fl.itm_size,
        fd.fulflmnt_dtl_id AS entryid,
        fl.pymt_status_id,
        fl.total_charges,
        fl.cnlled_total_charges,
        fl.ord_sales_tax_amt,
        fl.cnlled_ord_sales_tax_amt,
        fd.is_rejected,
        fl.short_reason_id,
        fl.rejected_flg,
        fl.created_ts AS rel_created_ts,
		fl.fv_orig_unit_price,
		fl.fv_unit_price,
        fl.fv_cnlled_total_disc,
        fl.fv_total_disc,
        fl.fv_cnlled_ord_ln_total,
        fl.fv_ord_ln_total,
        fl.fv_cnlled_total_charges,
        fl.fv_total_charges,
		fl.fv_cnlled_total_taxes,
		fl.fv_total_taxes,
		fl.fv_cnlled_ord_shipping_amt,
		fl.fv_ord_shipping_amt,
        fl.fv_cnlled_ord_shipping_tax_amt,
        fl.fv_ord_shipping_tax_amt,
        fl.fv_cnlled_ord_ln_sub_total,
        fl.fv_ord_ln_sub_total,
        fl.fv_cnlled_ord_sales_tax_amt,
        fl.fv_ord_sales_tax_amt
    FROM mao_ord_fulfillment_detail fd
    JOIN fct_mao_ful_line fl
        ON fd.rel_id = fl.rel_id AND fd.rel_ln_id = fl.rel_ln_id
),
consignments AS (
    SELECT
        c.*,
        CASE
            WHEN c.fulflmnt_type = 'DCFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id BETWEEN 1000 AND 2000 THEN 'CONSIGNMENT_PROCESSING_START'
                    WHEN c.fulflmnt_ln_status_id = 3000 THEN 'CONSIGNMENT_SUBMITTED'
                    WHEN c.fulflmnt_ln_status_id BETWEEN 3500 AND 3700 THEN 'CONSIGNMENT_PROCESSING'
                    WHEN c.fulflmnt_ln_status_id = 7000 THEN 'CONSIGNMENT_PROCESSING_END'
                    WHEN c.fulflmnt_ln_status_id = 9000 THEN 'CONSIGNMENT_CANCELLED'
                    ELSE UPPER(c.fulflmnt_ln_status_desc)
                END
            WHEN c.fulflmnt_type = 'StoreFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id = 1000 THEN 'CONSIGNMENT_PROCESSING_START'
                    WHEN c.fulflmnt_ln_status_id = 2000 THEN 'CONSIGNMENT_SUBMITTED'
                    WHEN c.fulflmnt_ln_status_id BETWEEN 3000 AND 4000 THEN 'CONSIGNMENT_PROCESSING'
                    WHEN c.fulflmnt_ln_status_id BETWEEN 4500 AND 6000 THEN 'CONSIGNMENT_PROCESSING_END'
                    WHEN c.fulflmnt_ln_status_id = 9000 THEN 'CONSIGNMENT_CANCELLED'
                    ELSE UPPER(c.fulflmnt_ln_status_desc)
                END
            ELSE UPPER(c.fulflmnt_ln_status_desc)
        END AS consignment_status,
        CASE
            WHEN c.fulflmnt_type = 'DCFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id BETWEEN 1000 AND 2000 THEN 'CREATED'
                    WHEN c.fulflmnt_ln_status_id = 3000 THEN 'SUBMITTED'
                    WHEN c.fulflmnt_ln_status_id = 3500 THEN 'SUBMITTED'
                    WHEN c.fulflmnt_ln_status_id = 3600 THEN 'PICKED'
                    WHEN c.fulflmnt_ln_status_id = 3700 THEN 'PACKED'
                    WHEN c.fulflmnt_ln_status_id = 7000 AND UPPER(c.dlvry_method_id) = 'PICKUPATSTORE' THEN 'PICKED_BY_CUST'
                    WHEN c.fulflmnt_ln_status_id = 7000 THEN 'SHIPPED'
                    WHEN c.fulflmnt_ln_status_id = 9000 THEN 'CANCELLED'
                    ELSE UPPER(c.fulflmnt_ln_status_desc)
                END
            WHEN c.fulflmnt_type = 'StoreFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id = 1000 THEN 'CREATED'
                    WHEN c.fulflmnt_ln_status_id = 2000 THEN 'SUBMITTED'
                    WHEN c.fulflmnt_ln_status_id = 3000 THEN 'PICKED'
                    WHEN c.fulflmnt_ln_status_id BETWEEN 3500 AND 4000 THEN 'PACKED'
                    WHEN c.fulflmnt_ln_status_id BETWEEN 4500 AND 5000 THEN 'SHIPPED'
                    WHEN c.fulflmnt_ln_status_id = 6000 THEN 'PICKED_BY_CUST'
                    WHEN c.fulflmnt_ln_status_id = 9000 THEN 'CANCELLED'
                    ELSE UPPER(c.fulflmnt_ln_status_desc)
                END
            ELSE UPPER(c.fulflmnt_ln_status_desc)
        END AS entrystatus
    FROM mao_consignments c
    WHERE c.fulflmnt_ln_status_id NOT IN (8000, 8500)
    UNION ALL
    SELECT
        c.*,
        CASE
            WHEN c.fulflmnt_type = 'DCFulfillment' AND c.fulflmnt_ln_status_id BETWEEN 1000 AND 2000 THEN 'CONSIGNMENT_PAYMENT_SETTLED'
            WHEN c.fulflmnt_type = 'StoreFulfillment' AND c.fulflmnt_ln_status_id = 1000 THEN 'CONSIGNMENT_PAYMENT_SETTLED'
        END AS consignment_status,
        CASE
            WHEN c.fulflmnt_type = 'DCFulfillment' AND UPPER(pymt_txn.pymt_txn_status_desc) = 'FAILURE' THEN 'SUBMIT_FAILED'
            WHEN c.fulflmnt_type = 'DCFulfillment' AND c.fulflmnt_ln_status_id BETWEEN 1000 AND 2000 THEN 'CREATED'
            WHEN c.fulflmnt_type = 'StoreFulfillment' AND UPPER(pymt_txn.pymt_txn_status_desc) = 'FAILURE' THEN 'SUBMIT_FAILED'
            WHEN c.fulflmnt_type = 'StoreFulfillment' AND c.fulflmnt_ln_status_id = 1000 THEN 'CREATED'
        END AS entrystatus
    FROM mao_consignments c
    LEFT JOIN pymt_txn_status pymt_txn
        ON c.org_id = pymt_txn.org_id AND c.ord_id = pymt_txn.ord_id
    WHERE c.pymt_status_id = 5000
        AND (
            (c.fulflmnt_type = 'DCFulfillment' AND c.fulflmnt_ln_status_id BETWEEN 1000 AND 2000)
            OR (c.fulflmnt_type = 'StoreFulfillment' AND c.fulflmnt_ln_status_id = 1000)
        )
),
consignments_main AS (
    SELECT *
    FROM (
        SELECT
            con.*,
            ROW_NUMBER() OVER (
                PARTITION BY con.org_id, con.ord_id, con.consignment_id, con.consignment_status
                ORDER BY con.updated_ts DESC, con.consignment_status
            ) AS consignment_rnk
        FROM consignments con
    )
    WHERE consignment_rnk = 1
),
consignment_entries AS (
    SELECT
        ent.org_id,
        ent.ord_id,
        ent.consignment_id,
        ent.consignment_status,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'baseShippingCharged', CAST(CASE WHEN ent.is_base_shipping_charged = 1 THEN 'true' ELSE 'false' END AS STRING),
                'cancelCode', CAST(cnl_reason.oms_cancel_code AS STRING),
                'cancelReason', CAST(ent.cnl_reason_desc AS STRING),
                'cancelledQty', CAST(ABS(COALESCE(ent.cnlled_qty, 0)) AS STRING),
                'carrier', CAST(ent.carrier_cd AS STRING),
                'cartonNum', CAST(ent.pkg_id AS STRING),
                'cegrRefId', CAST(ent.inv_id AS STRING),
                'entryId', CAST(ent.fulflmnt_dtl_id AS STRING),
                'entryStatus', CAST(ent.entrystatus AS STRING),
                'fulfilledQty', CAST(ABS(COALESCE(ent.fulfld_qty, 0)) AS STRING),
                'metadata', CAST(ent.ord_note AS STRING),
                'modifiedDate', CAST(TO_VARCHAR(
                    TRY_TO_TIMESTAMP(CAST(ent.updated_ts AS STRING), 'YYYY-MM-DD HH24:MI:SS.FF3'),
                    'YYYY-MM-DD"T"HH24:MI:SS.FF3"000000Z"'
                ) AS STRING),
                'pricing', OBJECT_CONSTRUCT(
                    'currencyIso', CAST(ent.ccy_cd AS STRING),
                    'discountAmount', CAST(ABS(COALESCE(ent.fv_cnlled_total_disc, 0) + COALESCE(ent.fv_total_disc, 0)) AS STRING),
                    'discountedTotalAmount', CAST((ABS(COALESCE(ent.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ent.fv_ord_ln_sub_total, 0)) + 
										ABS(COALESCE(ent.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ent.fv_ord_shipping_amt, 0)) +
										ABS(COALESCE(ent.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ent.fv_ord_shipping_tax_amt, 0)) +
										ABS(COALESCE(ent.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ent.fv_ord_sales_tax_amt, 0))) -
										ABS(COALESCE(ent.fv_cnlled_total_disc, 0) + COALESCE(ent.fv_total_disc, 0)) AS STRING),
                    'giftBoxAmount', NULL,
                    'giftBoxTaxAmount', NULL,
                    'originalRetailPrice', CAST(ABS(COALESCE(ent.fv_orig_unit_price, ent.fv_unit_price, 0)) AS STRING),
                    'priceOverrideReason', NULL,
                    'shippingAmount', CAST(ABS(COALESCE(ent.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ent.fv_ord_shipping_amt, 0)) AS STRING),
                    'shippingTaxAmount', CAST(ABS(COALESCE(ent.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ent.fv_ord_shipping_tax_amt, 0)) AS STRING),
                    'subTotalAmount', CAST(ABS(COALESCE(ent.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ent.fv_ord_ln_sub_total, 0)) AS STRING),
                    'taxAmount', CAST(ABS(COALESCE(ent.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ent.fv_ord_sales_tax_amt, 0)) AS STRING),
                    'totalAmount', CAST(ABS(COALESCE(ent.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ent.fv_ord_ln_sub_total, 0)) + 
										ABS(COALESCE(ent.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ent.fv_ord_shipping_amt, 0)) +
										ABS(COALESCE(ent.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ent.fv_ord_shipping_tax_amt, 0)) +
										ABS(COALESCE(ent.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ent.fv_ord_sales_tax_amt, 0)) AS STRING),
                    'unitPrice', CAST(ABS(COALESCE(ent.fv_unit_price, 0)) AS STRING)
                ),
                'productCode', CAST(TRIM(CASE
                    WHEN ent.item_id = 'ECARD20' THEN '2138264'
                    WHEN ent.item_id = 'ECARD21' THEN '2138265'
                    WHEN ent.item_id = 'ECARD22' THEN '2138266'
                    WHEN ent.item_id = 'ECARD45' THEN '20'
                    WHEN ent.item_id = 'ECARD77' THEN '2000003'
                    WHEN ent.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
                    WHEN ent.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.internal_product_number
                    ELSE ent.item_id
                END) AS STRING),
                'productType', CAST(ent.product_type AS STRING),
                'requestSystem', NULL,
                'requestedBy', CAST(ent.created_by AS STRING),
                'requestedQty', CAST(ABS(COALESCE(ent.qty, 0)) AS STRING),
                'requestingSystemLineNo', CAST(ent.fulflmnt_ln_id AS STRING),
                'shippedDate', CAST(ent.shpd_dt AS STRING),
                'size', CAST(CASE WHEN ent.is_gift_card = 1 THEN ent.itm_size ELSE pm.legacy_size_desc END AS STRING),
                'sku', CAST(CASE
                    WHEN ent.is_gift_card = 1 THEN ent.item_id
                    WHEN ent.channel != 'XSTORE' AND ent.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.online_us_sku
                    WHEN ent.channel != 'XSTORE' AND ent.org_id IN ('FL-CA', 'CH-CA') THEN pm.online_ca_sku
                    WHEN ent.channel = 'XSTORE' THEN pm_div.legacy_sku_size
                    ELSE itm.color
                END AS STRING),
                'storeNumber', NULL,
                'ticketNumber', NULL,
                'trackingId', CAST(ent.tracking_num AS STRING),
                'trackingUrl', CASE WHEN ent.tracking_num IS NOT NULL THEN CAST(CONCAT(
                    'https://',
                    CASE TRIM(ent.org_id)
                        WHEN 'FL-US' THEN 'footlocker'
                        WHEN 'FL-CA' THEN 'footlocker'
                        WHEN 'KFL-US' THEN 'kidsfootlocker'
                        WHEN 'CH-CA' THEN 'champs'
                        WHEN 'CH-US' THEN 'champs'
                    END,
                    '.narvar.com/',
                    CASE TRIM(ent.org_id)
                        WHEN 'FL-US' THEN 'footlocker'
                        WHEN 'FL-CA' THEN 'footlocker'
                        WHEN 'KFL-US' THEN 'kidsfootlocker'
                        WHEN 'CH-CA' THEN 'champs'
                        WHEN 'CH-US' THEN 'champs'
                    END,
                    '/tracking/',
                    ent.carrier_cd,
                    '/?order_number=',
                    ent.ord_id,
                    '-DOLBL48HRW',
                    '&tracking_numbers=',
                    ent.tracking_num,
                    '&locale=',
                    ent.ord_locale,
                    '&order_date=',
                    ent.created_ts,
                    '&ozip=',
                    loc.loc_addr_postal_cd,
                    '&origin_country=',
                    'US',
                    '&dzip=',
                    SUBSTRING(ent.ship_to_addr_postal_cd, 1, 5)::STRING,
                    '&destination_country=',
                    ent.ship_to_addr_country,
                    '&product_category=',
                    'WHSE',
                    '&service=HD%22'
                ) AS STRING) END,
                'updated', CASE
                    WHEN ent.updated_by IS NOT NULL THEN 'true'
                    ELSE 'false'
                END
            )
        ) AS consignmententries
        ,CAST(CASE
                    WHEN ent.item_id = 'ECARD20' THEN '2138264'
                    WHEN ent.item_id = 'ECARD21' THEN '2138265'
                    WHEN ent.item_id = 'ECARD22' THEN '2138266'
                    WHEN ent.item_id = 'ECARD45' THEN '20'
                    WHEN ent.item_id = 'ECARD77' THEN '2000003'
                    WHEN ent.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
                    WHEN ent.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.internal_product_number
                    ELSE ent.item_id
                END AS STRING) product_number
    FROM (
        SELECT
            stg.*,
            ROW_NUMBER() OVER (
                PARTITION BY stg.org_id, stg.ord_id, stg.ord_ln_id, stg.consignment_id, stg.consignment_status
                ORDER BY stg.updated_ts DESC
            ) AS entry_rnk
        FROM consignments stg
        JOIN consignments_main main
            ON stg.org_id = main.org_id
            AND stg.ord_id = main.ord_id
            AND stg.consignment_id = main.consignment_id
            AND stg.consignment_status = main.consignment_status
            AND stg.updated_ts <= main.updated_ts
    ) ent
    LEFT JOIN {{ source('dom_gold', 'dim_mao_loc_v') }} loc
        ON COALESCE(ent.ship_from_loc_id, ent.physical_org_id) = loc.loc_id
    LEFT JOIN dim_location dim_loc
        ON LPAD(dim_loc.loc_snum, 5, '0') = LPAD(COALESCE(ent.ship_from_loc_id, ent.physical_org_id), 5, '0')
    LEFT JOIN product_master pm
        ON ent.is_gift_card != 1
        AND TRIM(ent.item_id) = TRIM(pm.global_size_id)
        AND (CASE WHEN ent.org_id IN ('FL-CA', 'CH-CA') THEN '98' ELSE '81' END) = pm.banner_id
    LEFT JOIN product_master_div pm_div
        ON ent.is_gift_card != 1
        AND TRIM(ent.item_id) = TRIM(pm_div.global_size_id)
        AND ent.org_id = pm_div.org_desc
    LEFT JOIN {{ source('dom_gold', 'dim_mao_item_v') }} itm
        ON TRIM(ent.item_id) = TRIM(itm.item_id)
    LEFT JOIN {{ source('dom_gold', 'lkp_cancel_code_reason_v') }} cnl_reason
        ON ent.cnl_reason_id = cnl_reason.cancel_reason_id
    WHERE ent.entry_rnk = 1
    GROUP BY ALL
)
SELECT
    con.ord_id::STRING AS order_id,
    (CASE TRIM(con.org_id)
        WHEN 'FL-US' THEN '21'
        WHEN 'FL-CA' THEN '45'
        WHEN 'KFL-US' THEN '22'
        WHEN 'CH-CA' THEN '77'
        WHEN 'CH-US' THEN '20'
    END)::STRING AS company_number,
    con.cust_id::STRING AS cust_id,
    con.cust_id::STRING AS relatecustomerid,
    con.flx_id::STRING AS flxid,
    CONCAT(con.ship_to_addr_first_name, ' ', con.ship_to_addr_last_name)::STRING AS customername,
    con.ship_to_addr_email::STRING AS email,
    con.ship_to_addr_phone::STRING AS order_phonenumber,
    con.ship_to_addr_addr1::STRING AS shipping_addressline1,
    con.ship_to_addr_addr2::STRING AS shipping_addressline2,
    con.ship_to_addr_city::STRING AS shipping_city,
    con.ship_to_addr_country::STRING AS shipping_country,
    con.ship_to_addr_state::STRING AS shipping_state,
    con.ship_to_addr_postal_cd::STRING AS shipping_postal_code,
    con.ship_to_addr_email::STRING AS shipping_email,
    con.ship_to_addr_first_name::STRING AS shipping_first_name,
    con.ship_to_addr_last_name::STRING AS shipping_last_name,
    con.ship_to_addr_phone::STRING AS shipping_phonenumber,
    bill_dtl.bill_addr_addr1::STRING AS billing_address_line1,
    bill_dtl.bill_addr_addr2::STRING AS billing_address_line2,
    bill_dtl.bill_addr_city::STRING AS billing_city,
    bill_dtl.bill_addr_country::STRING AS billing_country,
    bill_dtl.bill_addr_email::STRING AS billing_email,
    bill_dtl.bill_addr_firstname::STRING AS billing_first_name,
    bill_dtl.bill_addr_lastname::STRING AS billing_last_name,
    bill_dtl.bill_addr_postal_cd::STRING AS billing_postal_code,
    bill_dtl.bill_addr_state::STRING AS billing_postal_state,
    bill_dtl.bill_addr_phone::STRING AS billing_phonenumber,
    LOWER(con.cart_shpmnt_method)::STRING AS ship_method,
    con.shpmnt_method::STRING AS ship_method_desc,
    CAST(con.ord_total AS DECIMAL(15,4)) AS ordertotalamount,
    con.consignment_id::STRING AS consignment_id,
    (CASE UPPER(TRIM(con.dlvry_method_id))
        WHEN 'EMAIL' THEN 'EGC'
        ELSE 'OBF'
    END)::STRING AS consignment_fullfillment_system,
    (CASE UPPER(TRIM(con.dlvry_method_id))
        WHEN 'PICKUPATSTORE' THEN 'PICK'
        WHEN 'SHIPTOADDRESS' THEN 'SHIP'
        WHEN 'SHIPTOSTORE' THEN 'SHIP'
        WHEN 'SHIPTORETURNCENTER' THEN 'SHIP'
        WHEN 'EMAIL' THEN 'ELECTRONIC'
        WHEN 'STORESALE' THEN 'XSTORE'
        WHEN 'STORERETURN' THEN 'XSTORE'
    END)::STRING AS consignment_fullfillment_type,
    NULL::STRING AS consignment_fulfillmentsystemrequestid,
    NULL::STRING AS consignment_fulfillmentsystemresponseid,
    con.inv_id::STRING AS invoice_num,
    ent.consignmententries::VARIANT AS consignmententries,
    COALESCE(dim_loc.loc_snum, con.ship_from_loc_id, con.physical_org_id)::STRING AS consignment_storenumber,
    con.consignment_status::STRING AS consignment_status,
    (CASE WHEN con.consignment_status = 'CONSIGNMENT_PAYMENT_SETTLED' THEN pymt_line.payments_info END)::VARIANT AS payments_info,
    con.updated_ts AS postedat,
    con.created_ts AS orderdatetime,
    con.updated_ts AS load_time_kafka,
    con.etl_updt_ts AS load_time_adls,
    con.gift_card_no::STRING AS gift_card_number,
    ABS(con.gift_card_value)::STRING AS gift_card_amount,
    (CASE con.is_pre_sale
        WHEN 1 THEN 'true'
        WHEN 0 THEN 'false'
    END)::STRING AS presell,
    pymt_line.pymt_txn_types::VARIANT AS payment_transaction_subtype,
    'MAO'::STRING AS ref_source,
    (CASE
        WHEN UPPER(loc.loc_type_id) = 'DC' AND UPPER(TRIM(loc.loc_addr_city)) = 'RENO' THEN 'Reno'
        WHEN UPPER(loc.loc_type_id) = 'DC' AND UPPER(TRIM(loc.loc_addr_city)) = 'CAMP HILL' THEN 'Camp Hill'
        WHEN UPPER(loc.loc_type_id) = 'DC' AND UPPER(TRIM(loc.loc_addr_city)) = 'MILTON' THEN 'Milton'
        WHEN UPPER(loc.loc_type_id) = 'DC' AND UPPER(TRIM(loc.loc_addr_city)) = 'JUNCTION CITY' THEN 'Junction City'
        WHEN UPPER(loc.loc_type_id) = 'DC' THEN 'DC'
        WHEN UPPER(loc.loc_type_id) = 'SUPPLIER' THEN 'Drop Ship'
        WHEN UPPER(loc.loc_type_id) = 'STORE' THEN 'Store'
    END)::STRING AS consignment_fulfillment_center
FROM consignments_main con
JOIN consignment_entries ent
    ON con.org_id = ent.org_id
    AND con.ord_id = ent.ord_id
    AND con.consignment_id = ent.consignment_id
    AND con.consignment_status = ent.consignment_status
LEFT JOIN payment_grouped pymt_line
    ON con.org_id = pymt_line.org_id AND con.ord_id = pymt_line.ord_id
LEFT JOIN bill_address_dtl bill_dtl
    ON bill_dtl.org_id = con.org_id AND bill_dtl.ord_id = con.ord_id
LEFT JOIN {{ source('dom_gold', 'dim_mao_loc_v') }} loc
    ON COALESCE(con.ship_from_loc_id, con.physical_org_id) = loc.loc_id
LEFT JOIN dim_location dim_loc
    ON LPAD(dim_loc.loc_snum, 5, '0') = LPAD(COALESCE(con.ship_from_loc_id, con.physical_org_id), 5, '0')
WHERE con.doc_type_id = 'CustomerOrder'
    AND ent.product_number is not null
