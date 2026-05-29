{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}

{{ config(
    materialized="incremental",
    unique_key=["company_number", "order_id", "order_linenumber", "order_status"],
    post_hook=["{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'postedat' ) }}"],
    meta={'strategy': "merge"}
) }}

WITH base_ord_hdr_hist AS (
    SELECT *
    FROM {{ source('dom_gold', 'fct_mao_ord_hdr_hist_v') }}
    WHERE doc_type_id = 'CustomerOrder'
        AND NOT (max_fulflmnt_status_id IS NULL OR max_fulflmnt_status_id > 9000)
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
product_master AS (
    SELECT
        pm.internal_product_number_flca,
        pm.internal_product_number,
        pm.legacy_size_desc,
        pm.online_us_sku,
        pm.online_ca_sku,
        pm.global_size_id,
        pm.banner_id,
        pm.global_brand_desc,
        pm.fob_desc,
        pm.desc_long_2,
        pm.desc,
        pm.designator_id,
        pm.cost,
        pm.size_default_established_cost,
        pm.size_default_established_cost_flca,
        pm.tax_code
    FROM {{ ref('product_master_v') }} pm
    WHERE pm.banner_id IN ('81', '98')
    GROUP BY all
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
        desc,
        designator_id,
        cost,
        size_default_established_cost,
        size_default_established_cost_flca,
        tax_code
    FROM {{ ref('product_master_v') }}
    WHERE banner_id IN ('03', '16', '18', '76', '77')
    GROUP BY all
),
fct_mao_ord_line_stg AS (
    SELECT
        ord_line.*,
        CASE
            WHEN ord_line.max_fulflmnt_status_id = '1000' THEN 'SUBMITTED'
            WHEN ord_line.max_fulflmnt_status_id = '1500' THEN 'SUBMITTED'
            WHEN ord_hdr.is_fraud_service_failed = 1 THEN 'FRAUD_CHECK_FAILED'
            WHEN ord_line.max_fulflmnt_status_id = '1600' THEN 'FULFILMENT_PROCESSING'
            WHEN ord_line.max_fulflmnt_status_id = '2000' THEN 'FULFILMENT_PROCESSING'
            WHEN ord_line.max_fulflmnt_status_id = '3000' THEN 'FULFILMENT_PROCESSING'
            WHEN ord_line.max_fulflmnt_status_id = '3500' THEN 'FULFILMENT_PROCESSING'
            WHEN ord_line.max_fulflmnt_status_id = '3600' THEN 'FULFILMENT_PROCESSING'
            WHEN ord_line.max_fulflmnt_status_id = '3700' THEN 'FULFILMENT_PROCESSING'
            WHEN ord_line.max_fulflmnt_status_id = '7000' THEN 'FULFILMENT_COMPLETE'
            WHEN ord_line.max_fulflmnt_status_id = '7500' THEN 'FULFILMENT_COMPLETE'
            WHEN ord_line.max_fulflmnt_status_id = '8000' THEN 'FULFILMENT_COMPLETE'
            WHEN ord_line.max_fulflmnt_status_id = '8500' THEN 'FULFILMENT_COMPLETE'
            WHEN ord_line.max_fulflmnt_status_id = '9000' THEN 'CANCELLED'
            WHEN ord_line.max_fulflmnt_status_id = '13000' THEN 'WAIT_FRAUD_SYSTEM_CHECK'
            ELSE UPPER(ord_line.max_fulflmnt_status_desc)
        END AS order_status,
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
		FIRST_VALUE(ord_line.gift_card_value) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_gift_card_value
    FROM base_ord_line_hist ord_line
    JOIN base_ord_hdr ord_hdr
        ON ord_line.org_id = ord_hdr.org_id AND ord_line.ord_id = ord_hdr.ord_id
    WHERE ord_hdr.doc_type_id = 'CustomerOrder'
        AND NOT (ord_line.max_fulflmnt_status_id IS NULL OR ord_line.max_fulflmnt_status_id > 9000)
        AND (ord_line.prnt_ord_id IS NULL OR ord_line.is_even_exchg = 1)
        {% if is_incremental() %}
            AND ord_line.etl_updt_ts >= {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL_DBIMART") | as_text }}'
        {% endif %}
),
fct_mao_ord_line AS (
    SELECT *
    FROM (
        SELECT
            ord_line.*,
            ROW_NUMBER() OVER (PARTITION BY org_id, ord_id, ord_ln_id, order_status ORDER BY updated_ts DESC) AS ord_ln_status_rnk
        FROM fct_mao_ord_line_stg ord_line
    )
    WHERE ord_ln_status_rnk = 1
),
fulfillment_stg AS (
    SELECT org_id, ord_id
    FROM {{ source('dom_gold', 'fct_mao_fulfillment_line_v') }}
    GROUP BY all
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
order_line as (
SELECT
    ord_line.ord_id AS order_id,
    CAST(CASE TRIM(ord_hdr.org_id)
        WHEN 'FL-US' THEN '21'
        WHEN 'FL-CA' THEN '45'
        WHEN 'KFL-US' THEN '22'
        WHEN 'CH-CA' THEN '77'
        WHEN 'CH-US' THEN '20'
    END AS VARCHAR) AS company_number,
    CASE
        WHEN UPPER(ord_line.dlvry_method_id) IN ('PICKUPATSTORE', 'PICKUP_IN_STORE', 'SHIPTORETURNCENTER') THEN 'PICK'
        WHEN UPPER(ord_line.dlvry_method_id) IN ('SHIPTOADDRESS', 'SHIPTOSTORE') THEN 'SHIP'
        WHEN UPPER(ord_line.dlvry_method_id) = 'EMAIL' THEN 'ELECTRONIC'
        WHEN UPPER(ord_line.dlvry_method_id) = 'STORERETURN' THEN 'STORE_RETURN'
        WHEN UPPER(ord_line.dlvry_method_id) = 'STORESALE' THEN 'XSTORE'
        ELSE UPPER(ord_line.dlvry_method_id)
    END AS fullfillment_type,
    CAST(CASE
        WHEN ord_line.is_free_shipping = 0 THEN 'true'
        WHEN ord_line.is_free_shipping = 1 THEN 'false'
    END AS VARCHAR) AS free_shipping,
    ord_line.ord_ln_id AS order_linenumber,
    ord_hdr.ccy_cd AS order_currency,
    CAST(NULL AS VARCHAR) AS order_salecode,
    ord_line.price_override_reason AS order_priceoverridereason,
    ABS(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0)) AS order_discountamount,
    CASE
        WHEN 
			ABS(COALESCE(ord_line.fv_ord_ln_total, 0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(COALESCE(ord_line.fv_gift_card_value,0))
        ELSE 
			(ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0))) -
			ABS(COALESCE(ord_line.fv_cnlled_total_disc, 0) + COALESCE(ord_line.fv_total_disc, 0))
	END AS order_discounted_totalamount,
    ABS(COALESCE(ord_line.fv_orig_unit_price, ord_line.fv_unit_price, 0)) AS order_original_retailprice,
    ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) AS order_shippingamount,
    ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) AS order_shippingtaxamount,
    CASE
        WHEN 
			ABS(COALESCE(ord_line.fv_ord_ln_total, 0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(COALESCE(ord_line.fv_gift_card_value,0))
        ELSE 
			ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0))
	END AS order_subtotalamount,
    ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS order_taxamount,
    CAST(NULL AS INT) AS order_giftboxamount,
    CAST(NULL AS INT) AS order_giftboxtaxamount,
    CASE
        WHEN 
			ABS(COALESCE(ord_line.fv_ord_ln_total, 0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(COALESCE(ord_line.fv_gift_card_value,0))
        ELSE 
			ABS(COALESCE(ord_line.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(ord_line.fv_ord_ln_sub_total, 0)) + 
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_amt, 0) + COALESCE(ord_line.fv_ord_shipping_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(ord_line.fv_ord_shipping_tax_amt, 0)) +
			ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0))
    END AS order_totalamount,
    CAST(ABS(COALESCE(ord_line.fv_gift_card_value,ord_line.fv_unit_price, 0)) AS VARCHAR) AS order_unitprice,
    CAST(NULL AS VARCHAR) AS order_giftcardnum,
    CAST(CASE UPPER(loc.loc_type_id)
        WHEN 'DC' THEN 'WHSE'
        WHEN 'STORE' THEN 'STORE'
        WHEN 'SUPPLIER' THEN 'DROPSHIP'
    END AS VARCHAR) AS order_inventorylocation,
    CAST(ord_line.itm_desc AS VARCHAR) AS order_product_name,
    CAST(ord_line.small_image_u_r_i AS VARCHAR) AS order_product_image,
    CAST(CASE
        WHEN ord_line.is_backorderflg = 1 THEN 'true'
        WHEN ord_line.is_backorderflg = 0 THEN 'false'
    END AS VARCHAR) AS order_backorderflag,
    CAST(UPPER(CASE WHEN ord_line.is_gift_card = 1 THEN ord_line.itm_brand ELSE pm.global_brand_desc END) AS VARCHAR) AS order_product_brand,
    CAST(CASE WHEN ord_line.is_gift_card = 1 THEN ord_line.itm_dept_name ELSE pm.fob_desc END AS VARCHAR) AS order_product_category,
    CAST(CASE WHEN ord_line.is_gift_card = 1 THEN ord_line.itm_color_desc ELSE pm.desc_long_2 END AS VARCHAR) AS order_product_color,
    CAST(pm.desc AS VARCHAR) AS order_product_description,
    CAST(CASE
        WHEN ord_hdr.is_prepaid = 1 THEN 'true'
        WHEN ord_hdr.is_prepaid = 0 THEN 'false'
    END AS VARCHAR) AS order_product_iscollectupfront,
    CAST(CASE
        WHEN ord_line.is_launch_sku_flg = 1 THEN 'true'
        WHEN ord_line.is_launch_sku_flg = 0 THEN 'false'
    END AS VARCHAR) AS order_product_launch_skuflag,
    CAST(CASE WHEN ord_line.is_gift_card = 1 THEN 'GFT' ELSE pm.designator_id END AS VARCHAR) AS order_product_designator,
    CAST(TRIM(CASE
        WHEN ord_line.item_id = 'ECARD20' THEN '2138264'
        WHEN ord_line.item_id = 'ECARD21' THEN '2138265'
        WHEN ord_line.item_id = 'ECARD22' THEN '2138266'
        WHEN ord_line.item_id = 'ECARD45' THEN '20'
        WHEN ord_line.item_id = 'ECARD77' THEN '2000003'
        WHEN ord_line.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
        WHEN ord_line.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.internal_product_number
        ELSE ord_line.item_id
    END) AS VARCHAR) AS order_product_number,
    CAST(ord_line.product_type AS VARCHAR) AS order_product_type,
    CAST(CASE WHEN ord_line.is_gift_card = 1 THEN ord_line.itm_size ELSE pm.legacy_size_desc END AS VARCHAR) AS order_product_size,
    CAST(CASE
        WHEN ord_line.is_gift_card = 1 THEN ord_line.item_id
        WHEN ord_hdr.channel != 'XSTORE' AND ord_line.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN pm.online_us_sku
        WHEN ord_hdr.channel != 'XSTORE' AND ord_line.org_id IN ('FL-CA', 'CH-CA') THEN pm.online_ca_sku
        WHEN ord_hdr.channel = 'XSTORE' THEN pm_div.legacy_sku_size
        ELSE itm.color
    END AS VARCHAR) AS order_product_sku,
    CAST(CASE WHEN ord_line.is_gift_card = 1 THEN ord_line.itm_tax_cd ELSE pm.tax_code END AS VARCHAR) AS order_product_taxcode,
    CASE WHEN ord_line.max_fulflmnt_status_id IN ('9000', '19000') THEN ord_line.orig_ord_qty ELSE ord_line.qty END AS order_quantity,
    CAST(ord_line.cart_shpmnt_method AS VARCHAR) AS order_shipmethod,
    CAST(ord_line.tax_cd AS VARCHAR) AS order_taxcode,
    CAST(ord_line.cart_shpmnt_method AS VARCHAR) AS store_fulfillment_shipmethod,
    CAST(dim_loc.loc_snum AS VARCHAR) AS store_fulfillment_storenumber,
    CAST(NULL AS VARCHAR) AS store_fulfillment_fulfillmenttype,
    CASE
        WHEN UPPER(ord_line.dlvry_method_id) IN ('PICKUPATSTORE', 'PICKUP_IN_STORE') THEN ord_hdr.cust_email
    END AS store_fulfillment_pickuppersonemail,
    CAST(ord_line.ship_to_addr_phone AS VARCHAR) AS store_fulfillment_pickuppersonmobile,
    CAST(NULL AS VARCHAR) AS store_fulfillment_storecostofgoods,
    CAST(NULL AS VARCHAR) AS store_fulfillment_deliveryestimateid,
    CAST(NULL AS VARCHAR) AS store_fulfillment_deliveryinstructions,
    CAST(ord_hdr.cust_phone AS VARCHAR) AS store_fulfillment_deliverycustomerphone,
    ord_hdr.created_ts AS order_datetime,
    ord_line.updated_ts AS postedat,
    CAST(ABS(COALESCE(pm.cost, pm.size_default_established_cost, pm.size_default_established_cost_flca) * ord_line.qty) AS INT) AS cogs,
    ord_line.order_status AS order_status,
    CAST(CASE WHEN ord_line.is_appeasement = 1 THEN 'true' END AS VARCHAR) AS appeasementorder,
    CAST(case when ord_hdr.ord_total=0 and ord_hdr.ord_type_id='CallCenter' and ord_line.is_gift_card!=1 then 'true' else 'false' end AS VARCHAR) as nochargeorder,
    CAST(CASE
        WHEN ord_line.dlvry_method_id = 'ShipToStore' AND ord_line.ship_from_loc_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS BOOLEAN) AS s2s,
    ord_line.ord_ln_pk AS lineid,
    CASE UPPER(ord_line.uom) WHEN 'EA' THEN 'EACH' WHEN 'U' THEN 'UNIT' END AS uom,
    ord_line.item_id AS productid,
    ARRAY_CONSTRUCT(
        OBJECT_CONSTRUCT(
            'locationId', dim_loc.loc_num,
            'locationType', UPPER(loc.loc_type_id)
        )
    ) AS locationreservationdetails,
    CAST(CASE WHEN ord_line.cnl_reason_id = 'FRAUD' THEN 'false' ELSE 'true' END AS BOOLEAN) AS obforder,
    OBJECT_CONSTRUCT(
        'addressLine1', loc.loc_addr1,
        'addressLine2', loc.loc_addr2,
        'city', loc.loc_addr_city,
        'companyName', loc.prnt_org_id,
        'country', loc.loc_addr_country,
        'countryCode', loc.loc_addr_country,
        'email', loc.loc_addr_email,
        'firstName', loc.loc_addr_first_name,
        'lastName', loc.loc_addr_last_name,
        'phoneNumber', loc.loc_addr_phone_no,
        'postalCode', loc.loc_addr_postal_cd,
        'state', loc.loc_addr_state
    ) AS storeaddress,
    COALESCE(ord_line.ord_coupons, '[]') AS orderlinediscounts,
    'MAO' AS ref_source
FROM fct_mao_ord_line ord_line
JOIN base_ord_hdr ord_hdr
    ON ord_line.org_id = ord_hdr.org_id AND ord_line.ord_id = ord_hdr.ord_id
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
LEFT JOIN fulfillment_stg fl
    ON ord_line.org_id = fl.org_id AND ord_line.ord_id = fl.ord_id
LEFT JOIN {{ source('dom_gold', 'dim_mao_loc_v') }} loc
    ON COALESCE(ord_line.ship_from_loc_id, ord_line.physical_org_id) = loc.loc_id
LEFT JOIN dim_location dim_loc
    ON LPAD(dim_loc.loc_snum, 5, '0') = LPAD(COALESCE(ord_line.ship_from_loc_id, ord_line.physical_org_id), 5, '0')
WHERE ord_hdr.doc_type_id = 'CustomerOrder'
)
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(company_number AS VARCHAR) AS company_number,
    CAST(fullfillment_type AS VARCHAR) AS fullfillment_type,
    CAST(free_shipping AS VARCHAR) AS free_shipping,
    CAST(order_linenumber AS VARCHAR) AS order_linenumber,
    CAST(order_currency AS VARCHAR) AS order_currency,
    CAST(order_salecode AS VARCHAR) AS order_salecode,
    CAST(order_priceoverridereason AS VARCHAR) AS order_priceoverridereason,
    CAST(order_discountamount AS DECIMAL(15,4)) AS order_discountamount,
    CAST(order_discounted_totalamount AS DECIMAL(15,4)) AS order_discounted_totalamount,
    CAST(order_original_retailprice AS DECIMAL(15,4)) AS order_original_retailprice,
    CAST(order_shippingamount AS DECIMAL(15,4)) AS order_shippingamount,
    CAST(order_shippingtaxamount AS DECIMAL(15,4)) AS order_shippingtaxamount,
    CAST(order_subtotalamount AS DECIMAL(15,4)) AS order_subtotalamount,
    CAST(order_taxamount AS DECIMAL(15,4)) AS order_taxamount,
    CAST(order_giftboxamount AS DECIMAL(15,4)) AS order_giftboxamount,
    CAST(order_giftboxtaxamount AS DECIMAL(15,4)) AS order_giftboxtaxamount,
    CAST(order_totalamount AS DECIMAL(15,4)) AS order_totalamount,
    CAST(order_unitprice AS DECIMAL(15,4)) AS order_unitprice,
    CAST(order_giftcardnum AS VARCHAR) AS order_giftcardnum,
    CAST(order_inventorylocation AS VARCHAR) AS order_inventorylocation,
    CAST(order_product_name AS VARCHAR) AS order_product_name,
    CAST(order_product_image AS VARCHAR) AS order_product_image,
    CAST(order_backorderflag AS VARCHAR) AS order_backorderflag,
    CAST(order_product_brand AS VARCHAR) AS order_product_brand,
    CAST(order_product_category AS VARCHAR) AS order_product_category,
    CAST(order_product_color AS VARCHAR) AS order_product_color,
    CAST(order_product_description AS VARCHAR) AS order_product_description,
    CAST(order_product_iscollectupfront AS VARCHAR) AS order_product_iscollectupfront,
    CAST(order_product_launch_skuflag AS VARCHAR) AS order_product_launch_skuflag,
    CAST(order_product_designator AS VARCHAR) AS order_product_designator,
    CAST(order_product_number AS VARCHAR) AS order_product_number,
    CAST(order_product_type AS VARCHAR) AS order_product_type,
    CAST(order_product_size AS VARCHAR) AS order_product_size,
    CAST(order_product_sku AS VARCHAR) AS order_product_sku,
    CAST(order_product_taxcode AS VARCHAR) AS order_product_taxcode,
    CAST(order_quantity AS DECIMAL(38,0)) AS order_quantity,
    CAST(order_shipmethod AS VARCHAR) AS order_shipmethod,
    CAST(order_taxcode AS VARCHAR) AS order_taxcode,
    CAST(store_fulfillment_shipmethod AS VARCHAR) AS store_fulfillment_shipmethod,
    CAST(store_fulfillment_storenumber AS VARCHAR) AS store_fulfillment_storenumber,
    CAST(store_fulfillment_fulfillmenttype AS VARCHAR) AS store_fulfillment_fulfillmenttype,
    CAST(store_fulfillment_pickuppersonemail AS VARCHAR) AS store_fulfillment_pickuppersonemail,
    CAST(store_fulfillment_pickuppersonmobile AS VARCHAR) AS store_fulfillment_pickuppersonmobile,
    CAST(store_fulfillment_storecostofgoods AS VARCHAR) AS store_fulfillment_storecostofgoods,
    CAST(store_fulfillment_deliveryestimateid AS VARCHAR) AS store_fulfillment_deliveryestimateid,
    CAST(store_fulfillment_deliveryinstructions AS VARCHAR(1000)) AS store_fulfillment_deliveryinstructions,
    CAST(store_fulfillment_deliverycustomerphone AS VARCHAR) AS store_fulfillment_deliverycustomerphone,
    CAST(order_datetime AS TIMESTAMP) AS order_datetime,
    CAST(postedat AS TIMESTAMP) AS postedat,
    CAST(cogs AS DECIMAL(15,4)) AS cogs,
    CAST(order_status AS VARCHAR) AS order_status,
    CAST(appeasementorder AS VARCHAR) AS appeasementorder,
    CAST(nochargeorder AS VARCHAR) AS nochargeorder,
    CAST(s2s AS BOOLEAN) AS s2s,
    CAST(lineid AS VARCHAR(200)) AS lineid,
    CAST(uom AS VARCHAR(200)) AS uom,
    CAST(productid AS VARCHAR(200)) AS productid,
    locationreservationdetails AS locationreservationdetails,
    CAST(obforder AS BOOLEAN) AS obforder,
    storeaddress AS storeaddress,
    CAST(orderlinediscounts AS VARCHAR) AS orderlinediscounts,
    CAST(ref_source AS VARCHAR(50)) AS ref_source
FROM order_line
WHERE order_product_number IS NOT NULL