-- This view aggregates the order header information to get totals and latest values per order.
CREATE OR REPLACE TEMP VIEW fct_mao_ord_hdr_agg AS
SELECT
    ord_id,
    sum(ORD_TOTAL_DISCS) AS ORDER_TOTAL_DISCS,
    sum(ORD_SUB_TOTAL) AS ORD_SUB_TOTAL,
    sum(ORD_TOTAL_TAXES) AS ORD_TOTAL_TAXES,
    sum(ORD_TOTAL) AS ORD_TOTAL,
    max(ORD_CREATED_BY) AS ORD_CREATED_BY,
    max(is_prepaid) AS is_prepaid,
    max(CAPTURED_TS) AS ORD_CAPTURE_DT,
    max(CUST_EMAIL) AS CUST_EMAIL,
    max(SELLING_CHANNEL_ID) AS SELLING_CHANNEL_ID,
    max(CONFIRMED_TS) AS CONFIRMED_TS,
    max(LOYALTY_NUM) AS LOYALTY_NUM,
    max(CREATED_TS) AS CREATED_TS
FROM
    sf_gold_dev_db.dom_gold_dev.fct_mao_ord_hdr_t
WHERE
    ACTIVE_FLG = 'Y'
GROUP BY
    ord_id;

-- This view aggregates payment line information, collecting payment types into an array for each order.
CREATE OR REPLACE TEMP VIEW fct_mao_ord_pymt_line_agg AS
SELECT
    ord_id,
    CCY_CODE,
    ADDR_CITY,
    ADDR_COUNTRY,
    ADDR_EMAIL,
    ADDR_POSTAL_CD,
    ADDR_STATE,
    ADDR_ADDR3,
    max(PYMT_GATEWAY_ID) AS PYMT_GATEWAY_ID,
    array_agg(PYMT_TYPE) AS PYMT_TYPE
FROM
    (
        SELECT
            ord_id,
            CCY_CODE,
            ADDR_CITY,
            ADDR_COUNTRY,
            ADDR_EMAIL,
            ADDR_POSTAL_CD,
            ADDR_ADDR3,
            ADDR_STATE,
            PYMT_GATEWAY_ID,
            PYMT_TYPE
        FROM
            sf_gold_dev_db.dom_gold_dev.fct_mao_ord_pymt_line_t
        WHERE
            active_flg = 'Y'
            AND PYMT_GATEWAY_ID IS NOT NULL
            AND ADDR_CITY IS NOT NULL
        GROUP BY
            ALL
    )
GROUP BY
    ord_id,
    CCY_CODE,
    ADDR_CITY,
    ADDR_COUNTRY,
    ADDR_EMAIL,
    ADDR_POSTAL_CD,
    ADDR_STATE,
    ADDR_ADDR3;

-- This view filters for active inventory management records.
CREATE OR REPLACE TEMP VIEW fct_mao_order_inv_mgmt_filtered AS
SELECT
    DISTINCT ord_id,
    ord_ln_id,
    org_id,
    req_id
FROM
    sf_gold_dev_db.dom_gold_dev.FCT_MAO_ORDER_INV_MGMT_T
WHERE
    active_flg = 'Y';

-- This view filters for active fulfillment line records.
CREATE OR REPLACE TEMP VIEW fct_mao_fulfillment_line_filtered AS
SELECT
    *
FROM
    sf_gold_dev_db.dom_gold_dev.FCT_MAO_FULFILLMENT_LINE_T
WHERE
    ACTIVE_FLG = 'Y';

-- This final view joins all the intermediate views to create the normalized order data.
-- This replaces the main query and creates the final temp view to be used downstream.
CREATE OR REPLACE TEMP VIEW oms_orders_dev_v1 AS
SELECT
    DISTINCT FCT_MAO_ORD_LINE_T.ORD_ID AS order_id,
    CASE
        WHEN FCT_MAO_ORD_LINE_T.org_id = 'FL-US' THEN '03'
        WHEN FCT_MAO_ORD_LINE_T.org_id = 'FL-CA' THEN '76'
        WHEN FCT_MAO_ORD_LINE_T.org_id = 'KFL-US' THEN '16'
        WHEN FCT_MAO_ORD_LINE_T.org_id = 'CH-CA' THEN '77'
        WHEN FCT_MAO_ORD_LINE_T.org_id = 'CH-US' THEN '18'
        WHEN FCT_MAO_ORD_LINE_T.org_id = 'FL-INC' THEN 'FL-INC'
        WHEN FCT_MAO_ORD_LINE_T.org_id = 'FL-INC-NA' THEN 'FL-INC-NA'
        ELSE FCT_MAO_ORD_LINE_T.org_id
    END AS company_number,
    CASE
        WHEN FCT_MAO_ORD_LINE_T.MAX_FULFLMNT_STATUS_DESC IN ('Open') THEN 'SUBMITTED'
        WHEN FCT_MAO_ORD_LINE_T.MAX_FULFLMNT_STATUS_DESC IN (
            'Back Ordered', 'Awaiting Procurement', 'Allocated', 'Released', 'In Process', 'Picked', 'Packed'
        ) THEN 'FULFILLMENT_PROCESSING'
        WHEN FCT_MAO_ORD_LINE_T.MAX_FULFLMNT_STATUS_DESC IN ('Fulfilled', 'Delivered') THEN 'FULFILLMENT_COMPLETE'
        WHEN FCT_MAO_ORD_LINE_T.MAX_FULFLMNT_STATUS_DESC IN ('Canceled') THEN 'CANCELLED'
        WHEN FCT_MAO_ORD_LINE_T.MAX_FULFLMNT_STATUS_DESC IN ('Pending Approval') THEN 'WAIT_FRAUD_SYSTEM_CHECK'
        ELSE FCT_MAO_ORD_LINE_T.MAX_FULFLMNT_STATUS_DESC
    END AS order_status,
    FCT_MAO_ORD_LINE_T.CNL_REASON_ID AS cancel_code,
    CAST(NULL AS STRING) AS cancelReason,
    CASE
        WHEN UPPER(DLVRY_METHOD_ID) IN ('PICKUPATSTORE', 'PICKUP_IN_STORE', 'SHIPTORETURNCENTER') THEN 'PICK'
        WHEN UPPER(DLVRY_METHOD_ID) IN ('SHIPTOADDRESS', 'SHIPTOSTORE') THEN 'SHIP'
        WHEN UPPER(DLVRY_METHOD_ID) = 'EMAIL' THEN 'ELECTRONIC'
        WHEN UPPER(DLVRY_METHOD_ID) = 'STORESALE' THEN 'XSTORE'
        ELSE DLVRY_METHOD_ID
    END AS fullfillment_type,
    fct_mao_ord_pymt_line_agg.PYMT_TYPE AS payment_type,
    CAST(
        CASE
            WHEN IS_FREE_SHIPPING = 0 THEN true
            WHEN IS_FREE_SHIPPING = 1 THEN false
            ELSE NULL
        END AS STRING
    ) AS free_shipping,
    FCT_MAO_ORD_LINE_T.ORD_LN_ID AS order_lineNumber,
    fct_mao_ord_pymt_line_agg.CCY_CODE AS order_currency,
    CAST(NULL AS STRING) AS order_salecode,
    CAST(NULL AS STRING) AS order_priceOverrideReason,
    CAST(FCT_MAO_ORD_LINE_T.TOTAL_DISC_ON_ITEM AS STRING) AS order_discountAmount,
    CAST(FCT_MAO_ORD_LINE_T.TOTAL_DISC AS STRING) AS order_discounted_totalAmount,
    CAST(FCT_MAO_ORD_LINE_T.ORIG_UNIT_PRICE AS STRING) AS order_original_retailPrice,
    CAST(ORD_SHIPPING_AMT AS STRING) AS order_shippingAmount,
    CAST(ORD_SHIPPING_TAX_AMT AS STRING) AS order_shippingTaxAmount,
    CAST(FCT_MAO_ORD_LINE_T.ORD_LN_SUB_TOTAL AS STRING) AS order_subTotalAmount,
    CAST(FCT_MAO_ORD_LINE_T.TOTAL_TAXES AS STRING) AS order_taxAmount,
    CAST(NULL AS STRING) AS order_giftBoxAmount,
    CAST(NULL AS STRING) AS order_giftBoxTaxAmount,
    CAST(FCT_MAO_ORD_LINE_T.ORD_LN_TOTAL AS STRING) AS order_totalAmount,
    CAST(FCT_MAO_ORD_LINE_T.ORIG_UNIT_PRICE - FCT_MAO_ORD_LINE_T.TOTAL_DISC_ON_ITEM AS STRING) AS order_unitPrice,
    CAST(NULL AS STRING) AS order_giftCardNum,
    DIM_MAO_LOC_T.LOC_TYPE_ID AS order_inventoryLocation,
    CAST(NULL AS STRING) AS order_product_name,
    CAST(NULL AS STRING) AS order_product_image,
    FCT_MAO_ORD_LINE_T.is_backorderFlg AS order_backorderFlag,
    CAST(NULL AS STRING) AS order_product_brand,
    CAST(NULL AS STRING) AS order_product_category,
    CAST(NULL AS STRING) AS order_product_color,
    CAST(NULL AS STRING) AS order_product_description,
    fct_mao_ord_hdr_agg.is_prepaid AS order_product_isCollectUpFront,
    FCT_MAO_ORD_LINE_T.is_launch_sku_flg AS order_product_launch_SkuFlag,
    CAST(NULL AS STRING) AS order_product_designator,
    CAST(NULL AS STRING) AS order_product_number,
    CAST(NULL AS STRING) AS order_product_type,
    CAST(NULL AS STRING) AS order_product_size,
    CAST(NULL AS STRING) AS order_product_sku,
    FCT_MAO_ORD_LINE_T.TAX_CODE AS order_product_taxCode,
    CAST(FCT_MAO_ORD_LINE_T.QTY AS STRING) AS order_quantity,
    FCT_MAO_ORD_LINE_T.SHIP_METHOD_ID AS order_shipMethod,
    FCT_MAO_ORD_LINE_T.TAX_CODE AS order_taxCode,
    FCT_MAO_ORD_LINE_T.SHIP_METHOD_ID AS store_fulfillment_shipMethod,
    FCT_MAO_ORD_LINE_T.SHIP_FROM_LOC_ID AS store_fulfillment_storeNumber,
    CAST(NULL AS STRING) AS store_fulfillment_fulfillmentType,
    CASE
        WHEN UPPER(DLVRY_METHOD_ID) IN ('PICKUPATSTORE', 'PICKUP_IN_STORE') THEN fct_mao_ord_hdr_agg.CUST_EMAIL
        ELSE NULL
    END AS store_fulfillment_pickupPersonEmail,
    CAST(NULL AS STRING) AS store_fulfillment_storeCostOfGoods,
    CASE
        WHEN FCT_MAO_ORD_LINE_T.ESTIMATED_DLVRY_TS IS NOT NULL THEN '1'
        WHEN FCT_MAO_ORD_LINE_T.ESTIMATED_DLVRY_TS IS NULL THEN '0'
    END AS store_fulfillment_deliveryEstimateId,
    CAST(FCT_MAO_ORD_LINE_T.IS_EVEN_EXCHG AS STRING) AS exchangeOrder_flag,
    fct_mao_ord_pymt_line_agg.ADDR_CITY AS billing_city,
    fct_mao_ord_pymt_line_agg.ADDR_COUNTRY AS billing_country,
    fct_mao_ord_pymt_line_agg.ADDR_EMAIL AS billing_email,
    fct_mao_ord_pymt_line_agg.ADDR_POSTAL_CD AS billing_postal_code,
    fct_mao_ord_pymt_line_agg.ADDR_STATE AS billing_postal_state,
    CAST(FCT_MAO_ORD_LINE_T.IS_GIFT AS STRING) AS order_giftbox_flag,
    CAST(FCT_MAO_ORD_LINE_T.IS_GIFT AS STRING) AS order_giftorder_flag,
    'NOT MAPPED' AS order_langID,
    ARRAY(FCT_MAO_ORD_LINE_T.req_disc) AS coupons,
    CASE
        WHEN fct_mao_ord_hdr_agg.SELLING_CHANNEL_ID = 'DIGITAL - MOBILE WEB' THEN 'mobile_web'
        WHEN lower(fct_mao_ord_hdr_agg.SELLING_CHANNEL_ID) = 'web' THEN 'desktop_web'
        ELSE fct_mao_ord_hdr_agg.SELLING_CHANNEL_ID
    END AS channel,
    CAST(FCT_MAO_ORD_LINE_T.IS_APPEASEMENT AS STRING) AS appeasementOrder,
    CAST(
        CASE
            WHEN FCT_MAO_ORD_LINE_T.ORD_LN_TOTAL = 0 THEN TRUE
            ELSE FALSE
        END AS STRING
    ) AS noChargeOrder,
    CAST(FCT_MAO_ORD_LINE_T.CREATED_TS AS STRING) AS order_datetime,
    'NOT MAPPED' AS baseShippingCharged,
    'NOT MAPPED' AS shippableConsignmentCount,
    CAST(NULL AS STRING) AS referralSite,
    CAST(FCT_MAO_ORD_LINE_T.ORD_SHIPPING_AMT AS STRING) AS oh_base_shipping_amount,
    CAST(fct_mao_ord_pymt_line_agg.CCY_CODE AS STRING) AS oh_currency,
    CAST(NULL AS STRING) AS oh_couponamount,
    CAST(NULL AS STRING) AS oh_giftBoxAmount,
    CAST(NULL AS STRING) AS oh_giftBoxTaxAmount,
    CAST(FCT_MAO_ORD_LINE_T.ORD_SHIPPING_TAX_AMT AS STRING) AS oh_baseShippingTaxAmount,
    CAST(NULL AS STRING) AS oh_settledAmount,
    CAST(fct_mao_ord_hdr_agg.ORDER_TOTAL_DISCS AS STRING) AS oh_discounted_amount,
    CAST(fct_mao_ord_hdr_agg.ORD_SUB_TOTAL - fct_mao_ord_hdr_agg.ORDER_TOTAL_DISCS AS STRING) AS oh_discounted_total_amount,
    fct_mao_ord_pymt_line_agg.PYMT_GATEWAY_ID AS oh_gateway,
    CAST(FCT_MAO_ORD_LINE_T.ORD_SHIPPING_AMT AS STRING) AS oh_shipping_amount,
    CAST(fct_mao_ord_hdr_agg.ORD_SUB_TOTAL AS STRING) AS oh_subTotal_amount,
    CAST(fct_mao_ord_hdr_agg.ORD_TOTAL_TAXES AS STRING) AS oh_tax_amount,
    CAST(fct_mao_ord_hdr_agg.ORD_TOTAL AS STRING) AS oh_total_amount,
    fct_mao_ord_hdr_agg.CUST_EMAIL AS email,
    DIM_MAO_EMPLOYEE_T.USER_ID AS user_id,
    DIM_MAO_EMPLOYEE_T.USER_TYPE_ID AS user_type,
    CAST(NULL AS STRING) AS controllerCustomerId,
    CAST(NULL AS STRING) AS relateCustomerId,
    FCT_MAO_ORD_LINE_T.FLX_ID AS flxId,
    CAST(NULL AS STRING) AS order_affiliateIdTime,
    CAST(NULL AS STRING) AS order_affiliate_id,
    fct_mao_order_inv_mgmt_filtered.REQ_ID AS order_flrequest_id,
    fct_mao_order_inv_mgmt_filtered.REQ_ID AS order_request_id,
    CAST(fct_mao_ord_hdr_agg.CONFIRMED_TS AS STRING) AS order_request_date,
    CAST(NULL AS STRING) AS order_request_type,
    'NOT MAPPED' AS order_requester,
    CAST(NULL AS STRING) AS rush_flag,
    FCT_MAO_ORD_LINE_T.ASSOCIATE_NUM AS sales_personID,
    FCT_MAO_ORD_LINE_T.SHIP_METHOD_ID AS ship_method,
    FCT_MAO_ORD_LINE_T.SHIP_METHOD_ID AS ship_method_desc,
    CAST(NULL AS STRING) AS vendorId,
    FCT_MAO_ORD_LINE_T.ORD_ID AS web_order_number,
    CAST(NULL AS STRING) AS migratedOrder,
    CAST(NULL AS STRING) AS order_overrideReasoncd,
    FCT_MAO_ORD_LINE_T.source AS order_source,
    CAST(NULL AS STRING) AS relatedordernumber_csa,
    CAST(NULL AS STRING) AS controllerOrderNumber,
    CAST(NULL AS STRING) AS shipping_city,
    CAST(NULL AS STRING) AS shipping_country,
    CAST(NULL AS STRING) AS shipping_state,
    CAST(NULL AS STRING) AS shipping_postal_code,
    fct_mao_ord_hdr_agg.CUST_EMAIL AS shipping_email,
    CAST(NULL AS STRING) AS csaAgentId,
    CAST(NULL AS STRING) AS csaOrderNote,
    CAST(NULL AS STRING) AS order_division,
    CAST(NULL AS STRING) AS omsOrderId,
    fct_mao_ord_hdr_agg.ORD_CREATED_BY AS postedBy,
    CAST(FCT_MAO_ORD_LINE_T.CREATED_TS AS STRING) AS postedAt,
    FCT_MAO_ORD_LINE_T.CREATED_TS AS load_time_kafka,
    CAST(fct_mao_ord_hdr_agg.CREATED_TS AS STRING) AS order_date,
    FCT_MAO_ORD_LINE_T.SRC_LOAD_TS AS load_time_adls,
    CAST(NULL AS DOUBLE) AS cogs,
    FCT_MAO_ORD_LINE_T.ETL_UPDT_TS AS updated_datetime,
    CAST(
        CASE
            WHEN DLVRY_METHOD_ID = 'ShipToStore' AND Ship_from_loc_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS STRING
    ) AS s2s,
    FCT_MAO_ORD_LINE_T.ORD_LN_ID AS lineId,
    FCT_MAO_ORD_LINE_T.UOM AS uom,
    FCT_MAO_ORD_LINE_T.ITEM_ID AS productId,
    'FALSE' AS obfOrder,
    ARRAY(
        STRUCT(
            DIM_MAO_LOC_T.LOC_ID,
            DIM_MAO_LOC_T.LOC_TYPE_ID
        )
    ) AS locationReservationDetails,
    STRUCT('address') AS storeAddress,
    CAST(FCT_MAO_ORD_LINE_T.TOTAL_DISC_ON_ITEM AS STRING) AS OrderLineDiscounts,
    CAST(FCT_MAO_ORD_LINE_T.IS_LOYALTY_DISC AS STRING) AS loyaltyDiscount,
    CAST(NULL AS STRING) AS loyaltyPointsRedeemed,
    CAST(NULL AS STRING) AS loyaltyRewardId,
    CAST(NULL AS STRING) AS loyaltyRedemptionId,
    CAST(NULL AS STRING) AS userAgent,
    CAST(NULL AS STRING) AS user_agent_info,
    CAST(NULL AS STRING) AS device_type
FROM
    sf_gold_dev_db.dom_gold_dev.fct_mao_ord_line_t AS FCT_MAO_ORD_LINE_T
    LEFT JOIN sf_gold_dev_db.dom_gold_dev.DIM_MAO_ORG_T AS DIM_MAO_ORG_T ON FCT_MAO_ORD_LINE_T.ORG_ID = DIM_MAO_ORG_T.ORG_ID
    LEFT JOIN fct_mao_ord_hdr_agg ON FCT_MAO_ORD_LINE_T.ORD_ID = fct_mao_ord_hdr_agg.ORD_ID
    LEFT JOIN fct_mao_fulfillment_line_filtered AS FCT_MAO_FULFILLMENT_LINE_T ON FCT_MAO_FULFILLMENT_LINE_T.ORD_ID = FCT_MAO_ORD_LINE_T.ORD_ID
    AND FCT_MAO_FULFILLMENT_LINE_T.ORD_LN_ID = FCT_MAO_ORD_LINE_T.ORD_LN_ID
    LEFT JOIN fct_mao_ord_pymt_line_agg ON fct_mao_ord_pymt_line_agg.ORD_ID = FCT_MAO_ORD_LINE_T.ORD_ID
    LEFT JOIN sf_gold_dev_db.dom_gold_dev.DIM_MAO_EMPLOYEE_T AS DIM_MAO_EMPLOYEE_T ON FCT_MAO_ORD_LINE_T.created_by = DIM_MAO_EMPLOYEE_T.user_id
    LEFT JOIN fct_mao_order_inv_mgmt_filtered ON FCT_MAO_ORD_LINE_T.ord_id = fct_mao_order_inv_mgmt_filtered.ord_id
    AND FCT_MAO_ORD_LINE_T.ord_ln_id = fct_mao_order_inv_mgmt_filtered.ord_ln_id
    AND FCT_MAO_ORD_LINE_T.org_id = fct_mao_order_inv_mgmt_filtered.org_id
    LEFT JOIN sf_gold_dev_db.dom_gold_dev.DIM_MAO_LOC_T AS DIM_MAO_LOC_T ON FCT_MAO_ORD_LINE_T.SHIP_FROM_LOC_ID = DIM_MAO_LOC_T.loc_id
    AND FCT_MAO_ORD_LINE_T.org_id = DIM_MAO_LOC_T.PRNT_ORG_ID
WHERE
    FCT_MAO_ORD_LINE_T.ACTIVE_FLG = 'Y'
    AND lower(FCT_MAO_ORD_LINE_T.MAX_FULFLMNT_STATUS_DESC) NOT LIKE '%return%';
