CREATE OR REPLACE TEMP VIEW product_master AS
(
   SELECT distinct
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
  pm.global_brand_desc,pm.desc
  FROM prod.product_npii.product_master pm
  where pm.banner_id in ('81','98')
);

CREATE OR REPLACE TEMP VIEW product_master_div AS
(
  SELECT distinct
  internal_product_number_flca,
  internal_product_number,
  legacy_size_desc,
  online_us_sku,
  online_ca_sku,
  concat(trim(legacy_sku),'-',trim(legacy_size_code)) as legacy_sku_size,
  global_size_id,
 banner_id,
  case  when banner_id = '03' then 'FL-US'
        when banner_id = '16' then 'KFL-US'
        when banner_id = '18' then 'CH-US'
        when banner_id = '76' then 'FL-CA'
        when banner_id = '77' then 'CH-CA'
        end org_desc,
  global_brand_desc,
  fob_desc,
  desc_long_2,
  desc,
  designator_id,
  cost,
  size_default_established_cost,
  size_default_established_cost_flca,
  tax_code
  FROM prod.product_npii.product_master
  where banner_id in ('03','16','18','76','77')
);

CREATE OR REPLACE TEMP VIEW payment_grouped AS
(
WITH payment AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY org_id, ord_id, pymt_method_id, pymt_txn_id
    ORDER BY NVL(PYMT_TXN_DETAIL_ID, '') DESC
  ) AS rnk
  FROM ${dom_gold_db}.${dom_gold_schema}.FCT_MAO_ORD_PYMT_LINE_V 
),
authorizations_agg AS (
  SELECT
    org_id,
    ord_id,
    COLLECT_LIST(
      STRUCT(
        CAST(pymt_method_amt AS STRING) AS amount,
        CAST(auth_cd AS STRING) AS authCode,
        CAST(txn_card_last4 AS STRING) AS cardLast4,
        CAST(inv_id AS STRING) AS cegrRefId,
        CAST(PYMT_TXN_ID AS STRING) AS paymentTransactionId,
        CAST(NULL AS STRING) AS paymentTransactionSubType,
        CAST(PYMT_TXN_TYPE AS STRING) AS paymentTransactionType,
        CAST(case when pymt_type='Gift Card' then 'GIFTCARD' else upper(pymt_type) end AS STRING) AS paymentType,
        CAST(PYMT_CARD_TYPE AS STRING) AS creditCardType,
        CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt) AS STRING) AS date,
        CAST(NULL AS STRING) AS shippingTaxAmount,
        CAST(NULL AS STRING) AS taxAmount,
        STRUCT(
          STRUCT(
            CAST(pymt_txn_status_desc AS STRING) AS authResponse,
            CAST(attrib_avs_code AS STRING) AS avsCode,
            CAST(card_alias AS STRING) AS cardAlias,
            CAST(pymt_grp_id AS STRING) AS cardBin,
            CAST(attrib_card_last4 AS STRING) AS cardLast4,
            CAST(card_token AS STRING) AS cardToken,
            CAST(attrib_card_type_display AS STRING) AS cardType,
            CAST(NULL AS STRING) AS confirmationCode,
            CAST(attrib_cvv_response AS STRING) AS cvvResponse,
            CAST(addr_email AS STRING) AS email,
            CAST(attrib_card_expiry_dt AS STRING) AS expirationDate,
            CAST(CASE WHEN pymt_type = 'Gift Card' THEN attrib_card_last4 END AS STRING) AS giftCardNumber,
            CAST(CASE WHEN pymt_type = 'Gift Card' THEN SELLER_PROTECTION_STATUS END AS STRING) AS sellerProtection
          ) AS attributes,
          CAST(pymt_txn_req_amt AS STRING) AS authAmount,
          CAST(auth_cd AS STRING) AS authCode,
          CAST(NULL AS STRING) AS errorMessage,
          CAST(NULL AS STRING) AS id,
          CAST(ord_id AS STRING) AS originalOrderNumber,
          CAST(pymt_type AS STRING) AS paymentType,
          CAST(NULL AS STRING) AS preSettled,
          CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt) AS STRING) AS transactionDate,
          CAST(pymt_txn_id AS STRING) AS transactionId
        ) AS authorization,
        CASE WHEN pymt_type = 'Credit Card' THEN
          STRUCT(
            STRUCT(
              CAST(pymt_txn_req_amt AS STRING) AS authAmount,
              CAST(auth_cd AS STRING) AS authCode,
              CAST(pymt_txn_status_desc AS STRING) AS authResponse,
              CAST(pymt_txn_req_dt AS STRING) AS authTime,
              CAST(attrib_avs_code AS STRING) AS avsCode,
              CAST(attrib_cvv_response AS STRING) AS cvvResponse,
              CAST(ord_id AS STRING) AS originalOrderNumber,
              CAST(NULL AS STRING) AS preSettled,
              CAST(transaction_ref_id AS STRING) AS referenceNumber,
              CAST(pymt_txn_id AS STRING) AS transactionId
            ) AS authInfo,
            CAST(card_alias AS STRING) AS cardAlias,
            CAST(pymt_grp_id AS STRING) AS cardBin,
            CAST(txn_card_last4 AS STRING) AS cardLast4,
            CAST(card_token AS STRING) AS cardToken,
            CAST(attrib_card_expiry_dt AS STRING) AS expirationDate,
            CAST(attrib_card_type_display AS STRING) AS type
          )
        ELSE NULL END AS creditCard,
        CASE WHEN pymt_type = 'Gift Card' THEN
          STRUCT(
            CAST(pymt_method_amt AS STRING) AS amount,
            CAST(auth_cd AS STRING) AS authCode,
            CAST(attrib_card_last4 AS STRING) AS giftCardNumber,
            CAST(ord_id AS STRING) AS originalOrderNumber,
            CAST(NULL AS STRING) AS preSettled,
            CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt) AS STRING) AS transactionDate,
            CAST(pymt_txn_id AS STRING) AS transactionId
          )
        ELSE NULL END AS giftCard,
        CASE WHEN pymt_type = 'PayPal' THEN
          STRUCT(
            CAST(pymt_method_amt AS STRING) AS amount,
            STRUCT(
              CAST(crnt_auth_amt AS STRING) AS authAmount,
              CAST(auth_cd AS STRING) AS authCode,
              CAST(NULL AS STRING) AS authResponse,
              CAST(pymt_txn_req_dt AS STRING) AS authTime,
              CAST(attrib_avs_code AS STRING) AS avsCode,
              CAST(attrib_cvv_response AS STRING) AS cvvResponse,
              CAST(ord_id AS STRING) AS originalOrderNumber,
              CAST(NULL AS STRING) AS preSettled,
              CAST(transaction_ref_id AS STRING) AS referenceNumber,
              CAST(pymt_txn_id AS STRING) AS transactionId
            ) AS authInfo,
            CAST(addr_email AS STRING) AS paypalEmailId,
            CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt) AS STRING) AS transactionDate,
            CAST(pymt_txn_id AS STRING) AS transactionId
        )
        ELSE NULL END AS paypal
      )
    ) AS Payments_Info
  FROM payment
  WHERE rnk = 1
  GROUP BY org_id, ord_id
)
SELECT
  org_id,
  ord_id,
  Payments_Info
FROM authorizations_agg
);

CREATE OR REPLACE TEMP VIEW exchanges AS
(
WITH fct_mao_ord_line_stg AS (
  SELECT
    ord_line.*,
    CASE
      WHEN ord_line.max_fulflmnt_status_id IN (1000, 1500) THEN 'EXCHANGE_ORDER_CREATED'
      ELSE 'EXCHANGE_INITIATED'
    END AS exchange_status,
    first_value(ord_line.orig_unit_price) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_orig_unit_price,
	first_value(ord_line.unit_price) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_unit_price,
    first_value(ord_line.cnlled_total_disc) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_total_disc,
    first_value(ord_line.total_disc) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_total_disc,
    first_value(ord_line.cnlled_ord_ln_total) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_ln_total,
    first_value(ord_line.ord_ln_total) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_ln_total,
    first_value(ord_line.cnlled_total_charges) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_total_charges,
    first_value(ord_line.total_charges) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_total_charges,
    first_value(ord_line.cnlled_total_taxes) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_total_taxes,
    first_value(ord_line.total_taxes) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_total_taxes,
    first_value(ord_line.cnlled_orig_ord_shipping_tax_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_shipping_tax_amt,
    first_value(ord_line.cnlled_orig_ord_shipping_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_shipping_amt,
    first_value(ord_line.orig_ord_shipping_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_shipping_amt,
    first_value(ord_line.orig_ord_shipping_tax_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_shipping_tax_amt,
    first_value(ord_line.cnlled_ord_ln_sub_total) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_ln_sub_total,
    first_value(ord_line.ord_ln_sub_total) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_ln_sub_total,
    first_value(ord_line.cnlled_orig_ord_sales_tax_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_sales_tax_amt,
    first_value(ord_line.orig_ord_sales_tax_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_sales_tax_amt,
    first_value(ord_line.gift_card_value) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_gift_card_value
  FROM ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_line_hist_v ord_line
  JOIN ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v ord_hdr
    ON ord_line.org_id = ord_hdr.org_id AND ord_line.ord_id = ord_hdr.ord_id
  WHERE ord_hdr.doc_type_id = 'CustomerOrder'
    AND ord_line.is_gift_card = 0
    AND ord_line.is_even_exchg = 1
    AND ord_line.etl_updt_ts > '${lookback_date}'
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
)
SELECT
  CAST(ord_line.PRNT_ORD_ID AS STRING)                         AS order_id,
  CAST(ord_line.ORD_ID AS STRING)                              AS exchangeId,
  CAST(ord_line.exchange_status AS STRING)                     AS Header_exchangeStatus,
  CAST(CASE TRIM(ord_line.org_id)
    WHEN 'FL-US'  THEN '21'
    WHEN 'FL-CA'  THEN '45'
    WHEN 'KFL-US' THEN '22'
    WHEN 'CH-CA'  THEN '77'
    WHEN 'CH-US'  THEN '20'
  END AS STRING)                                               AS companyNumber,
  CAST(ord_line.CREATED_BY AS STRING)                          AS exchange_createdBy,
  CAST(ord_line.created_by AS STRING)                          AS userId,
  CAST(NULL AS STRING)                                         AS source,
    cast(DATE_FORMAT(
                  TO_TIMESTAMP(coalesce(ord_hdr.confirmed_ts,ord_hdr.captured_ts), 'yyyy-MM-dd HH:mm:ss.SSS'),
                  "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
            ) as string)                                          AS exchange_header_date,
  CAST(ord_line.ORD_ID AS STRING)                              AS exchangeNum,
  CAST(ord_line.ORD_ID AS STRING)                              AS returnNum,
  CAST(ord_line.ORD_ID AS STRING)                              AS exchangeOrderNum,
  CAST(ord_line.exchange_status AS STRING)                     AS exchange_status,
  CAST(CASE
    WHEN UPPER(ord_line.DLVRY_METHOD_ID) = 'PICKUPATSTORE'      THEN 'PICK'
    WHEN UPPER(ord_line.DLVRY_METHOD_ID) = 'PICKUP_IN_STORE'    THEN 'PICK'
    WHEN UPPER(ord_line.DLVRY_METHOD_ID) = 'SHIPTORETURNCENTER' THEN 'PICK'
    WHEN UPPER(ord_line.DLVRY_METHOD_ID) = 'SHIPTOADDRESS'      THEN 'SHIP'
    WHEN UPPER(ord_line.DLVRY_METHOD_ID) = 'SHIPTOSTORE'        THEN 'SHIP'
    WHEN UPPER(ord_line.DLVRY_METHOD_ID) = 'EMAIL'              THEN 'ELECTRONIC'
    WHEN UPPER(ord_line.DLVRY_METHOD_ID) = 'STORESALE'          THEN 'XSTORE'
    ELSE ord_line.DLVRY_METHOD_ID
  END AS STRING)                                           AS fullfillmentType,
  CAST(ord_line.cart_shpmnt_method AS STRING)              AS shipMethod,
  CAST(ord_line.shpmnt_method AS STRING)                   AS shipMethodDesc,
  CAST(ord_hdr.ccy_cd AS STRING)                           AS shippingAmount_currencyIso,
  CAST(ABS(coalesce(ord_line.ord_shipping_amt,0)) AS STRING) AS shippingAmount_value,
  ARRAY(STRUCT(
    CAST(NULL AS STRING) AS code,
    CAST(NULL AS STRING) AS price
  ))                                                       AS shippingLine,
  CAST(ord_line.ORD_LN_ID AS STRING)                       AS lineNumber,
  CAST(NULL AS STRING)                                     AS line_saleCode,
  CAST(ord_line.TAX_CD AS STRING)                          AS line_taxCode,
  CAST(NULL AS STRING)                                     AS line_,
  CAST(NULL AS STRING)                                     AS line_giftReceipientEmail,
  CAST(NULL AS STRING)                                     AS line_giftFrom,
  CAST(NULL AS STRING)                                     AS line_giftTo,
  CAST(NULL AS STRING)                                     AS line_giftCardNum,
  CAST(ord_line.cart_shpmnt_method AS STRING)              AS line_shipMethod,
  CAST(CASE
    WHEN ord_line.IS_FREE_SHIPPING = 1 THEN 'true'
    WHEN ord_line.IS_FREE_SHIPPING = 0 THEN 'false'
  END AS STRING)                                           AS line_freeShipping,
  CAST(ord_line.QTY AS STRING)                             AS line_quantity,
  CAST(CASE UPPER(LOC.LOC_TYPE_ID)
    WHEN 'DC' THEN 'WHSE'
    WHEN 'STORE' THEN 'STORE'
    WHEN 'SUPPLIER' THEN 'DROPSHIP'
  END AS STRING)                                           AS line_inventoryLocation,
  CAST(ord_line.itm_desc AS STRING)                        AS name,
  CAST(ord_line.small_image_u_r_i AS STRING)               AS image,
  CAST(case when ord_line.is_gift_card=1 then ord_line.item_id
          WHEN ord_hdr.CHANNEL !='XSTORE' AND  ord_line.org_id in ('FL-US','KFL-US','CH-US') then pm.online_us_sku
          WHEN ord_hdr.CHANNEL !='XSTORE' AND  ord_line.org_id in ('FL-CA','CH-CA') then pm.online_ca_sku
          WHEN ord_hdr.CHANNEL ='XSTORE' THEN PM_DIV.legacy_sku_size
          else itm.COLOR end as STRING)                          AS sku,
  CAST(pm.legacy_size_desc AS STRING)                      AS size,
  CAST(pm.desc_long_2 AS STRING)                           AS color,
  CAST(pm.global_brand_desc AS STRING)                       AS brand,
  CAST(pm.fob_desc AS STRING)                              AS category,
  CAST(pm.desc AS STRING)                                  AS description,
  CAST(CASE
    WHEN ord_hdr.is_prepaid = 1 THEN 'true'
    WHEN ord_hdr.is_prepaid = 0 THEN 'false'
  END AS STRING)                                           AS isCollectUpFront,
  CAST(CASE
    WHEN ord_line.IS_BACKORDERFLG = 1 THEN 'true'
    WHEN ord_line.IS_BACKORDERFLG = 0 THEN 'false'
  END AS STRING)                                           AS backorderFlag,
  CAST(CASE
    WHEN ord_line.IS_LAUNCH_SKU_FLG = 1 THEN 'true'
    WHEN ord_line.IS_LAUNCH_SKU_FLG = 0 THEN 'false'
  END AS STRING)                                           AS launchSkuFlag,
  CAST(pm.tax_code AS STRING)                              AS taxCode,
  CAST(pm.designator_id AS STRING)                         AS productDesignator,
  CAST(trim(CASE
            WHEN ord_line.item_id = 'ECARD20' THEN '2138264'
            WHEN ord_line.item_id = 'ECARD21' THEN '2138265'
            WHEN ord_line.item_id = 'ECARD22' THEN '2138266'
            WHEN ord_line.item_id = 'ECARD45' THEN '20'
            WHEN ord_line.item_id = 'ECARD77' THEN '2000003'
            WHEN ord_line.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
            WHEN ord_line.org_id in ('FL-US','KFL-US','CH-US') THEN pm.internal_product_number
            else ord_line.item_id
      END) AS STRING)                               AS productNumber,
  CAST(ord_line.product_type AS STRING)                    AS productType,
  CAST(ord_hdr.CCY_CD AS STRING)                           AS currencyIso,
  CAST(ord_line.PRICE_OVERRIDE_REASON AS STRING)           AS priceOverrideReason,
  CAST(ABS(coalesce(ord_line.fv_orig_unit_price, ord_line.fv_unit_price,0)) AS STRING)  AS originalRetailPrice,
  CAST(ABS(coalesce(ord_line.fv_unit_price,0)) AS STRING)     AS UnitPrice,
  CAST(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) AS STRING) AS subTotalAmount,
  CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS STRING) AS taxAmount,
  CAST(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) AS STRING) AS shippingAmount,
  CAST(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) AS STRING) AS shippingTaxAmount,
  CAST(NULL AS STRING)                                     AS giftBoxAmount,
  CAST(NULL AS STRING)                                     AS giftBoxTaxAmount,
  CAST(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) + 
	ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) + 
	ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) + 
	ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) AS STRING) AS totalAmount,
  CAST(ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) AS STRING) AS discountAmount,
  CAST((ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) + 
	ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) + 
	ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) + 
	ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0))) - 
	ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) AS STRING)  AS discountedTotalAmount,
  STRUCT(
    STRUCT(
      TRANSFORM(pmt.payments_info, p -> STRUCT(
        p.authorization.attributes          AS attributes,
        p.authorization.authAmount          AS authAmount,
        p.authorization.authCode            AS authCode,
        p.authorization.errorMessage        AS errorMessage,
        p.authorization.id                  AS id,
        p.authorization.originalOrderNumber AS originalOrderNumber,
        p.authorization.paymentType         AS paymentType,
        p.authorization.preSettled          AS preSettled,
        p.authorization.transactionDate     AS transactionDate,
        p.authorization.transactionId       AS transactionId
      )) AS authorizations,
      pmt.payments_info[0].creditCard AS creditCard,
      TRANSFORM(pmt.payments_info, p -> p.creditCard) AS creditCards,
      CAST(NULL AS STRING) AS currencyIso,
      STRUCT(
        STRUCT(
          CAST(NULL AS STRING) AS currencyIso,
          CAST(NULL AS STRING) AS value
        ) AS amount
      ) AS electronicCheck,
      TRANSFORM(pmt.payments_info, p -> p.giftCard) AS giftCards,
      TRANSFORM(pmt.payments_info, p -> STRUCT(
        CAST(NULL AS STRING) AS consignmentId,
        CAST(NULL AS STRING) AS giftCardNum,
        CAST(NULL AS STRING) AS refundId,
        CAST(NULL AS STRING) AS returnId,
        STRUCT(
          p.amount                    AS amount,
          p.authCode                  AS authCode,
          p.authorization             AS authorization,
          p.cardLast4                 AS cardLast4,
          p.cegrRefId                 AS cegrRefId,
          p.creditCard                AS creditCard,
          p.creditCardType            AS creditCardType,
          p.date                      AS date,
          p.giftCard                  AS giftCard,
          p.paymentTransactionId      AS paymentTransactionId,
          p.paymentTransactionSubType AS paymentTransactionSubType,
          p.paymentTransactionType    AS paymentTransactionType,
          p.paymentType               AS paymentType,
          p.paypal                    AS paypal,
          p.shippingTaxAmount         AS shippingTaxAmount,
          p.taxAmount                 AS taxAmount
        ) AS transaction
      )) AS paymentTransactions,
      pmt.payments_info[0].paypal AS paypal,
      CAST(NULL AS STRING) AS version
    ) AS alternatePayment,
    CAST(NULL AS STRING)        AS companyNumber,
    CAST(NULL AS STRING)        AS consignmentId,
    CAST(ord_line.ORD_ID AS STRING) AS orderId,
    CAST(NULL AS STRING)        AS paymentTransactionRefNum,
    CAST(NULL AS STRING)        AS retry,
    CAST(NULL AS STRING)        AS totalAmount,
    CAST(NULL AS STRING)        AS totalTax,
    CAST(NULL AS STRING)        AS totalrefundAmount
  ) AS paymentRequest,
  pmt.payments_info                                        AS paymentsInfo,
  ord_line.ship_to_addr_first_name                         AS alternateShipping_firstName,
  ord_line.ship_to_addr_last_name                          AS alternateShipping_lastName,
  ord_line.ship_to_addr_email                              AS alternateShipping_email,
  CAST(
    CASE TRIM(ord_line.org_id)
        WHEN 'FL-US' THEN 'footlocker'
        WHEN 'FL-CA' THEN 'footlocker'
        WHEN 'KFL-US' THEN 'kidsfootlocker'
        WHEN 'CH-CA' THEN 'champs'
        WHEN 'CH-US' THEN 'champs'
    END AS STRING
  )                                                        AS alternateShipping_companyName,
  ord_line.ship_to_addr_phone                              AS alternateShipping_phoneNumber,
  ord_line.ship_to_addr_addr1                              AS alternateShipping_addressLine1,
  ord_line.ship_to_addr_addr2                              AS alternateShipping_addressLine2,
  ord_line.ship_to_addr_city                               AS alternateShipping_city,
  ord_line.ship_to_addr_state                              AS alternateShipping_state,
  ord_line.ship_to_addr_state                              AS alternateShipping_stateCode,
  ord_line.ship_to_addr_country                            AS alternateShipping_country,
  ord_line.ship_to_addr_country                            AS alternateShipping_countryCode,
  ord_line.ship_to_addr_postal_cd                          AS alternateShipping_postalCode,
  CAST(ord_hdr.created_ts AS STRING)                       AS orderDateTime,
  CAST(ord_hdr.updated_ts AS STRING)                       AS postedAt,
  CAST(ord_line.updated_by AS STRING)                      AS postedBy,
  CAST(ord_line.updated_ts AS TIMESTAMP)                   AS load_time_kafka,
  CAST(ord_line.ETL_UPDT_TS AS TIMESTAMP)                  AS load_time_adls,
  CAST(DATE(coalesce(ord_hdr.confirmed_ts,ord_hdr.captured_ts)) AS DATE) AS orderdate,
  CAST('MAO' AS STRING)                                    AS ref_source
FROM fct_mao_ord_line ord_line
  JOIN ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v ord_hdr ON (ord_line.org_id = ord_hdr.org_id and ord_line.ord_id = ord_hdr.ord_id)
  LEFT JOIN payment_grouped pmt ON ord_line.ORD_ID = pmt.ord_id AND ord_line.ORG_ID = pmt.ORG_ID
  LEFT JOIN ${dom_gold_db}.${dom_gold_schema}.DIM_MAO_EMPLOYEE_V emp ON ord_line.created_by = emp.user_id
  LEFT JOIN product_master pm ON ((ord_line.is_gift_card!=1 and trim(ord_line.item_id) = trim(pm.global_size_id) AND (CASE WHEN ord_line.ORG_ID IN ('FL-CA','CH-CA') THEN '98' ELSE '81' END)=pm.banner_id))
  LEFT JOIN product_master_div pm_div ON (ord_line.is_gift_card!=1 and trim(ord_line.item_id) = trim(pm_div.global_size_id) AND ord_line.ORG_ID = pm_div.org_desc)
  LEFT JOIN ${dom_gold_db}.${dom_gold_schema}.dim_mao_item_v itm on (trim(ord_line.ITEM_ID) = trim(itm.ITEM_ID))
  LEFT JOIN ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v loc ON (coalesce(ord_line.ship_from_loc_id,ord_line.physical_org_id) = loc.loc_id)
WHERE coalesce(pm.online_us_sku,ord_line.item_id,pm.online_ca_sku) is not null
);
