create or replace temp view product_master as
(
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
  pm.desc,
  pm.tax_code,
  pm.designator_id
  FROM prod.product_npii.product_master pm
  where pm.banner_id in ('81','98')
  group by all
);
 create or replace temp view product_master_div as
(
  SELECT
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
  group by all
);

CREATE 	OR REPLACE TEMP VIEW payment_grouped AS
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
        CAST(CASE WHEN pymt_type = 'Gift Card' THEN 'GIFTCARD' WHEN pymt_type = 'Credit Card' THEN 'CREDITCARD' ELSE UPPER(pymt_type) END AS STRING) AS paymentType,
        CAST(PYMT_CARD_TYPE AS STRING) AS creditCardType,
        CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt) AS STRING) AS date,
        --CAST(ccy_code AS STRING) as currencyIso,
        CAST(NULL AS STRING) AS shippingTaxAmount,
        CAST(NULL AS STRING) AS taxAmount,
        -- authorization struct (with attributes nested)
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
              CAST(pymt_txn_status_desc AS STRING) AS authResponse,
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
    ) AS PaymentsInfo
  FROM payment
  WHERE rnk = 1
  GROUP BY org_id, ord_id
)
SELECT
  org_id,
  ord_id,
  PaymentsInfo
FROM authorizations_agg
);

CREATE 	OR REPLACE TEMP VIEW refunds AS
(
WITH
base_ord_hdr AS (
    SELECT *,
        MAX(ord_total) OVER(PARTITION BY org_id,ord_id) AS max_ord_total,
        MIN(ord_total) OVER(PARTITION BY org_id,ord_id) AS min_ord_total
    FROM ${dom_gold_db}.${dom_gold_schema}.FCT_MAO_ORD_HDR_V
    WHERE doc_type_id = 'CustomerOrder'
),
base_ord_line_hist AS (
    SELECT *
    FROM ${dom_gold_db}.${dom_gold_schema}.FCT_MAO_ORD_LINE_HIST_V
    WHERE max_fulflmnt_status_id IS NOT NULL
        AND max_fulflmnt_status_id NOT IN (8000, 8500)
),
base_ord_line AS (
    SELECT *
    FROM (
        SELECT ol.*,
            MAX(ol.ord_ln_total) OVER(PARTITION BY ol.org_id,ol.ord_id,ol.ord_ln_id) AS max_ln_total,
            MIN(ol.ord_ln_total) OVER(PARTITION BY ol.org_id,ol.ord_id,ol.ord_ln_id) AS min_ln_total,
            ROW_NUMBER() OVER (PARTITION BY ol.org_id, ol.ord_id, ol.ord_ln_id ORDER BY ol.updated_ts DESC) AS rn
        FROM base_ord_line_hist ol
    )
    WHERE rn = 1
),
REFUND_HIST_DETAILS AS (
    SELECT FOL.ORG_ID, FOL.ORD_ID, FOL.ORD_LN_ID,
        COLLECT_LIST(struct(
            CAST(case
	            when foh.PYMT_STATUS_ID='6000' then 'REFUND_PROCESSING'
	            when foh.PYMT_STATUS_ID='7000' then 'REFUNDED'
	            when foh.PYMT_STATUS_ID='5000' and (fol.max_ln_total != fol.min_ln_total AND foh.max_ord_total != 0) then 'REFUNDED'
                else upper(foh.PYMT_STATUS_DESC)
            end as string) AS refundstatus,
            CAST( DATE_FORMAT(
                TO_TIMESTAMP(fol.CREATED_TS, 'yyyy-MM-dd HH:mm:ss.SSS'),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
                ) AS STRING) AS timestamp
        )) AS REFUND_STATUS_HISTORY,
        COLLECT_LIST(struct(
            CAST(ABS(coalesce(fol.ORD_LN_TOTAL,0)) AS STRING) as amount,
            CAST(UPPER(coalesce(foh.refund_pymt_method,'')) AS STRING) as type
        )) AS refundMethods
    FROM (
        SELECT *,
            MAX(ord_ln_total) OVER(PARTITION BY org_id,ord_id,ord_ln_id) AS max_ln_total,
            MIN(ord_ln_total) OVER(PARTITION BY org_id,ord_id,ord_ln_id) AS min_ln_total
        FROM base_ord_line_hist
    ) fol
    JOIN base_ord_hdr foh ON nvl(fol.prnt_ord_id, fol.ord_id) = foh.ORD_ID AND fol.ORG_ID = foh.ORG_ID
    WHERE (foh.PYMT_STATUS_ID in (6000,7000) or (foh.PYMT_STATUS_ID =5000 and fol.max_ln_total != fol.min_ln_total AND foh.max_ord_total != 0))
    GROUP BY all
),
fct_mao_ord_line_fv AS (
    SELECT
        org_id,
        ord_id,
        ord_ln_id,
        first_value(orig_unit_price) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_orig_unit_price,
		first_value(unit_price) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_unit_price,
        first_value(cnlled_total_disc) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_cnlled_total_disc,
        first_value(total_disc) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_total_disc,
        first_value(cnlled_ord_ln_total) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_cnlled_ord_ln_total,
        first_value(ord_ln_total) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_ord_ln_total,
        first_value(cnlled_total_charges) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_cnlled_total_charges,
        first_value(total_charges) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_total_charges,
        first_value(cnlled_total_taxes) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_cnlled_total_taxes,
        first_value(total_taxes) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_total_taxes,
        first_value(cnlled_orig_ord_shipping_amt) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_cnlled_ord_shipping_amt,
        first_value(orig_ord_shipping_amt) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_ord_shipping_amt,
        first_value(cnlled_orig_ord_shipping_tax_amt) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_cnlled_ord_shipping_tax_amt,
        first_value(orig_ord_shipping_tax_amt) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_ord_shipping_tax_amt,
        first_value(cnlled_ord_ln_sub_total) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_cnlled_ord_ln_sub_total,
        first_value(ord_ln_sub_total) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_ord_ln_sub_total,
        first_value(cnlled_orig_ord_sales_tax_amt) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_cnlled_ord_sales_tax_amt,
        first_value(orig_ord_sales_tax_amt) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_ord_sales_tax_amt,
        first_value(gift_card_value) over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_gift_card_value,
        row_number() over (partition by org_id, ord_id, ord_ln_id order by max_fulflmnt_status_id asc, updated_ts asc) as fv_rnk
    FROM base_ord_line_hist
)
    SELECT
        CAST(nvl(fol.PRNT_ORD_ID,fol.ord_id) AS STRING) AS order_id,
        CAST(foh.created_ts AS STRING) AS order_datetime,
        fol.ORD_ID AS refundId,
        cast(case trim(foh.org_id)
            when 'FL-US' then '21'
            when 'FL-CA' then '45'
            when 'KFL-US' then '22'
            when 'CH-CA' then '77'
            when 'CH-US' then '20'
        end as string) companynumber,
        CAST(date(coalesce(foh.confirmed_ts,foh.captured_ts)) AS STRING) AS refund_header_date,
        --CAST(fol.max_fulflmnt_status_desc AS STRING) AS refund_status,
        CAST(case
	            when foh.PYMT_STATUS_ID='6000' then 'REFUND_PROCESSING'
	            when foh.PYMT_STATUS_ID='7000' then 'REFUNDED'
	            when foh.PYMT_STATUS_ID='5000' and (fol.max_ln_total != fol.min_ln_total AND foh.min_ord_total != 0) then 'REFUNDED'
                else upper(foh.PYMT_STATUS_DESC)
            end as string ) AS refund_status,
        fol.created_by AS refund_createdBy,
        foh.cust_id AS refund_userId,
        CASE
            WHEN LOWER(foh.ord_type_id) in ('web','return') THEN 'OMS_OBF_SVC'
            WHEN LOWER(foh.ord_type_id) = 'callcenter' THEN 'CUSTOMER_SVC'
            WHEN LOWER(foh.ord_type_id) = 'savethesale' THEN 'XSTORE'
--            ELSE UPPER(foh.ord_type_id)
        END AS source,
        fol.ORD_ID AS refundNum,
        CAST(case when fol.max_fulflmnt_status_id in ('9000','19000') then fol.orig_ord_qty else fol.QTY end AS STRING) AS quantity,
        CAST(fol.itm_desc AS STRING) AS name,
        CAST(case
              when fol.is_gift_card=1 then fol.item_id
              WHEN foh.CHANNEL !='XSTORE' AND  foh.org_id in ('FL-US','KFL-US','CH-US') then pm.online_us_sku
              WHEN foh.CHANNEL !='XSTORE' AND  foh.org_id in ('FL-CA','CH-CA') then pm.online_ca_sku
              WHEN foh.CHANNEL ='XSTORE' THEN PM_DIV.legacy_sku_size
              else itm.COLOR
        end AS STRING) AS sku,
        CAST(case when fol.is_gift_card=1 then fol.itm_size else pm.legacy_size_desc end AS STRING) AS size,
        CAST(case when fol.is_gift_card=1 then fol.itm_color_desc else pm.desc_long_2 end AS STRING) AS color,
        CAST(fol.small_image_u_r_i AS STRING)  AS image,
        CAST(upper(case when fol.is_gift_card=1 then fol.itm_brand else pm.global_brand_desc end) AS STRING) AS brand,
        CAST(case when fol.is_gift_card=1 then fol.itm_dept_name else pm.fob_desc end AS STRING) AS category,
        CAST(pm.desc AS STRING)  AS description,
        CASE WHEN foh.IS_PREPAID = 0 THEN 'false' END AS isCollectUpFront,
        CASE WHEN fol.IS_BACKORDERFLG = 1 THEN 'true' WHEN fol.IS_BACKORDERFLG = 0 THEN 'false' END AS backorderFlag,
        CASE WHEN fol.IS_LAUNCH_SKU_FLG = 1 THEN 'true' WHEN fol.IS_LAUNCH_SKU_FLG = 0 THEN 'false' END AS launchSkuFlag,
        CAST(case when fol.is_gift_card=1 then fol.itm_tax_cd else pm.tax_code end AS STRING) AS taxCode,
        CAST(case when fol.is_gift_card=1 then 'GFT' else pm.designator_id end AS STRING) AS productDesignator,
        --CAST(pm.internal_product_number AS STRING) AS productNumber,
        cast( trim(CASE
                WHEN fol.item_id = 'ECARD20' THEN '2138264'
                WHEN fol.item_id = 'ECARD21' THEN '2138265'
                WHEN fol.item_id = 'ECARD22' THEN '2138266'
                WHEN fol.item_id = 'ECARD45' THEN '20'
                WHEN fol.item_id = 'ECARD77' THEN '2000003'
                WHEN fol.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
                WHEN fol.org_id in ('FL-US','KFL-US','CH-US') THEN pm.internal_product_number
                else fol.item_id
            END) as string) as productNumber,
        CAST(fol.product_type AS STRING) AS productType,
        cast(foh.inv_id as string) AS cegrRefId,
        CAST(ABS(coalesce(fv.fv_orig_unit_price, fv.fv_unit_price,0)) AS STRING) AS originalRetailPrice,
        CAST(ABS(coalesce(fv.fv_unit_price,0)) AS STRING) AS originalUnitPrice,
        CAST(ABS(coalesce(fv.fv_cnlled_total_disc,0) + coalesce(fv.fv_total_disc,0)) AS STRING) AS originalUnitDiscountAmount,
        CAST(ABS(coalesce(fv.fv_cnlled_ord_ln_sub_total,0) + coalesce(fv.fv_ord_ln_sub_total,0)) AS STRING) AS lineRefundSubTotal, --REFUND_PRICE
		CAST(ABS(COALESCE(fv.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(fv.fv_ord_sales_tax_amt, 0)) AS STRING) AS taxAmount,
        CAST(ABS(coalesce(fv.fv_cnlled_ord_shipping_amt,0) + coalesce(fv.fv_ord_shipping_amt,0)) AS STRING) AS shippingAmount,
        CAST(ABS(coalesce(fv.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(fv.fv_ord_shipping_tax_amt,0)) AS STRING) AS shippingTaxAmount,
        CAST(0 AS STRING) AS inboundShippingAmount,
        CAST(CASE
            WHEN foh.PYMT_STATUS_ID = 5000 AND (fol.max_ln_total != fol.min_ln_total AND foh.min_ord_total != 0) THEN ABS(coalesce(fol.max_ln_total,0) - coalesce(fol.min_ln_total,0))
            ELSE
				ABS(coalesce(fv.fv_cnlled_ord_ln_sub_total,0) + coalesce(fv.fv_ord_ln_sub_total,0)) +
				ABS(coalesce(fv.fv_cnlled_ord_shipping_amt,0) + coalesce(fv.fv_ord_shipping_amt,0)) +
				ABS(coalesce(fv.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(fv.fv_ord_shipping_tax_amt,0)) +
				ABS(coalesce(fv.fv_cnlled_ord_sales_tax_amt,0) + coalesce(fv.fv_ord_sales_tax_amt,0))
        END AS STRING) AS totalAmount,
        CAST(false AS STRING) AS returnExpected,
        CAST(case fol.IS_RTN when 1 then 'true' end AS STRING) AS returned,
        CASE WHEN FOH.ORD_TYPE_ID = 'Return' THEN
            Array(STRUCT(
                cast(FOL.ORD_ID as string) as returnNumber,
                cast( DATE_FORMAT(
                TO_TIMESTAMP(FOL.CREATED_TS, 'yyyy-MM-dd HH:mm:ss.SSS'),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
                )as string) as timestamp
            ))
        END AS returnNumbers,
        CAST(case fol.IS_RTN when 1 then ABS(coalesce(fol.QTY,0)) else 0 end AS STRING) AS quantityReturned,
        coalesce(foh.confirmed_ts,foh.captured_ts) AS returnDate,
        CAST(case
                when fol.rtn_reason is not null then fol.rtn_reason
                when fol.appeasment_reason_cd is not null then fol.appeasment_reason_cd
                when fol.cnl_reason_desc is not null then fol.cnl_reason_desc
            end  AS STRING) AS reasoncode,
        CAST(0 AS STRING) AS ih_inboundShippingAmount,
        CAST(ABS(SUM(COALESCE(fv.fv_cnlled_ord_ln_sub_total, 0) + COALESCE(fv.fv_ord_ln_sub_total, 0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS STRING) AS ih_lineRefundSubTotal,
        CAST(ABS(SUM(COALESCE(fv.fv_orig_unit_price,fv.fv_unit_price,0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS STRING) AS ih_originalRetailPrice,
        CAST(ABS(SUM(COALESCE(fv.fv_cnlled_total_disc, 0) + COALESCE(fv.fv_total_disc, 0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS STRING) AS ih_originalUnitDiscountAmount,
        CAST(ABS(SUM(COALESCE(fv.fv_unit_price,0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS STRING) AS ih_originalUnitPrice,
        CAST(ABS(SUM(coalesce(fv.fv_cnlled_ord_shipping_amt,0) + coalesce(fv.fv_ord_shipping_amt,0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS STRING) AS ih_shippingAmount,
        CAST(ABS(SUM(coalesce(fv.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(fv.fv_ord_shipping_tax_amt,0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS STRING) AS ih_shippingTaxAmount,
        CAST(ABS(SUM(coalesce(fv.fv_cnlled_ord_sales_tax_amt,0) + coalesce(fv.fv_ord_sales_tax_amt,0)) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS STRING) AS ih_taxAmount,
        CAST(ABS(SUM(CASE
            WHEN foh.PYMT_STATUS_ID = 5000 AND (fol.max_ln_total != fol.min_ln_total AND foh.min_ord_total != 0) THEN ABS(coalesce(fol.max_ln_total,0) - coalesce(fol.min_ln_total,0))
            ELSE
				ABS(coalesce(fv.fv_cnlled_ord_ln_sub_total,0) + coalesce(fv.fv_ord_ln_sub_total,0)) +
				ABS(coalesce(fv.fv_cnlled_ord_shipping_amt,0) + coalesce(fv.fv_ord_shipping_amt,0)) +
				ABS(coalesce(fv.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(fv.fv_ord_shipping_tax_amt,0)) +
				ABS(coalesce(fv.fv_cnlled_ord_sales_tax_amt,0) + coalesce(fv.fv_ord_sales_tax_amt,0))
        END) OVER (PARTITION BY fol.org_id, fol.ord_id)) AS STRING) AS ih_totalAmount,
        RTN.refundMethods AS refundMethods,
        pymt_line.paymentsInfo AS paymentsInfo,
        cast(NULL as string ) AS refund_notes,
        REFUND_STATUS_HISTORY AS refunds_statushistory,
        fol.UPDATED_TS AS load_time_kafka,
        fol.ETL_UPDT_TS AS load_time_adls,
        coalesce(foh.created_ts):: DATE AS order_date,
        fol.UPDATED_TS AS postedAt,
        fol.UPDATED_BY AS postedBy,
        fol.UPDATED_TS AS updated_datetime,
        CAST(fol.ORD_LN_ID AS STRING) AS lineNumber,
        'MAO' as ref_source
    FROM base_ord_line fol
        join base_ord_hdr foh ON nvl(fol.prnt_ord_id, fol.ord_id) = foh.ORD_ID AND fol.ORG_ID = foh.ORG_ID
        left join REFUND_HIST_DETAILS AS RTN ON (RTN.ORG_ID = FOL.ORG_ID AND RTN.ORD_ID = fol.ORD_ID AND RTN.ORD_LN_ID = fol.ord_ln_id)
        left join payment_grouped pymt_line ON fol.ord_id = pymt_line.ord_id AND fol.ORG_ID = pymt_line.ORG_ID
        left join product_master pm ON (fol.is_gift_card!=1 and trim(fol.item_id) = trim(pm.global_size_id) AND (CASE WHEN foh.org_id IN ('FL-CA','CH-CA') THEN '98' ELSE '81' END)=pm.banner_id)
        left join product_master_div pm_div ON (fol.is_gift_card!=1 and trim(fol.item_id) = trim(pm_div.global_size_id) AND fol.ORG_ID = pm_div.org_desc)
        left join fct_mao_ord_line_fv fv ON (fol.org_id = fv.org_id AND fol.ord_id = fv.ord_id AND fol.ord_ln_id = fv.ord_ln_id AND fv.fv_rnk = 1)
        left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_item_v itm on (trim(fol.ITEM_ID) = trim(itm.ITEM_ID))
    WHERE
        fol.is_even_exchg = 0
        AND (
            (fol.is_rtn = 1
                AND foh.PYMT_STATUS_ID IN (6000, 7000)
                AND fol.max_fulflmnt_status_id > 9000)
            OR
            (fol.appeasment_reason_cd IS NOT NULL
                AND ((foh.PYMT_STATUS_ID IN (6000, 7000)) or (foh.PYMT_STATUS_ID = 5000 and fol.max_ln_total != fol.min_ln_total AND foh.max_ord_total != 0))
                AND fol.max_fulflmnt_status_id <= 9000)
            OR
            (fol.cnl_reason_id IS NOT NULL
                AND ((foh.PYMT_STATUS_ID IN (6000, 7000)) or (foh.PYMT_STATUS_ID = 5000 and fol.max_ln_total != fol.min_ln_total AND foh.max_ord_total != 0))
                AND fol.max_fulflmnt_status_id <= 9000)
        )
);
