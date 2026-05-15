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
            CAST(NULL AS STRING) AS authResponse,
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
        -- creditCard struct: only populate if pymt_type = 'Credit Card', else null
        CASE WHEN pymt_type = 'Credit Card' THEN
          STRUCT(
            STRUCT(
              CAST(pymt_txn_req_amt AS STRING) AS authAmount,
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
            CAST(card_alias AS STRING) AS cardAlias,
            CAST(pymt_grp_id AS STRING) AS cardBin,
            CAST(txn_card_last4 AS STRING) AS cardLast4,
            CAST(card_token AS STRING) AS cardToken,
            CAST(attrib_card_expiry_dt AS STRING) AS expirationDate,
            CAST(attrib_card_type_display AS STRING) AS type
          )
        ELSE NULL END AS creditCard,
        -- giftCard struct: only populate if pymt_type = 'Gift Card', else null
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

CREATE 	OR REPLACE TEMP VIEW returns AS
(
with dim_location as (
    select * from
        (select
            loc_snum,loc_num,row_number() over (partition by lpad(loc_snum, 5, 0) order by loc_sk desc,loc_seq_num desc) loc_rnk
        from sf_gold_prod_db.location_gold_prod.dim_location_v where UPPER(banner_geo)='NA')
        where loc_rnk=1
),
base_ord_hdr_hist AS (
    SELECT *
    FROM  ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_hist_v
    WHERE doc_type_id = 'CustomerOrder'
),
base_ord_hdr AS (
    SELECT *
    FROM base_ord_hdr_hist
    QUALIFY ROW_NUMBER() OVER (PARTITION BY org_id, ord_id ORDER BY updated_ts DESC) = 1
),
base_ord_line_hist AS (
    SELECT ol.*
    FROM ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_line_hist_v ol
    JOIN base_ord_hdr oh
        ON ol.org_id = oh.org_id AND ol.ord_id = oh.ord_id
),
base_ord_line AS (
    SELECT *
    FROM base_ord_line_hist
    QUALIFY ROW_NUMBER() OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY updated_ts DESC) = 1
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
fct_mao_ord_line_stg as (
    select ord_line.*,
        case
          when COALESCE(ORD_LINE.max_fulflmnt_status_id,ORD_HDR.RTN_STATUS_ID) = '11000' then 'CREATED'
          when COALESCE(ORD_LINE.max_fulflmnt_status_id,ORD_HDR.RTN_STATUS_ID) = '18000' then 'RETURN_COMPLETE'
          when COALESCE(ORD_LINE.max_fulflmnt_status_id,ORD_HDR.RTN_STATUS_ID) = '19000' then 'CANCELLED'
          else upper(TRIM(COALESCE(ORD_LINE.max_fulflmnt_status_desc,ORD_HDR.RTN_STATUS_DESC)))
        end as return_status,
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
    from base_ord_line_hist  ord_line
    join base_ord_hdr ord_hdr on ord_line.org_id = ord_hdr.org_id and ord_line.ord_id = ord_hdr.ord_id
    where ord_hdr.doc_type_id = 'CustomerOrder'
        and ord_line.max_fulflmnt_status_id is not null
        and ord_line.max_fulflmnt_status_id > '9000'
),
fct_mao_ord_line as (
    select *
    from
      (select ord_line.*,
        row_number() over (partition by org_id,ord_id,ord_ln_id order by updated_ts desc) as ord_ln_status_rnk
      from fct_mao_ord_line_stg ord_line)
    where ord_ln_status_rnk=1
)
SELECT
  cast(nvl(ORD_LINE.prnt_ord_id,ORD_LINE.ord_id) as string) AS order_id,
  cast( DATE_FORMAT(
            TO_TIMESTAMP(ord_hdr.created_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
        )as string) AS order_datetime,
  cast(ORD_LINE.ord_id as string) AS return_id,
  cast(case trim(ORD_LINE.org_id)
    when 'FL-US'  then '21'
    when 'FL-CA'  then '45'
    when 'KFL-US' then '22'
    when 'CH-CA'  then '77'
    when 'CH-US'  then '20'
  end as string) AS company_number,
  ord_line.return_status AS return_status,
  cast(ORD_LINE.ORD_ID as string) AS return_number,
  cast(case when ord_line.is_refund_gift_card =1 or orig.is_refund_gift_card =1 then 'E_GIFT_CARD' ELSE UPPER(ORD_HDR.refund_pymt_method) END as string) AS refund_method,
  cast(case
    when upper(loc.loc_type_id) = 'STORE' then 'XSTORE'
    when upper(loc.loc_type_id) = 'DC' and upper(trim(loc.loc_addr_city)) = 'RENO' then 'RENO'
    when upper(loc.loc_type_id) = 'DC' and upper(trim(loc.loc_addr_city)) = 'CAMP HILL' then 'CAMPHILL'
    when upper(loc.loc_type_id) = 'DC' and upper(trim(loc.loc_addr_city)) = 'JUNCTION CITY' then 'JC'
    when upper(loc.loc_type_id) = 'DC' and upper(trim(loc.loc_addr_city)) = 'MILTON' then 'MILTON'
    when upper(loc.loc_type_id) = 'DC' then upper(trim(loc.loc_addr_city))
  end as string) AS return_location,
  cast(ord_exch.ord_id as string) AS exchangeOrderNumber,
  cast(ord_exch.ord_id as string) AS exchangeNumber,
  cast( DATE_FORMAT(
            TO_TIMESTAMP(coalesce(ord_hdr.confirmed_ts,ord_hdr.captured_ts), 'yyyy-MM-dd HH:mm:ss.SSS'),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
        ) as string) AS return_date,
  cast(ORD_LINE.updated_by as string) AS return_agent,
  cast(ORD_LINE.TXN_REF_ID as string) AS return_taxTransId,
  cast(ABS(coalesce(ORD_HDR.RTN_ACT_ADJ_AMT,0)) as string) AS return_act_adjustmentAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_ACT_TOTAL_CREDITS,0)) as string) AS return_act_creditsAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_discountAmount,
  cast(ABS(SUM(
    (ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)))
    - ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0))
  ) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_discountedTotalAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_ACT_TOTAL_AMT,0)) as string) AS return_act_refundTotalAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_ACT_TOTAL_AMT,0)) as string) AS return_act_totalReturnAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_ACT_EXCH_CREDIT_AMT,0)) as string) AS return_act_exchangeCreditAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_ACT_TOTAL_CC_AMT,0)) as string) AS return_act_creditCardRefundAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_ACT_TOTAL_PAYPAL_AMT,0)) as string) AS return_act_paypalRefundAmount,
  cast(case when ord_line.is_refund_gift_card =1 then 'E_GIFT_CARD' else ABS(coalesce(ORD_HDR.RFND_ACT_TOTAL_AMT,0)) end as string) AS return_act_giftCardrefundAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_shippingAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_shippingTaxAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_subTotalAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_taxAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_REQ_ADJ_AMT,0)) as string) AS req_tot_adjustmentAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_REQ_TOTAL_CREDITS,0)) as string) AS req_tot_creditsAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS req_tot_discountAmount,
  cast(ABS(SUM(
    (ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)))
    - ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0))
  ) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS req_tot_discountedTotalAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_REQ_TOTAL_AMT,0)) as string) AS req_tot_refundTotalAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_REQ_TOTAL_CREDITS,0)) as string) AS req_tot_totalReturnAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_REQ_EXCH_CREDIT_AMT,0)) as string) AS req_tot_exchangeCreditAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_REQ_TOTAL_CC_AMT,0)) as string) AS req_tot_creditCardRefundAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_REQ_TOTAL_PAYPAL_AMT,0)) as string) AS req_tot_paypalRefundAmount,
  cast(case when ord_line.is_refund_gift_card =1 then 'E_GIFT_CARD' else ABS(coalesce(ORD_HDR.RFND_ACT_TOTAL_AMT,0)) end as string) AS req_tot_giftCardrefundAmount,
  CAST(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS STRING) AS req_tot_shippingAmount,
  CAST(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS STRING) AS req_tot_shippingTaxAmount,
  CAST(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS STRING) AS req_tot_subTotalAmount,
  CAST(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS STRING) AS req_tot_taxAmount,
  cast(case when ord_hdr.pymt_status_desc in ('Refunded', 'Awaiting Refund') then ord_line.ord_id end as string) AS lines_refundNum,
  cast(null as string) AS lines_act_priceOverrideReason,
  cast(ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) as string) AS lines_act_discountAmount,
  cast((ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)))
    - ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) as string) AS lines_act_discountedTotalAmount,
  cast(ABS(coalesce(ord_line.fv_orig_unit_price, ord_line.fv_unit_price,0)) as string) AS lines_act_originalRetailPrice,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) as string) AS lines_act_shippingAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) as string) AS lines_act_shippingTaxAmount,
  cast(null as string) AS lines_act_giftBoxAmount,
  cast(null as string) AS lines_act_giftBoxTaxAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) as string) AS lines_act_subTotalAmount,
  CAST(ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) as string) AS lines_act_taxAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) as string) AS lines_act_totalAmount,
  cast(ABS(coalesce(ord_line.fv_unit_price,0)) as string) AS lines_act_unitPrice,
  CAST(case
            when ORD_LINE.is_backorderFlg=1 then 'true'
            when ORD_LINE.is_backorderFlg=0 then 'false'
        end AS STRING) AS product_backorderFlag,
  CAST(upper(case when ord_line.is_gift_card=1 then ord_line.itm_brand else pm.global_brand_desc end) AS STRING) AS product_brand,
  CAST(case when ord_line.is_gift_card=1 then ord_line.itm_dept_name else pm.fob_desc end AS STRING) AS product_category,
  CAST(case when ord_line.is_gift_card=1 then ord_line.itm_color_desc else pm.desc_long_2 end AS STRING) as product_color,
  CAST(pm.desc AS STRING) AS product_description,
  CAST(ORD_LINE.small_image_u_r_i AS STRING) AS product_image,
  CAST(case
            when ORD_HDR.is_prepaid=0 then 'false'
        end AS STRING) AS product_isCollectUpFront,
  CAST(CASE
            WHEN ord_line.is_launch_sku_flg = 1 THEN 'true'
            WHEN ord_line.is_launch_sku_flg = 0 THEN 'false'
        END AS STRING) AS product_launchSkuFlag,
  CAST(ORD_LINE.itm_desc AS STRING) AS product_name,
  CAST(case when ord_line.is_gift_card=1 then 'GFT' else pm.designator_id end AS STRING) AS productDesignator,
  --CAST(pm.internal_product_number AS STRING) AS product_number,
  cast( trim(CASE
                WHEN ord_line.item_id = 'ECARD20' THEN '2138264'
                WHEN ord_line.item_id = 'ECARD21' THEN '2138265'
                WHEN ord_line.item_id = 'ECARD22' THEN '2138266'
                WHEN ord_line.item_id = 'ECARD45' THEN '20'
                WHEN ord_line.item_id = 'ECARD77' THEN '2000003'
                WHEN ord_line.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
                WHEN ord_line.org_id in ('FL-US','KFL-US','CH-US') THEN pm.internal_product_number
                else ord_line.item_id
            END) as string) as product_number,
  CAST(ORD_LINE.product_type AS STRING) AS product_type,
  CAST(case when ord_line.is_gift_card=1 then ord_line.itm_size else pm.legacy_size_desc end AS STRING) AS product_size, --CAST(ORD_LINE.itm_size AS STRING) AS product_size,
  CAST(case -- for gift cards, use item_id as SKU to differentiate from regular products since multiple gift cards can have same designator
              when ord_line.is_gift_card=1 then ord_line.item_id
              WHEN ord_hdr.CHANNEL !='XSTORE' AND  ord_hdr.org_id in ('FL-US','KFL-US','CH-US') then pm.online_us_sku
              WHEN ord_hdr.CHANNEL !='XSTORE' AND  ord_hdr.org_id in ('FL-CA','CH-CA') then pm.online_ca_sku
              WHEN ord_hdr.CHANNEL ='XSTORE' THEN PM_DIV.legacy_sku_size
              else itm.COLOR
    end AS STRING) AS product_sku,
  CAST(case when ord_line.is_gift_card=1 then ord_line.itm_tax_cd else pm.tax_code end AS STRING) AS product_taxCode, --CAST(ORD_LINE.itm_tax_cd AS STRING) AS product_taxCode,
  CAST(case when ord_line.max_fulflmnt_status_id in ('9000','19000') then ord_line.orig_ord_qty else ord_line.QTY end AS STRING) AS lines_qty,
  CASE
    WHEN trim(ORD_LINE.org_id) IN ('CH-US','FL-US','KFL-US','FL-CA','CH-CA') THEN
        CASE
            WHEN upper(ord_line.rtn_reason) = 'COLOR MISMATCH' THEN 'PC'
            WHEN upper(ord_line.rtn_reason) = 'BOSS WRONG ITEM' THEN 'BW'
            WHEN upper(ord_line.rtn_reason) = 'DROP SHIP WRONG ITEM' THEN 'DI'
            WHEN upper(ord_line.rtn_reason) = 'EMBROIDERY QUALITY' THEN 'EQ'
            WHEN upper(ord_line.rtn_reason) = 'EB4KIDS FIT GUARNTEE' THEN 'FG'
            WHEN upper(ord_line.rtn_reason) = 'DO NOT USE' THEN 'LS'
            WHEN upper(ord_line.rtn_reason) = 'WRONG ART GRAPHIC' THEN 'PA'
            WHEN upper(ord_line.rtn_reason) = 'PERSONALIZED-BUYER' THEN 'PB'
            WHEN upper(ord_line.rtn_reason) = 'WRONG COLOR' THEN 'PC'
            WHEN upper(ord_line.rtn_reason) = 'PERSONALZD WRONG ITM' THEN 'PI'
            WHEN upper(ord_line.rtn_reason) = 'PERSONALZD UNWANTED' THEN 'PN'
            WHEN upper(ord_line.rtn_reason) = 'PRINTING QUALITY' THEN 'PQ'
            WHEN upper(ord_line.rtn_reason) = 'MIS-SPELLING' THEN 'PS'
            WHEN upper(ord_line.rtn_reason) = 'PERSONALIZED-AGENT' THEN 'PU'
            WHEN upper(ord_line.rtn_reason) = 'DIDN''T HOLD UP' THEN 'PW'
            WHEN upper(ord_line.rtn_reason) = 'STORE DID NOT REFUND' THEN 'SN'
            WHEN upper(ord_line.rtn_reason) = 'STORE DID REFUND' THEN 'SY'
            WHEN upper(ord_line.rtn_reason) = 'USED CLEAT' THEN 'UC'
            WHEN upper(ord_line.rtn_reason) = 'USED SHOE NOT DEFECT' THEN 'US'
            WHEN upper(ord_line.rtn_reason) = 'WRONG ITEM ORDERED' THEN 'WO'
            WHEN upper(ord_line.rtn_reason) = 'DAMAGED IN TRANSIT (throw in trash)' THEN 'WT'
            WHEN upper(ord_line.rtn_reason) = 'XSTORE RETURN' THEN 'XX'
            WHEN upper(ord_line.rtn_reason) IN ('DAMAGED','DEFECTIVE ITEM','DEFECTIVE ITEM NO RETURN FEE','FLX DEFECTIVE ITEM','FLX DEFECTIVE ITEM NO RETURN FEE','["DEFECTIVE ITEM"]') THEN 'DQ'
            WHEN upper(ord_line.rtn_reason) IN ('WRONG ITEM','I ORDERED THE WRONG ITEM','FLX I ORDERED THE WRONG ITEM','WRONG ITEM SHIPPED','FLX WRONG ITEM SHIPPED','["WRONG ITEM SHIPPED"]') THEN 'WI'
            WHEN upper(ord_line.rtn_reason) IN ('ITEM NOT AS DESCRIBE','ITEM NOT AS DESCRIBED/ PICTURED','FLX ITEM NOT AS DESCRIBED/ PICTURED','["ITEM NOT AS DESCRIBED/ PICTURED"]') THEN 'WD'
            WHEN upper(ord_line.rtn_reason) IN ('TOO BIG / LONG','TOO BIG LONG','TOO BIG/ LONG','TOO BIG/ LONG NO RETURN FEE','TOO LONG','ITEM DOES NOT FIT','FLX TOO BIG/ LONG','FLX TOO BIG/ LONG NO RETURN FEE') THEN 'TB'
            WHEN upper(ord_line.rtn_reason) IN ('TOO SMALL / SHORT','TOO SMALL/ SHORT','TOO SMALL/SHORT','TOO SMALL/SHORT NO RETURN FEE','FLX TOO SMALL/ SHORT','FLX TOO SMALL/SHORT','FLX TOO SMALL/ SHORT NO RETURN FEE') THEN 'TS'
            WHEN upper(ord_line.rtn_reason) IN ('TOO NARROW','TOO NARROW NO RETURN FEE','FLX TOO NARROW','["TOO NARROW"]') THEN 'TN'
            WHEN upper(ord_line.rtn_reason) IN ('TOO WIDE','TOO WIDE NO RETURN FEE','FLX TOO WIDE','FLX TOO WIDE NO RETURN FEE','["TOO WIDE"]') THEN 'TW'
            WHEN upper(ord_line.rtn_reason) IN ('UNWANTED ITEM','UNWANTED / CHANGED MY MIND','UNWANTED/ CHANGED MY MIND','UNWANTED/ CHANGED MY MIND NO RETURN FEE','FLX UNWANTED/ CHANGED MY MIND') THEN 'U'
            WHEN upper(ord_line.rtn_reason) = 'LOST PACKAGE' THEN 'LP'
            WHEN upper(ord_line.rtn_reason) = 'NOT DELIVERABLE' THEN 'ND'
            WHEN upper(ord_line.rtn_reason) IN ('RETURN','RETURNREASON','INSTORE CS RETURNS','STORERETURN RETURNS','DC RETURNS') THEN 'IS'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '501-%' THEN '501'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '502-%' THEN '502'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '503-%' THEN '503'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '504-%' THEN '504'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '505-%' THEN '505'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '506-%' THEN '506'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '507-%' THEN '507'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '508-%' THEN '508'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '509-%' THEN '509'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '510-%' THEN '510'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '511-%' THEN '511'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '512-%' THEN '512'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '513-%' THEN '513'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '514-%' THEN '514'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '515-%' THEN '515'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '521-%' THEN '521'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '522-%' THEN '522'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '523-%' THEN '523'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '525-%' THEN '525'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '526-%' THEN '526'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '527-%' THEN '527'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '529-%' THEN '529'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '534-%' THEN '534'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '539-%' THEN '539'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '999-%' THEN '999'
        END
  END AS lines_reasonCode,
  CASE
    WHEN trim(ORD_LINE.org_id) IN ('CH-US','FL-US','KFL-US','FL-CA','CH-CA') THEN
        CASE
            WHEN upper(ord_line.rtn_reason) = 'BOSS WRONG ITEM' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 390   -- company 20
					WHEN 'FL-US' THEN 294   -- company 21
					WHEN 'KFL-US' THEN 262  -- company 22
					WHEN 'FL-CA' THEN 326   -- company 45
					WHEN 'CH-CA' THEN 548   -- company 77
				END
			WHEN upper(ord_line.rtn_reason) = 'DROP SHIP WRONG ITEM' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 391
					WHEN 'FL-US' THEN 295
					WHEN 'KFL-US' THEN 263
					WHEN 'FL-CA' THEN 327
					WHEN 'CH-CA' THEN 549
				END
			WHEN upper(ord_line.rtn_reason) = 'EMBROIDERY QUALITY' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 393
					WHEN 'FL-US' THEN 297
					WHEN 'KFL-US' THEN 265
					WHEN 'FL-CA' THEN 329
					WHEN 'CH-CA' THEN 551
				END
			WHEN upper(ord_line.rtn_reason) = 'WRONG ITEM' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 419
					WHEN 'FL-US' THEN 323
					WHEN 'KFL-US' THEN 291
					WHEN 'FL-CA' THEN 355
					WHEN 'CH-CA' THEN 577
				END
			WHEN upper(ord_line.rtn_reason) = 'ITEM NOT AS DESCRIBE' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 418
					WHEN 'FL-US' THEN 322
					WHEN 'KFL-US' THEN 290
					WHEN 'FL-CA' THEN 354
					WHEN 'CH-CA' THEN 576
				END
			WHEN upper(ord_line.rtn_reason) = 'UNWANTED ITEM' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 415
					WHEN 'FL-US' THEN 319
					WHEN 'KFL-US' THEN 287
					WHEN 'FL-CA' THEN 351
					WHEN 'CH-CA' THEN 573
				END
			WHEN upper(ord_line.rtn_reason) = 'INSTORE CS RETURNS' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 395
					WHEN 'FL-US' THEN 299
					WHEN 'KFL-US' THEN 267
					WHEN 'FL-CA' THEN 331
					WHEN 'CH-CA' THEN 553
				END
			WHEN upper(ord_line.rtn_reason) = 'XSTORE RETURN' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'FL-US' THEN 762
				END
			WHEN upper(ord_line.rtn_reason) = 'WRONG ART GRAPHIC' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 400
					WHEN 'FL-US' THEN 304
					WHEN 'KFL-US' THEN 272
					WHEN 'FL-CA' THEN 336
					WHEN 'CH-CA' THEN 558
				END
			WHEN upper(ord_line.rtn_reason) = 'PERSONALIZED-BUYER' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 401
					WHEN 'FL-US' THEN 305
					WHEN 'KFL-US' THEN 273
					WHEN 'FL-CA' THEN 337
					WHEN 'CH-CA' THEN 559
				END
			WHEN upper(ord_line.rtn_reason) = 'PERSONALZD WRONG ITM' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 403
					WHEN 'FL-US' THEN 307
					WHEN 'KFL-US' THEN 275
					WHEN 'FL-CA' THEN 339
					WHEN 'CH-CA' THEN 561
				END
			WHEN upper(ord_line.rtn_reason) = 'PERSONALZD UNWANTED' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 404
					WHEN 'FL-US' THEN 308
					WHEN 'KFL-US' THEN 276
					WHEN 'FL-CA' THEN 340
					WHEN 'CH-CA' THEN 562
				END
			WHEN upper(ord_line.rtn_reason) = 'PRINTING QUALITY' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 405
					WHEN 'FL-US' THEN 309
					WHEN 'KFL-US' THEN 277
					WHEN 'FL-CA' THEN 341
					WHEN 'CH-CA' THEN 563
				END
			WHEN upper(ord_line.rtn_reason) = 'MIS-SPELLING' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 406
					WHEN 'FL-US' THEN 310
					WHEN 'KFL-US' THEN 278
					WHEN 'FL-CA' THEN 342
					WHEN 'CH-CA' THEN 564
				END
			WHEN upper(ord_line.rtn_reason) = 'PERSONALIZED-AGENT' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 407
					WHEN 'FL-US' THEN 311
					WHEN 'KFL-US' THEN 279
					WHEN 'FL-CA' THEN 343
					WHEN 'CH-CA' THEN 565
				END
			WHEN upper(ord_line.rtn_reason) = 'DIDN''T HOLD UP' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 408
					WHEN 'FL-US' THEN 312
					WHEN 'KFL-US' THEN 280
					WHEN 'FL-CA' THEN 344
					WHEN 'CH-CA' THEN 566
				END
			WHEN upper(ord_line.rtn_reason) = 'STORE DID NOT REFUND' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 409
					WHEN 'FL-US' THEN 313
					WHEN 'KFL-US' THEN 281
					WHEN 'FL-CA' THEN 345
					WHEN 'CH-CA' THEN 567
				END
			WHEN upper(ord_line.rtn_reason) = 'STORE DID REFUND' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 410
					WHEN 'FL-US' THEN 314
					WHEN 'KFL-US' THEN 282
					WHEN 'FL-CA' THEN 346
					WHEN 'CH-CA' THEN 568
				END
			WHEN upper(ord_line.rtn_reason) = 'USED CLEAT' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 416
					WHEN 'FL-US' THEN 320
					WHEN 'KFL-US' THEN 288
					WHEN 'FL-CA' THEN 352
					WHEN 'CH-CA' THEN 574
				END
			WHEN upper(ord_line.rtn_reason) = 'USED SHOE NOT DEFECT' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 417
					WHEN 'FL-US' THEN 321
					WHEN 'KFL-US' THEN 289
					WHEN 'FL-CA' THEN 353
					WHEN 'CH-CA' THEN 575
				END
			WHEN upper(ord_line.rtn_reason) = 'WRONG ITEM ORDERED' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 420
					WHEN 'FL-US' THEN 324
					WHEN 'KFL-US' THEN 292
					WHEN 'FL-CA' THEN 356
					WHEN 'CH-CA' THEN 578
				END
			WHEN upper(ord_line.rtn_reason) = 'DAMAGED IN TRANSIT (throw in trash)' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 421
					WHEN 'FL-US' THEN 325
					WHEN 'KFL-US' THEN 293
					WHEN 'FL-CA' THEN 357
					WHEN 'CH-CA' THEN 579
				END
			WHEN upper(ord_line.rtn_reason) = 'DO NOT USE' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 397
					WHEN 'FL-US' THEN 301
					WHEN 'KFL-US' THEN 269
					WHEN 'FL-CA' THEN 333
					WHEN 'CH-CA' THEN 555
				END
            WHEN upper(ord_line.rtn_reason) = 'COLOR MISMATCH' THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 402
                    WHEN 'FL-US' THEN 306
                    WHEN 'KFL-US' THEN 274
                    WHEN 'FL-CA' THEN 338
                    WHEN 'CH-CA' THEN 560
                END
            WHEN upper(ord_line.rtn_reason) IN ('DAMAGED','DEFECTIVE ITEM','DEFECTIVE ITEM NO RETURN FEE','FLX DEFECTIVE ITEM','FLX DEFECTIVE ITEM NO RETURN FEE','["DEFECTIVE ITEM"]') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 392
                    WHEN 'FL-US' THEN 296
                    WHEN 'KFL-US' THEN 264
                    WHEN 'FL-CA' THEN 328
                    WHEN 'CH-CA' THEN 550
                END
            WHEN upper(ord_line.rtn_reason) IN ('I ORDERED THE WRONG ITEM','FLX I ORDERED THE WRONG ITEM','WRONG ITEM SHIPPED','FLX WRONG ITEM SHIPPED','["WRONG ITEM SHIPPED"]') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 419
                    WHEN 'FL-US' THEN 323
                    WHEN 'KFL-US' THEN 291
                    WHEN 'FL-CA' THEN 355
                    WHEN 'CH-CA' THEN 577
                END
            WHEN upper(ord_line.rtn_reason) IN ('ITEM NOT AS DESCRIBED/ PICTURED','FLX ITEM NOT AS DESCRIBED/ PICTURED','["ITEM NOT AS DESCRIBED/ PICTURED"]') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 418
                    WHEN 'FL-US' THEN 322
                    WHEN 'KFL-US' THEN 290
                    WHEN 'FL-CA' THEN 354
                    WHEN 'CH-CA' THEN 576
                END
            WHEN upper(ord_line.rtn_reason) IN ('TOO BIG / LONG','TOO BIG LONG','TOO BIG/ LONG','TOO BIG/ LONG NO RETURN FEE','TOO LONG','ITEM DOES NOT FIT','FLX TOO BIG/ LONG','FLX TOO BIG/ LONG NO RETURN FEE') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 411
                    WHEN 'FL-US' THEN 315
                    WHEN 'KFL-US' THEN 283
                    WHEN 'FL-CA' THEN 347
                    WHEN 'CH-CA' THEN 569
                END
            WHEN upper(ord_line.rtn_reason) IN ('TOO SMALL / SHORT','TOO SMALL/ SHORT','TOO SMALL/SHORT','TOO SMALL/SHORT NO RETURN FEE','FLX TOO SMALL/ SHORT','FLX TOO SMALL/SHORT','FLX TOO SMALL/ SHORT NO RETURN FEE') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 413
                    WHEN 'FL-US' THEN 317
                    WHEN 'KFL-US' THEN 285
                    WHEN 'FL-CA' THEN 349
                    WHEN 'CH-CA' THEN 571
                END
            WHEN upper(ord_line.rtn_reason) IN ('TOO NARROW','TOO NARROW NO RETURN FEE','FLX TOO NARROW','["TOO NARROW"]') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 412
                    WHEN 'FL-US' THEN 316
                    WHEN 'KFL-US' THEN 284
                    WHEN 'FL-CA' THEN 348
                    WHEN 'CH-CA' THEN 570
                END
            WHEN upper(ord_line.rtn_reason) IN ('TOO WIDE','TOO WIDE NO RETURN FEE','FLX TOO WIDE','FLX TOO WIDE NO RETURN FEE','["TOO WIDE"]') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 414
                    WHEN 'FL-US' THEN 318
                    WHEN 'KFL-US' THEN 286
                    WHEN 'FL-CA' THEN 350
                    WHEN 'CH-CA' THEN 572
                END
            WHEN upper(ord_line.rtn_reason) IN ('UNWANTED / CHANGED MY MIND','UNWANTED/ CHANGED MY MIND','UNWANTED/ CHANGED MY MIND NO RETURN FEE','FLX UNWANTED/ CHANGED MY MIND') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 415
                    WHEN 'FL-US' THEN 319
                    WHEN 'KFL-US' THEN 287
                    WHEN 'FL-CA' THEN 351
                    WHEN 'CH-CA' THEN 573
                END
            WHEN upper(ord_line.rtn_reason) = 'LOST PACKAGE' THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 398
                    WHEN 'FL-US' THEN 302
                    WHEN 'KFL-US' THEN 270
                    WHEN 'FL-CA' THEN 334
                    WHEN 'CH-CA' THEN 556
                END
            WHEN upper(ord_line.rtn_reason) = 'NOT DELIVERABLE' THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 399
                    WHEN 'FL-US' THEN 303
                    WHEN 'KFL-US' THEN 271
                    WHEN 'FL-CA' THEN 335
                    WHEN 'CH-CA' THEN 557
                END
            WHEN upper(ord_line.rtn_reason) IN ('RETURN','RETURNREASON','STORERETURN RETURNS','DC RETURNS') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 395
                    WHEN 'FL-US' THEN 299
                    WHEN 'KFL-US' THEN 267
                    WHEN 'FL-CA' THEN 331
                    WHEN 'CH-CA' THEN 553
                END
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '501-%' THEN 763
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '502-%' THEN 764
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '503-%' THEN 765
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '504-%' THEN 766
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '505-%' THEN 767
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '506-%' THEN 768
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '507-%' THEN 769
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '508-%' THEN 770
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '509-%' THEN 771
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '510-%' THEN 772
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '511-%' THEN 773
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '512-%' THEN 774
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '513-%' THEN 775
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '514-%' THEN 776
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '515-%' THEN 777
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '521-%' THEN 778
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '522-%' THEN 779
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '523-%' THEN 780
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '525-%' THEN 781
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '526-%' THEN 782
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '527-%' THEN 783
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '529-%' THEN 784
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '534-%' THEN 785
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '539-%' THEN 786
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '999-%' THEN 787
        END
  END AS lines_reasonCodeId,
  cast(ORD_HDR.ccy_cd as string) AS currencyIso,
  CAST(NULL AS STRING) AS req_lines_priceOverrideReason,
  cast(ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) as string) AS req_lines_discountAmount,
  cast((ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)))
    - ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) as string) AS req_lines_discountedTotalAmount,
  cast(ABS(coalesce(ord_line.fv_orig_unit_price, ord_line.fv_unit_price,0)) as string) AS req_lines_originalRetailPrice,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) as string) AS req_lines_shippingAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) as string) AS req_lines_shippingTaxAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) as string) AS req_lines_subTotalAmount,
  CAST(ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) as string) AS req_lines_taxAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) as string) as req_lines_totalAmount,
  CAST(NULL AS STRING) AS req_lines_giftBoxAmount,
  CAST(NULL AS STRING) AS req_lines_giftBoxTaxAmount,
  cast(ABS(coalesce(ord_line.fv_unit_price,0)) as string) AS req_lines_unitPrice,
  CAST(CASE
        WHEN trim(ORD_LINE.org_id) = 'FL-US' AND REGEXP_LIKE(UPPER(TRIM(REPLACE(REPLACE(rtn_reason,'["',''),'"]',''))),'^[0-9]{3}-') THEN 'false'
        WHEN UPPER(TRIM(rtn_reason)) IS NOT NULL THEN 'true'
        ELSE 'true'
    END AS STRING) AS restockable,
  cast(ORD_HDR.inv_id as string) AS taxInvoiceNum,
  coalesce(ord_hdr.credits_info,'[]') as credits,
  coalesce(ord_line.rtn_fee_info,'[]') AS adjustments,
  pymt_line.PaymentsInfo as PaymentsInfo,
  cast(DATE_FORMAT(
            TO_TIMESTAMP(ORD_LINE.UPDATED_TS, 'yyyy-MM-dd HH:mm:ss.SSS'),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
        )as string) AS postedAt,
  cast(ORD_LINE.UPDATED_BY as string) AS postedBy,
  ORD_LINE.UPDATED_TS AS load_time_kafka,
  ORD_LINE.etl_updt_TS AS load_time_adls,
  to_date(ord_hdr.created_ts) AS order_date,
  ORD_LINE.UPDATED_TS AS updated_datetime,
  CAST(CASE
    WHEN upper(loc.loc_type_id) = 'DC' then NULL
    ELSE dim_loc.loc_num
    END
    AS STRING) AS returningStore,
  cast(orig.xstore_ord_id as string) AS xstoreTransactionNumber,
  CAST(case when ORD_LINE.is_loyalty_disc=1 then 'true' end AS STRING) AS loyaltyDiscount,
  coalesce(ord_line.ord_coupons,'[]') as req_tot_discounts,
	coalesce(ord_line.ord_coupons,'[]') as return_act_discounts,
  cast(ORD_LINE.ord_ln_id as string) as lines_lineNumber,
  'MAO' as ref_source
from fct_mao_ord_line ORD_LINE
  join base_ord_hdr  ORD_HDR on (ORD_LINE.ORG_ID = ORD_HDR.ORG_ID and ORD_LINE.ORD_ID = ORD_HDR.ORD_ID)
  left join payment_grouped pymt_line on (ORD_LINE.ORG_ID = pymt_line.ORG_ID and ord_line.ord_id = pymt_line.ord_id)
  left join product_master pm ON ((ord_line.is_gift_card!=1 and trim(ord_line.item_id) = trim(pm.global_size_id) AND (CASE WHEN ord_hdr.ORG_ID IN ('FL-CA','CH-CA') THEN '98' ELSE '81' END)=pm.banner_id))
  left join product_master_div pm_div ON (ord_line.is_gift_card!=1 and trim(ord_line.item_id) = trim(pm_div.global_size_id) AND ord_line.ORG_ID = pm_div.org_desc)
  left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_item_v itm on (trim(ord_line.ITEM_ID) = trim(itm.ITEM_ID))
  left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v loc on (coalesce(ord_line.ship_to_loc_id,ord_line.physical_org_id) = loc.loc_id)
  left join dim_location dim_loc on (lpad(dim_loc.loc_snum,5,0) = lpad(coalesce(ord_line.ship_to_loc_id,ord_line.physical_org_id),5,0))
  left join fct_exchange_orders ord_exch on (ord_line.ORG_ID = ord_exch.ORG_ID and  ord_line.ORD_ID = ord_exch.PRNT_ORD_ID and ord_line.ord_ln_id=ord_exch.prnt_ord_ln_id)
  left join fct_original_orders orig on (ord_line.org_id=orig.org_id and ord_line.prnt_ord_id=orig.ord_id and  ord_line.prnt_ord_ln_id=orig.ord_ln_id)
where
  ord_hdr.doc_type_id = 'CustomerOrder'
  and ord_line.etl_updt_ts > '${lookback_date}'

);

create or replace temp view returns_landing as
(
with
  base_ord_hdr_hist AS (
    SELECT *
    FROM ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_hist_v
    WHERE doc_type_id = 'CustomerOrder'
  ),
  base_ord_hdr AS (
    SELECT *
    FROM base_ord_hdr_hist
    QUALIFY ROW_NUMBER() OVER (PARTITION BY org_id, ord_id ORDER BY updated_ts DESC) = 1
  ),
  base_ord_line_hist AS (
    SELECT ol.*
    FROM ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_line_hist_v ol
    JOIN base_ord_hdr oh
        ON ol.org_id = oh.org_id AND ol.ord_id = oh.ord_id
  ),
  base_ord_line AS (
    SELECT *
    FROM base_ord_line_hist
    QUALIFY ROW_NUMBER() OVER (PARTITION BY org_id, ord_id, ord_ln_id ORDER BY updated_ts DESC) = 1
  ),
  fct_mao_ord_line_stg as (
    select ord_line.*,
        case
          when COALESCE(ORD_LINE.max_fulflmnt_status_id,ORD_HDR.RTN_STATUS_ID) = '11000' then 'CREATED'
          when COALESCE(ORD_LINE.max_fulflmnt_status_id,ORD_HDR.RTN_STATUS_ID) = '18000' then 'RETURN_COMPLETE'
          when COALESCE(ORD_LINE.max_fulflmnt_status_id,ORD_HDR.RTN_STATUS_ID) = '19000' then 'CANCELLED'
          else upper(TRIM(COALESCE(ORD_LINE.max_fulflmnt_status_desc,ORD_HDR.RTN_STATUS_DESC)))
        end as return_status,
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
    from
  	  base_ord_line_hist  ord_line
      join base_ord_hdr ord_hdr on ord_line.org_id = ord_hdr.org_id and ord_line.ord_id = ord_hdr.ord_id
    where
      ord_hdr.doc_type_id = 'CustomerOrder'
      and ord_line.max_fulflmnt_status_id is not null
	  and ord_line.max_fulflmnt_status_id>'9000'
	  and ord_line.etl_updt_ts > '${lookback_date}'
  ),
  fct_mao_ord_line as (
    select *
    from
      (select ord_line.*,
        row_number() over (partition by org_id,ord_id,ord_ln_id,return_status order by updated_ts desc) as ord_ln_status_rnk
      from fct_mao_ord_line_stg ord_line)
    where ord_ln_status_rnk=1
  ),
dim_location as (
    select * from
        (select
            loc_snum,loc_num,row_number() over (partition by lpad(loc_snum, 5, 0) order by loc_sk desc,loc_seq_num desc) loc_rnk
        from sf_gold_prod_db.location_gold_prod.dim_location_v where UPPER(banner_geo)='NA')
        where loc_rnk=1
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
)
SELECT
  cast(nvl(ORD_LINE.prnt_ord_id,ORD_LINE.ord_id) as string) AS order_id,
  ord_hdr.created_ts AS order_datetime,
  cast(ORD_LINE.ord_id as string) AS return_id,
  cast(case trim(ORD_LINE.org_id)
    when 'FL-US'  then '21'
    when 'FL-CA'  then '45'
    when 'KFL-US' then '22'
    when 'CH-CA'  then '77'
    when 'CH-US'  then '20'
  end as string) AS company_number,
  ord_line.return_status AS return_status,
  cast(ORD_LINE.ORD_ID as string) AS return_number,
  cast(case when ord_line.is_refund_gift_card =1 or orig.is_refund_gift_card =1 then 'E_GIFT_CARD' ELSE UPPER(ORD_HDR.refund_pymt_method) END as string) AS refund_method,
  cast(case
    when upper(loc.loc_type_id) = 'STORE' then 'XSTORE'
    when upper(loc.loc_type_id) = 'DC' and upper(trim(loc.loc_addr_city)) = 'RENO' then 'RENO'
    when upper(loc.loc_type_id) = 'DC' and upper(trim(loc.loc_addr_city)) = 'CAMP HILL' then 'CAMPHILL'
    when upper(loc.loc_type_id) = 'DC' and upper(trim(loc.loc_addr_city)) = 'JUNCTION CITY' then 'JC'
    when upper(loc.loc_type_id) = 'DC' and upper(trim(loc.loc_addr_city)) = 'MILTON' then 'MILTON'
    when upper(loc.loc_type_id) = 'DC' then upper(trim(loc.loc_addr_city))
  end as string) AS return_location,
  cast(ord_exch.ord_id as string) AS exchangeOrderNumber,
  cast(ord_exch.ord_id as string) AS exchangeNumber,
  coalesce(ord_hdr.confirmed_ts,ord_hdr.captured_ts) AS return_date,
  cast(ORD_LINE.updated_by as string) AS return_agent,
  cast(ORD_LINE.TXN_REF_ID as string) AS return_taxTransId,
  cast(ABS(coalesce(ORD_HDR.RTN_ACT_ADJ_AMT,0)) as string) AS return_act_adjustmentAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_ACT_TOTAL_CREDITS,0)) as string) AS return_act_creditsAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_discountAmount,
  cast(ABS(SUM(
    (ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)))
    - ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0))
  ) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_discountedTotalAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_ACT_TOTAL_AMT,0)) as string) AS return_act_refundTotalAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_ACT_TOTAL_AMT,0)) as string) AS return_act_totalReturnAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_ACT_EXCH_CREDIT_AMT,0)) as string) AS return_act_exchangeCreditAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_ACT_TOTAL_CC_AMT,0)) as string) AS return_act_creditCardRefundAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_ACT_TOTAL_PAYPAL_AMT,0)) as string) AS return_act_paypalRefundAmount,
  cast(case when ord_line.is_refund_gift_card =1 then 'E_GIFT_CARD' else ABS(coalesce(ORD_HDR.RFND_ACT_TOTAL_AMT,0)) end as string) AS return_act_giftCardrefundAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_shippingAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_shippingTaxAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_subTotalAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS return_act_taxAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_REQ_ADJ_AMT,0)) as string) AS req_tot_adjustmentAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_REQ_TOTAL_CREDITS,0)) as string) AS req_tot_creditsAmount,
  cast(ABS(SUM(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS req_tot_discountAmount,
  cast(ABS(SUM(
    (ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)))
    - ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0))
  ) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) as string) AS req_tot_discountedTotalAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_REQ_TOTAL_AMT,0)) as string) AS req_tot_refundTotalAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_REQ_TOTAL_CREDITS,0)) as string) AS req_tot_totalReturnAmount,
  cast(ABS(coalesce(ORD_HDR.RTN_REQ_EXCH_CREDIT_AMT,0)) as string) AS req_tot_exchangeCreditAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_REQ_TOTAL_CC_AMT,0)) as string) AS req_tot_creditCardRefundAmount,
  cast(ABS(coalesce(ORD_HDR.RFND_REQ_TOTAL_PAYPAL_AMT,0)) as string) AS req_tot_paypalRefundAmount,
  cast(case when ord_line.is_refund_gift_card =1 then 'E_GIFT_CARD' else ABS(coalesce(ORD_HDR.RFND_ACT_TOTAL_AMT,0)) end as string) AS req_tot_giftCardrefundAmount,
  CAST(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS STRING) AS req_tot_shippingAmount,
  CAST(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS STRING) AS req_tot_shippingTaxAmount,
  CAST(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS STRING) AS req_tot_subTotalAmount,
  CAST(ABS(SUM(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) OVER (PARTITION BY ord_line.org_id, ord_line.ord_id)) AS STRING) AS req_tot_taxAmount,
  cast(case when ord_hdr.pymt_status_desc in ('Refunded', 'Awaiting Refund') then ord_line.ord_id end as string) AS lines_refundNum,
  cast(null as string) AS lines_act_priceOverrideReason,
  cast(ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) as string) AS lines_act_discountAmount,
  cast((ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)))
    - ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) as string) AS lines_act_discountedTotalAmount,
  cast(ABS(coalesce(ord_line.fv_orig_unit_price, ord_line.fv_unit_price,0)) as string) AS lines_act_originalRetailPrice,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) as string) AS lines_act_shippingAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) as string) AS lines_act_shippingTaxAmount,
  cast(null as string) AS lines_act_giftBoxAmount,
  cast(null as string) AS lines_act_giftBoxTaxAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) as string) AS lines_act_subTotalAmount,
  CAST(ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) as string) AS lines_act_taxAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) as string) AS lines_act_totalAmount,
  cast(ABS(coalesce(ord_line.fv_unit_price,0)) as string) AS lines_act_unitPrice,
  CAST(case
            when ORD_LINE.is_backorderFlg=1 then 'true'
            when ORD_LINE.is_backorderFlg=0 then 'false'
        end AS STRING) AS product_backorderFlag,
  CAST(upper(case when ord_line.is_gift_card=1 then ord_line.itm_brand else pm.global_brand_desc end) AS STRING) AS product_brand,
  CAST(case when ord_line.is_gift_card=1 then ord_line.itm_dept_name else pm.fob_desc end AS STRING) AS product_category,
  CAST(case when ord_line.is_gift_card=1 then ord_line.itm_color_desc else pm.desc_long_2 end AS STRING) as product_color,
  CAST(pm.desc AS STRING) AS product_description,
  CAST(ORD_LINE.small_image_u_r_i AS STRING) AS product_image,
  CAST(case
            when ORD_HDR.is_prepaid=0 then 'false'
        end AS STRING) AS product_isCollectUpFront,
  CAST(CASE
            WHEN ord_line.is_launch_sku_flg = 1 THEN 'true'
            WHEN ord_line.is_launch_sku_flg = 0 THEN 'false'
        END AS STRING) AS product_launchSkuFlag,
  CAST(ORD_LINE.itm_desc AS STRING) AS product_name,
  CAST(case when ord_line.is_gift_card=1 then 'GFT' else pm.designator_id end AS STRING) AS productDesignator,
    cast( trim(CASE
                WHEN ord_line.item_id = 'ECARD20' THEN '2138264'
                WHEN ord_line.item_id = 'ECARD21' THEN '2138265'
                WHEN ord_line.item_id = 'ECARD22' THEN '2138266'
                WHEN ord_line.item_id = 'ECARD45' THEN '20'
                WHEN ord_line.item_id = 'ECARD77' THEN '2000003'
                WHEN ord_line.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
                WHEN ord_line.org_id in ('FL-US','KFL-US','CH-US') THEN pm.internal_product_number
                else ord_line.item_id
            END) as string) as product_number,
  CAST(ORD_LINE.product_type AS STRING) AS product_type,
  CAST(case when ord_line.is_gift_card=1 then ord_line.itm_size else pm.legacy_size_desc end AS STRING) AS product_size,
  CAST(case -- for gift cards, use item_id as SKU to differentiate from regular products since multiple gift cards can have same designator
              when ord_line.is_gift_card=1 then ord_line.item_id
              WHEN ord_hdr.CHANNEL !='XSTORE' AND  ord_hdr.org_id in ('FL-US','KFL-US','CH-US') then pm.online_us_sku
              WHEN ord_hdr.CHANNEL !='XSTORE' AND  ord_hdr.org_id in ('FL-CA','CH-CA') then pm.online_ca_sku
              WHEN ord_hdr.CHANNEL ='XSTORE' THEN PM_DIV.legacy_sku_size
              else itm.COLOR
    end AS STRING) AS product_sku,
  CAST(case when ord_line.is_gift_card=1 then ord_line.itm_tax_cd else pm.tax_code end AS STRING) AS product_taxCode,
  CAST(case when ord_line.max_fulflmnt_status_id in ('9000','19000') then ord_line.orig_ord_qty else ord_line.QTY end AS STRING) AS lines_qty,
  CASE
    WHEN trim(ORD_LINE.org_id) IN ('CH-US','FL-US','KFL-US','FL-CA','CH-CA') THEN
        CASE
            WHEN upper(ord_line.rtn_reason) = 'COLOR MISMATCH' THEN 'PC'
            WHEN upper(ord_line.rtn_reason) = 'BOSS WRONG ITEM' THEN 'BW'
            WHEN upper(ord_line.rtn_reason) = 'DROP SHIP WRONG ITEM' THEN 'DI'
            WHEN upper(ord_line.rtn_reason) = 'EMBROIDERY QUALITY' THEN 'EQ'
            WHEN upper(ord_line.rtn_reason) = 'EB4KIDS FIT GUARNTEE' THEN 'FG'
            WHEN upper(ord_line.rtn_reason) = 'DO NOT USE' THEN 'LS'
            WHEN upper(ord_line.rtn_reason) = 'WRONG ART GRAPHIC' THEN 'PA'
            WHEN upper(ord_line.rtn_reason) = 'PERSONALIZED-BUYER' THEN 'PB'
            WHEN upper(ord_line.rtn_reason) = 'WRONG COLOR' THEN 'PC'
            WHEN upper(ord_line.rtn_reason) = 'PERSONALZD WRONG ITM' THEN 'PI'
            WHEN upper(ord_line.rtn_reason) = 'PERSONALZD UNWANTED' THEN 'PN'
            WHEN upper(ord_line.rtn_reason) = 'PRINTING QUALITY' THEN 'PQ'
            WHEN upper(ord_line.rtn_reason) = 'MIS-SPELLING' THEN 'PS'
            WHEN upper(ord_line.rtn_reason) = 'PERSONALIZED-AGENT' THEN 'PU'
            WHEN upper(ord_line.rtn_reason) = 'DIDN''T HOLD UP' THEN 'PW'
            WHEN upper(ord_line.rtn_reason) = 'STORE DID NOT REFUND' THEN 'SN'
            WHEN upper(ord_line.rtn_reason) = 'STORE DID REFUND' THEN 'SY'
            WHEN upper(ord_line.rtn_reason) = 'USED CLEAT' THEN 'UC'
            WHEN upper(ord_line.rtn_reason) = 'USED SHOE NOT DEFECT' THEN 'US'
            WHEN upper(ord_line.rtn_reason) = 'WRONG ITEM ORDERED' THEN 'WO'
            WHEN upper(ord_line.rtn_reason) = 'DAMAGED IN TRANSIT (throw in trash)' THEN 'WT'
            WHEN upper(ord_line.rtn_reason) = 'XSTORE RETURN' THEN 'XX'
            WHEN upper(ord_line.rtn_reason) IN ('DAMAGED','DEFECTIVE ITEM','DEFECTIVE ITEM NO RETURN FEE','FLX DEFECTIVE ITEM','FLX DEFECTIVE ITEM NO RETURN FEE','["DEFECTIVE ITEM"]') THEN 'DQ'
            WHEN upper(ord_line.rtn_reason) IN ('WRONG ITEM','I ORDERED THE WRONG ITEM','FLX I ORDERED THE WRONG ITEM','WRONG ITEM SHIPPED','FLX WRONG ITEM SHIPPED','["WRONG ITEM SHIPPED"]') THEN 'WI'
            WHEN upper(ord_line.rtn_reason) IN ('ITEM NOT AS DESCRIBE','ITEM NOT AS DESCRIBED/ PICTURED','FLX ITEM NOT AS DESCRIBED/ PICTURED','["ITEM NOT AS DESCRIBED/ PICTURED"]') THEN 'WD'
            WHEN upper(ord_line.rtn_reason) IN ('TOO BIG / LONG','TOO BIG LONG','TOO BIG/ LONG','TOO BIG/ LONG NO RETURN FEE','TOO LONG','ITEM DOES NOT FIT','FLX TOO BIG/ LONG','FLX TOO BIG/ LONG NO RETURN FEE') THEN 'TB'
            WHEN upper(ord_line.rtn_reason) IN ('TOO SMALL / SHORT','TOO SMALL/ SHORT','TOO SMALL/SHORT','TOO SMALL/SHORT NO RETURN FEE','FLX TOO SMALL/ SHORT','FLX TOO SMALL/SHORT','FLX TOO SMALL/ SHORT NO RETURN FEE') THEN 'TS'
            WHEN upper(ord_line.rtn_reason) IN ('TOO NARROW','TOO NARROW NO RETURN FEE','FLX TOO NARROW','["TOO NARROW"]') THEN 'TN'
            WHEN upper(ord_line.rtn_reason) IN ('TOO WIDE','TOO WIDE NO RETURN FEE','FLX TOO WIDE','FLX TOO WIDE NO RETURN FEE','["TOO WIDE"]') THEN 'TW'
            WHEN upper(ord_line.rtn_reason) IN ('UNWANTED ITEM','UNWANTED / CHANGED MY MIND','UNWANTED/ CHANGED MY MIND','UNWANTED/ CHANGED MY MIND NO RETURN FEE','FLX UNWANTED/ CHANGED MY MIND') THEN 'U'
            WHEN upper(ord_line.rtn_reason) = 'LOST PACKAGE' THEN 'LP'
            WHEN upper(ord_line.rtn_reason) = 'NOT DELIVERABLE' THEN 'ND'
            WHEN upper(ord_line.rtn_reason) IN ('RETURN','RETURNREASON','INSTORE CS RETURNS','STORERETURN RETURNS','DC RETURNS') THEN 'IS'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '501-%' THEN '501'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '502-%' THEN '502'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '503-%' THEN '503'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '504-%' THEN '504'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '505-%' THEN '505'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '506-%' THEN '506'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '507-%' THEN '507'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '508-%' THEN '508'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '509-%' THEN '509'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '510-%' THEN '510'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '511-%' THEN '511'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '512-%' THEN '512'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '513-%' THEN '513'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '514-%' THEN '514'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '515-%' THEN '515'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '521-%' THEN '521'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '522-%' THEN '522'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '523-%' THEN '523'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '525-%' THEN '525'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '526-%' THEN '526'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '527-%' THEN '527'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '529-%' THEN '529'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '534-%' THEN '534'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '539-%' THEN '539'
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '999-%' THEN '999'
        END
  END AS lines_reasonCode,
  CASE
    WHEN trim(ORD_LINE.org_id) IN ('CH-US','FL-US','KFL-US','FL-CA','CH-CA') THEN
        CASE
            WHEN upper(ord_line.rtn_reason) = 'BOSS WRONG ITEM' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 390   -- company 20
					WHEN 'FL-US' THEN 294   -- company 21
					WHEN 'KFL-US' THEN 262  -- company 22
					WHEN 'FL-CA' THEN 326   -- company 45
					WHEN 'CH-CA' THEN 548   -- company 77
				END
			WHEN upper(ord_line.rtn_reason) = 'DROP SHIP WRONG ITEM' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 391
					WHEN 'FL-US' THEN 295
					WHEN 'KFL-US' THEN 263
					WHEN 'FL-CA' THEN 327
					WHEN 'CH-CA' THEN 549
				END
			WHEN upper(ord_line.rtn_reason) = 'EMBROIDERY QUALITY' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 393
					WHEN 'FL-US' THEN 297
					WHEN 'KFL-US' THEN 265
					WHEN 'FL-CA' THEN 329
					WHEN 'CH-CA' THEN 551
				END
			WHEN upper(ord_line.rtn_reason) = 'WRONG ITEM' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 419
					WHEN 'FL-US' THEN 323
					WHEN 'KFL-US' THEN 291
					WHEN 'FL-CA' THEN 355
					WHEN 'CH-CA' THEN 577
				END
			WHEN upper(ord_line.rtn_reason) = 'ITEM NOT AS DESCRIBE' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 418
					WHEN 'FL-US' THEN 322
					WHEN 'KFL-US' THEN 290
					WHEN 'FL-CA' THEN 354
					WHEN 'CH-CA' THEN 576
				END
			WHEN upper(ord_line.rtn_reason) = 'UNWANTED ITEM' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 415
					WHEN 'FL-US' THEN 319
					WHEN 'KFL-US' THEN 287
					WHEN 'FL-CA' THEN 351
					WHEN 'CH-CA' THEN 573
				END
			WHEN upper(ord_line.rtn_reason) = 'INSTORE CS RETURNS' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 395
					WHEN 'FL-US' THEN 299
					WHEN 'KFL-US' THEN 267
					WHEN 'FL-CA' THEN 331
					WHEN 'CH-CA' THEN 553
				END
			WHEN upper(ord_line.rtn_reason) = 'XSTORE RETURN' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'FL-US' THEN 762
				END
			WHEN upper(ord_line.rtn_reason) = 'WRONG ART GRAPHIC' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 400
					WHEN 'FL-US' THEN 304
					WHEN 'KFL-US' THEN 272
					WHEN 'FL-CA' THEN 336
					WHEN 'CH-CA' THEN 558
				END
			WHEN upper(ord_line.rtn_reason) = 'PERSONALIZED-BUYER' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 401
					WHEN 'FL-US' THEN 305
					WHEN 'KFL-US' THEN 273
					WHEN 'FL-CA' THEN 337
					WHEN 'CH-CA' THEN 559
				END
			WHEN upper(ord_line.rtn_reason) = 'PERSONALZD WRONG ITM' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 403
					WHEN 'FL-US' THEN 307
					WHEN 'KFL-US' THEN 275
					WHEN 'FL-CA' THEN 339
					WHEN 'CH-CA' THEN 561
				END
			WHEN upper(ord_line.rtn_reason) = 'PERSONALZD UNWANTED' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 404
					WHEN 'FL-US' THEN 308
					WHEN 'KFL-US' THEN 276
					WHEN 'FL-CA' THEN 340
					WHEN 'CH-CA' THEN 562
				END
			WHEN upper(ord_line.rtn_reason) = 'PRINTING QUALITY' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 405
					WHEN 'FL-US' THEN 309
					WHEN 'KFL-US' THEN 277
					WHEN 'FL-CA' THEN 341
					WHEN 'CH-CA' THEN 563
				END
			WHEN upper(ord_line.rtn_reason) = 'MIS-SPELLING' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 406
					WHEN 'FL-US' THEN 310
					WHEN 'KFL-US' THEN 278
					WHEN 'FL-CA' THEN 342
					WHEN 'CH-CA' THEN 564
				END
			WHEN upper(ord_line.rtn_reason) = 'PERSONALIZED-AGENT' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 407
					WHEN 'FL-US' THEN 311
					WHEN 'KFL-US' THEN 279
					WHEN 'FL-CA' THEN 343
					WHEN 'CH-CA' THEN 565
				END
			WHEN upper(ord_line.rtn_reason) = 'DIDN''T HOLD UP' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 408
					WHEN 'FL-US' THEN 312
					WHEN 'KFL-US' THEN 280
					WHEN 'FL-CA' THEN 344
					WHEN 'CH-CA' THEN 566
				END
			WHEN upper(ord_line.rtn_reason) = 'STORE DID NOT REFUND' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 409
					WHEN 'FL-US' THEN 313
					WHEN 'KFL-US' THEN 281
					WHEN 'FL-CA' THEN 345
					WHEN 'CH-CA' THEN 567
				END
			WHEN upper(ord_line.rtn_reason) = 'STORE DID REFUND' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 410
					WHEN 'FL-US' THEN 314
					WHEN 'KFL-US' THEN 282
					WHEN 'FL-CA' THEN 346
					WHEN 'CH-CA' THEN 568
				END
			WHEN upper(ord_line.rtn_reason) = 'USED CLEAT' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 416
					WHEN 'FL-US' THEN 320
					WHEN 'KFL-US' THEN 288
					WHEN 'FL-CA' THEN 352
					WHEN 'CH-CA' THEN 574
				END
			WHEN upper(ord_line.rtn_reason) = 'USED SHOE NOT DEFECT' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 417
					WHEN 'FL-US' THEN 321
					WHEN 'KFL-US' THEN 289
					WHEN 'FL-CA' THEN 353
					WHEN 'CH-CA' THEN 575
				END
			WHEN upper(ord_line.rtn_reason) = 'WRONG ITEM ORDERED' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 420
					WHEN 'FL-US' THEN 324
					WHEN 'KFL-US' THEN 292
					WHEN 'FL-CA' THEN 356
					WHEN 'CH-CA' THEN 578
				END
			WHEN upper(ord_line.rtn_reason) = 'DAMAGED IN TRANSIT (throw in trash)' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 421
					WHEN 'FL-US' THEN 325
					WHEN 'KFL-US' THEN 293
					WHEN 'FL-CA' THEN 357
					WHEN 'CH-CA' THEN 579
				END
			WHEN upper(ord_line.rtn_reason) = 'DO NOT USE' THEN
				CASE trim(ORD_LINE.org_id)
					WHEN 'CH-US' THEN 397
					WHEN 'FL-US' THEN 301
					WHEN 'KFL-US' THEN 269
					WHEN 'FL-CA' THEN 333
					WHEN 'CH-CA' THEN 555
				END
            WHEN upper(ord_line.rtn_reason) = 'COLOR MISMATCH' THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 402
                    WHEN 'FL-US' THEN 306
                    WHEN 'KFL-US' THEN 274
                    WHEN 'FL-CA' THEN 338
                    WHEN 'CH-CA' THEN 560
                END
            WHEN upper(ord_line.rtn_reason) IN ('DAMAGED','DEFECTIVE ITEM','DEFECTIVE ITEM NO RETURN FEE','FLX DEFECTIVE ITEM','FLX DEFECTIVE ITEM NO RETURN FEE','["DEFECTIVE ITEM"]') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 392
                    WHEN 'FL-US' THEN 296
                    WHEN 'KFL-US' THEN 264
                    WHEN 'FL-CA' THEN 328
                    WHEN 'CH-CA' THEN 550
                END
            WHEN upper(ord_line.rtn_reason) IN ('I ORDERED THE WRONG ITEM','FLX I ORDERED THE WRONG ITEM','WRONG ITEM SHIPPED','FLX WRONG ITEM SHIPPED','["WRONG ITEM SHIPPED"]') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 419
                    WHEN 'FL-US' THEN 323
                    WHEN 'KFL-US' THEN 291
                    WHEN 'FL-CA' THEN 355
                    WHEN 'CH-CA' THEN 577
                END
            WHEN upper(ord_line.rtn_reason) IN ('ITEM NOT AS DESCRIBED/ PICTURED','FLX ITEM NOT AS DESCRIBED/ PICTURED','["ITEM NOT AS DESCRIBED/ PICTURED"]') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 418
                    WHEN 'FL-US' THEN 322
                    WHEN 'KFL-US' THEN 290
                    WHEN 'FL-CA' THEN 354
                    WHEN 'CH-CA' THEN 576
                END
            WHEN upper(ord_line.rtn_reason) IN ('TOO BIG / LONG','TOO BIG LONG','TOO BIG/ LONG','TOO BIG/ LONG NO RETURN FEE','TOO LONG','ITEM DOES NOT FIT','FLX TOO BIG/ LONG','FLX TOO BIG/ LONG NO RETURN FEE') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 411
                    WHEN 'FL-US' THEN 315
                    WHEN 'KFL-US' THEN 283
                    WHEN 'FL-CA' THEN 347
                    WHEN 'CH-CA' THEN 569
                END
            WHEN upper(ord_line.rtn_reason) IN ('TOO SMALL / SHORT','TOO SMALL/ SHORT','TOO SMALL/SHORT','TOO SMALL/SHORT NO RETURN FEE','FLX TOO SMALL/ SHORT','FLX TOO SMALL/SHORT','FLX TOO SMALL/ SHORT NO RETURN FEE') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 413
                    WHEN 'FL-US' THEN 317
                    WHEN 'KFL-US' THEN 285
                    WHEN 'FL-CA' THEN 349
                    WHEN 'CH-CA' THEN 571
                END
            WHEN upper(ord_line.rtn_reason) IN ('TOO NARROW','TOO NARROW NO RETURN FEE','FLX TOO NARROW','["TOO NARROW"]') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 412
                    WHEN 'FL-US' THEN 316
                    WHEN 'KFL-US' THEN 284
                    WHEN 'FL-CA' THEN 348
                    WHEN 'CH-CA' THEN 570
                END
            WHEN upper(ord_line.rtn_reason) IN ('TOO WIDE','TOO WIDE NO RETURN FEE','FLX TOO WIDE','FLX TOO WIDE NO RETURN FEE','["TOO WIDE"]') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 414
                    WHEN 'FL-US' THEN 318
                    WHEN 'KFL-US' THEN 286
                    WHEN 'FL-CA' THEN 350
                    WHEN 'CH-CA' THEN 572
                END
            WHEN upper(ord_line.rtn_reason) IN ('UNWANTED / CHANGED MY MIND','UNWANTED/ CHANGED MY MIND','UNWANTED/ CHANGED MY MIND NO RETURN FEE','FLX UNWANTED/ CHANGED MY MIND') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 415
                    WHEN 'FL-US' THEN 319
                    WHEN 'KFL-US' THEN 287
                    WHEN 'FL-CA' THEN 351
                    WHEN 'CH-CA' THEN 573
                END
            WHEN upper(ord_line.rtn_reason) = 'LOST PACKAGE' THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 398
                    WHEN 'FL-US' THEN 302
                    WHEN 'KFL-US' THEN 270
                    WHEN 'FL-CA' THEN 334
                    WHEN 'CH-CA' THEN 556
                END
            WHEN upper(ord_line.rtn_reason) = 'NOT DELIVERABLE' THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 399
                    WHEN 'FL-US' THEN 303
                    WHEN 'KFL-US' THEN 271
                    WHEN 'FL-CA' THEN 335
                    WHEN 'CH-CA' THEN 557
                END
            WHEN upper(ord_line.rtn_reason) IN ('RETURN','RETURNREASON','STORERETURN RETURNS','DC RETURNS') THEN
                CASE trim(ORD_LINE.org_id)
                    WHEN 'CH-US' THEN 395
                    WHEN 'FL-US' THEN 299
                    WHEN 'KFL-US' THEN 267
                    WHEN 'FL-CA' THEN 331
                    WHEN 'CH-CA' THEN 553
                END
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '501-%' THEN 763
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '502-%' THEN 764
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '503-%' THEN 765
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '504-%' THEN 766
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '505-%' THEN 767
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '506-%' THEN 768
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '507-%' THEN 769
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '508-%' THEN 770
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '509-%' THEN 771
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '510-%' THEN 772
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '511-%' THEN 773
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '512-%' THEN 774
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '513-%' THEN 775
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '514-%' THEN 776
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '515-%' THEN 777
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '521-%' THEN 778
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '522-%' THEN 779
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '523-%' THEN 780
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '525-%' THEN 781
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '526-%' THEN 782
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '527-%' THEN 783
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '529-%' THEN 784
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '534-%' THEN 785
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '539-%' THEN 786
            WHEN trim(ORD_LINE.org_id) = 'FL-US' AND upper(ord_line.rtn_reason) LIKE '999-%' THEN 787
        END
  END AS lines_reasonCodeId,
  cast(ORD_HDR.ccy_cd as string) AS currencyIso,
  CAST(NULL AS STRING) AS req_lines_priceOverrideReason,
  cast(ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) as string) AS req_lines_discountAmount,
  cast((ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)))
    - ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) as string) AS req_lines_discountedTotalAmount,
  cast(ABS(coalesce(ord_line.fv_orig_unit_price, ord_line.fv_unit_price,0)) as string) AS req_lines_originalRetailPrice,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) as string) AS req_lines_shippingAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) as string) AS req_lines_shippingTaxAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) as string) AS req_lines_subTotalAmount,
  CAST(ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) as string) AS req_lines_taxAmount,
  cast(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0))
    + ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0)) as string) as req_lines_totalAmount,
  CAST(NULL AS STRING) AS req_lines_giftBoxAmount,
  CAST(NULL AS STRING) AS req_lines_giftBoxTaxAmount,
  cast(ABS(coalesce(ord_line.fv_unit_price,0)) as string) AS req_lines_unitPrice,
  CAST(CASE
        WHEN trim(ORD_LINE.org_id) = 'FL-US' AND REGEXP_LIKE(UPPER(TRIM(REPLACE(REPLACE(rtn_reason,'["',''),'"]',''))),'^[0-9]{3}-') THEN 'false'
        WHEN UPPER(TRIM(rtn_reason)) IS NOT NULL THEN 'true'
        ELSE 'true'
    END AS STRING) AS restockable,
  cast(ORD_HDR.inv_id as string) AS taxInvoiceNum,
  from_json(coalesce(ord_hdr.credits_info,'[]'), 'ARRAY<STRUCT<adjustmentType: STRING, adjustmentTypeDesc: STRING, adjustmentTypeId: STRING, amount: STRING, override: STRING>>') as credits,
  from_json(coalesce(ord_line.rtn_fee_info,'[]'), 'ARRAY<STRUCT<adjustmentType: STRING, adjustmentTypeDesc: STRING, adjustmentTypeId: STRING, amount: STRING, override: STRING>>') AS adjustments,
  pymt_line.PaymentsInfo as PaymentsInfo,
  ORD_LINE.UPDATED_TS AS postedAt,
  cast(ORD_LINE.UPDATED_BY as string) AS postedBy,
  ORD_LINE.UPDATED_TS AS load_time_kafka,
  ORD_LINE.etl_updt_TS AS load_time_adls,
  to_date(ord_hdr.created_ts) AS order_date,
  ORD_LINE.UPDATED_TS AS updated_datetime,
  CAST(CASE
        WHEN upper(loc.loc_type_id) = 'DC' THEN NULL
        ELSE dim_loc.loc_num
    END AS STRING) AS returningStore,
  cast(orig.xstore_ord_id as string) AS xstoreTransactionNumber,
  CAST(case when ORD_LINE.is_loyalty_disc=1 then 'true' end AS STRING) AS loyaltyDiscount,
	coalesce(ord_line.ord_coupons,'[]') as req_tot_discounts,
	coalesce(ord_line.ord_coupons,'[]') as return_act_discounts,
  cast(ORD_LINE.ord_ln_id as string) as lines_lineNumber,
  'MAO' as ref_source
from  fct_mao_ord_line ord_line
  join  base_ord_hdr  ORD_HDR on (ORD_LINE.ORG_ID = ORD_HDR.ORG_ID and ORD_LINE.ORD_ID = ORD_HDR.ORD_ID)
  left join payment_grouped pymt_line on (ORD_LINE.ORG_ID = pymt_line.ORG_ID and ord_line.ord_id = pymt_line.ord_id)
  left join product_master pm ON ((ord_line.is_gift_card!=1 and trim(ord_line.item_id) = trim(pm.global_size_id) AND (CASE WHEN ord_hdr.ORG_ID IN ('FL-CA','CH-CA') THEN '98' ELSE '81' END)=pm.banner_id))
  left join product_master_div pm_div ON (ord_line.is_gift_card!=1 and trim(ord_line.item_id) = trim(pm_div.global_size_id) AND ord_line.ORG_ID = pm_div.org_desc)
  left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v loc on (coalesce(ord_line.ship_to_loc_id,ord_line.physical_org_id) = loc.loc_id)
  left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_item_v itm on (trim(ord_line.ITEM_ID) = trim(itm.ITEM_ID))
  left join dim_location dim_loc on (lpad(dim_loc.loc_snum,5,0) = lpad(coalesce(ord_line.ship_to_loc_id,ord_line.physical_org_id),5,0))
  left join fct_exchange_orders ord_exch on (ord_line.ORG_ID = ord_exch.ORG_ID and  ord_line.ORD_ID = ord_exch.PRNT_ORD_ID and ord_line.ord_ln_id=ord_exch.prnt_ord_ln_id)
  left join fct_original_orders orig on (ord_line.org_id=orig.org_id and ord_line.prnt_ord_id=orig.ord_id and  ord_line.prnt_ord_ln_id=orig.ord_ln_id)
);
