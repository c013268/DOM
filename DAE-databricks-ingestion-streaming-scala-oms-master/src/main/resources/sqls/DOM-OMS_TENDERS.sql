CREATE 	OR REPLACE TEMP VIEW tenders AS
(
WITH
payment AS (
  select * from (SELECT *, ROW_NUMBER() OVER (
    PARTITION BY org_id, ord_id, pymt_method_id, pymt_txn_id
    ORDER BY NVL(PYMT_TXN_DETAIL_ID, '') DESC
  ) AS pymt_rnk
  FROM ${dom_gold_db}.${dom_gold_schema}.FCT_MAO_ORD_PYMT_LINE_V where etl_updt_ts > '${lookback_date}')
  where pymt_rnk = 1
),
consignment AS (
  select org_id,ord_id,rel_id from (
    select fd.*,
      row_number() over (partition by fd.org_id,fd.ord_id order by updated_ts desc) ful_det_rnk
    from ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_fulfillment_dtl_v fd
    )
  where ful_det_rnk = 1
)
SELECT
  pymt_line.CCY_CODE                                             AS currencyIso,
  case
    when lower(hdr.ord_type_id) = 'return' and lower(hdr.ord_id) like '%-e_' then upper('exchange')
    when lower(hdr.ord_type_id) = 'return' and lower(hdr.ord_id) like '%-r_' then upper('return')
    when lower(hdr.ord_type_id) <> 'return' and consignment.rel_id is null then upper('order')
    when lower(hdr.ord_type_id) <> 'return' and consignment.rel_id is not null then upper('consignment')
  end                                                            AS messageType,
  pymt_line.ord_id                                               AS orderId,
  CAST(hdr.ord_id AS STRING)                                     AS omsOrderId,
  CAST(CASE TRIM(pymt_line.org_id)
    WHEN 'FL-US'  THEN '21'
    WHEN 'FL-CA'  THEN '45'
    WHEN 'KFL-US' THEN '22'
    WHEN 'CH-CA'  THEN '77'
    WHEN 'CH-US'  THEN '20'
  END AS STRING)                                                 AS companyNumber,
  CASE hdr.max_fulflmnt_status_id
    WHEN '1000'  THEN 'SUBMITTED'
    WHEN '1600'  THEN 'FULFILMENT_PROCESSING'
    WHEN '2000'  THEN 'FULFILMENT_PROCESSING'
    WHEN '3000'  THEN 'FULFILMENT_PROCESSING'
    WHEN '3500'  THEN 'FULFILMENT_PROCESSING'
    WHEN '3600'  THEN 'FULFILMENT_PROCESSING'
    WHEN '3700'  THEN 'FULFILMENT_PROCESSING'
    WHEN '7000'  THEN 'FULFILMENT_COMPLETE'
    WHEN '7500'  THEN 'FULFILMENT_COMPLETE'
    WHEN '9000'  THEN 'CANCELLED'
    WHEN '13000' THEN 'WAIT_FRAUD_SYSTEM_CHECK'
    ELSE upper(hdr.max_fulflmnt_status_desc)
  END                                                            AS orderStatus,
  CAST(ABS(COALESCE(hdr.ord_total_discs, 0)) AS STRING)          AS orderHeader_discountedTotalAmount,
  DATE(pymt_line.etl_updt_ts)                                    AS load_date,
  CASE pymt_line.pymt_type
    WHEN 'Affirm'               THEN 'AFFIRM'
    WHEN 'AliPay'               THEN 'ALIPAY'
    WHEN 'ApplePay'             THEN 'APPLEPAY'
    WHEN 'Cash'                 THEN 'CASH'
    WHEN 'Credit Card'          THEN 'CREDITCARD'
    WHEN 'Gift Card'            THEN 'GIFTCARD'
    WHEN 'GooglePay'            THEN 'PAYWITHGOOGLE'
    WHEN 'In Store Credit Card' THEN 'INTERNAL'
    WHEN 'Store Refund'         THEN 'INTERNAL'
    WHEN 'Klarna Account'       THEN 'KLARNA_ACCOUNT'
    WHEN 'PayPal'               THEN 'PAYPAL'
    WHEN 'Venmo'                THEN 'VENMO'
    WHEN 'WeChat'               THEN 'WECHAT'
    WHEN 'Other'                THEN 'OTHER'
    ELSE UPPER(pymt_line.pymt_type)
  END                                                            AS authorizations_paymentType,
  CAST(pymt_line.CRNT_AUTH_AMT AS DOUBLE)                        AS authorizations_authAmount,
  pymt_line.auth_cd                                              AS authorizations_authCode,
  case when pymt_line.PYMT_GATEWAY_ID ='FootLockerPaymentGateway' then 'Internal' else pymt_line.PYMT_GATEWAY_ID end AS authorizations_gateway,
  pymt_line.pymt_provider                                        AS authorizations_paymentVendor,
  case when lower(pymt_line.AUTH_STATUS)='closed' then 'COMPLETED' else 'PENDING' end AS authorizations_status,
  cast(DATE_FORMAT(
              TO_TIMESTAMP(pymt_line.AUTH_TXN_DT, 'yyyy-MM-dd HH:mm:ss.SSS'),
              "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
        )as string)                                            AS authorizations_transactionDate,
  pymt_line.pymt_txn_id                                          AS authorizations_transactionId,
  consignment.rel_id                                             AS paymentTransactions_consignmentId,
  cast(case when hdr.pymt_status_desc in ('Refunded', 'Awaiting Refund') then hdr.ord_id end as string) AS paymentTransactions_refundId,
  pymt_line.pymt_txn_status_desc                                 AS attributes_authResponse,
  pymt_line.ATTRIB_AVS_CODE                                      AS attributes_avsCode,
  CAST(pymt_line.card_alias AS STRING)                                           AS attributes_cardAlias,
  CAST(pymt_line.pymt_grp_id AS STRING)                          AS attributes_cardBin,
  pymt_line.ATTRIB_CARD_LAST4                                    AS attributes_cardLast4,
  CAST(pymt_line.CARD_TOKEN AS STRING)                           AS attributes_cardToken,
  CAST(CASE
    WHEN upper(pymt_line.PYMT_CARD_TYPE) IN ('VISA')                                    THEN 1
    WHEN upper(pymt_line.PYMT_CARD_TYPE) IN ('MASTERCARD', 'MASTER')                    THEN 2
    WHEN upper(pymt_line.PYMT_CARD_TYPE) IN ('AMEX')                                    THEN 3
    WHEN upper(pymt_line.PYMT_CARD_TYPE) IN ('DISCOVER')                                THEN 4
    WHEN upper(pymt_line.PYMT_CARD_TYPE) IN ('JCB')                                     THEN 5
    WHEN upper(pymt_line.PYMT_CARD_TYPE) IN ('DINERS', 'DINERS_CLUB_INTERNATIONAL')     THEN 6
    WHEN upper(pymt_line.PYMT_CARD_TYPE) IN ('VENMO', 'OTHER', 'WECHAT', 'KLARNA_ACCOUNT', 'PAYPAL','AFFIRM', 'PAYBRIGHT', 'KLARNA', 'ALIPAY'
    )                                                                                   THEN 7
    WHEN upper(pymt_line.PYMT_CARD_TYPE) IN ('UNIONPAY')                                THEN 8
    ELSE 9 END AS STRING)                                        AS attributes_cardType,
  upper(pymt_line.PYMT_CARD_TYPE)                                AS attributes_cardTypeDisplay,
  CAST(null AS STRING)                                           AS attributes_confirmationCode,
  pymt_line.ATTRIB_CVV_RESPONSE                                  AS attributes_cvvResponse,
  pymt_line.ATTRIB_CARD_EXPIRY_DT                                AS attributes_expirationDate,
  CAST(CASE
    WHEN pymt_line.PYMT_TYPE = 'Credit Card' THEN 'CREDIT'
    WHEN pymt_line.PYMT_TYPE = 'Debit Card'  THEN 'DEBIT'
    WHEN pymt_line.PYMT_TYPE = 'Gift Card'   THEN 'PREPAID'
    ELSE UPPER(pymt_line.PYMT_TYPE)
  END AS STRING)                                                 AS attributes_fundingSource,
  CAST(pymt_line.SELLER_PROTECTION_STATUS AS STRING)             AS attributes_sellerProtectionStatus,
  CASE WHEN pymt_line.PYMT_TXN_TYPE='Refund' THEN CAST(-pymt_line.pymt_txn_req_amt AS DOUBLE)
  ELSE CAST(pymt_line.pymt_txn_req_amt AS DOUBLE) END            AS paymentTransactions_transaction_amount,
  pymt_line.auth_cd                                              AS paymentTransactions_transaction_authCode,
  pymt_line.txn_card_last4                                       AS paymentTransactions_transaction_cardLast4,
  upper(pymt_line.pymt_card_type)                                AS paymentTransactions_transaction_creditCardType,
  CAST(coalesce(pymt_line.PYMT_TXN_DT,pymt_line.pymt_txn_req_dt) AS STRING) AS paymentTransactions_transaction_date,
  CAST(pymt_line.pymt_txn_id AS STRING)                          AS paymentTransactions_transaction_id,
  pymt_line.PYMT_TXN_REQ_ID                                      AS paymentTransactions_transaction_paymentTransactionRequestId,
  CASE pymt_line.PYMT_TXN_TYPE
    WHEN 'Authorization'          THEN 'CAPTURE'
    WHEN 'Authorization Reversal' THEN 'CANCEL'
    WHEN 'Refund'                 THEN 'REFUND'
    WHEN 'Return Credit'          THEN 'REFUND'
    WHEN 'Settlement'             THEN 'CAPTURE'
    WHEN 'Void'                   THEN 'CANCEL'
    ELSE UPPER(pymt_line.PYMT_TXN_TYPE)
  END                                                            AS paymentTransactions_transaction_paymentTransactionType,
  CASE pymt_line.pymt_type
    WHEN 'Affirm'               THEN 'AFFIRM'
    WHEN 'AliPay'               THEN 'ALIPAY'
    WHEN 'ApplePay'             THEN 'APPLEPAY'
    WHEN 'Cash'                 THEN 'CASH'
    WHEN 'Credit Card'          THEN 'CREDITCARD'
    WHEN 'Gift Card'            THEN 'GIFTCARD'
    WHEN 'GooglePay'            THEN 'PAYWITHGOOGLE'
    WHEN 'In Store Credit Card' THEN 'INTERNAL'
    WHEN 'Store Refund'         THEN 'INTERNAL'
    WHEN 'Klarna Account'       THEN 'KLARNA_ACCOUNT'
    WHEN 'PayPal'               THEN 'PAYPAL'
    WHEN 'Venmo'                THEN 'VENMO'
    WHEN 'WeChat'               THEN 'WECHAT'
    WHEN 'Other'                THEN 'OTHER'
    ELSE UPPER(pymt_line.pymt_type)
  END                                                            AS paymentTransactions_transaction_paymentType,
  upper(pymt_line.pymt_txn_status_desc)                          AS paymentTransactions_transaction_status,
  CAST(NULL AS BOOLEAN)                                          AS authorizations_preSettled,
  pymt_line.auth_original_order_id                               AS authorizations_originalOrderNumber,
  pymt_line.attrib_str_merch_id                                  AS attributes_storeMerchantId,
  pymt_line.attrib_str_terminal_id                               AS attributes_storeTerminalId,
  pymt_line.attrib_str_ord_req_id                                AS attributes_storeOrderRequestId,
  pymt_line.attrib_str_inv_id                                    AS attributes_storeInvoiceId,
  CASE WHEN LOWER(pymt_line.pymt_type) = 'gift card'
    THEN pymt_line.attrib_card_last4
  END                                                            AS attributes_giftCardNumber,
  pymt_line.ADDR_EMAIL                                           AS attributes_email,
  case when upper(hdr.cust_type_id)='GUEST' then 'UNVERIFIED'
       when upper(hdr.cust_type_id)='REGISTERED' then 'VERIFIED' end  AS attributes_payerStatus,
  CAST('MAO' AS STRING)                                          AS ref_source
FROM payment pymt_line
  JOIN ${dom_gold_db}.${dom_gold_schema}.FCT_MAO_ORD_HDR_V hdr ON pymt_line.ORD_ID = hdr.ORD_ID AND pymt_line.org_id = hdr.org_id
  LEFT JOIN consignment ON pymt_line.ORD_ID = consignment.ORD_ID AND pymt_line.org_id = consignment.org_id
WHERE
  lower(hdr.doc_type_id) = 'customerorder'
);
