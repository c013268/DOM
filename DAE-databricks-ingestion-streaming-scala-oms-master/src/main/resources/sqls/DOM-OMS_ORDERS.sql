create or replace temp view product_master as
(
  SELECT distinct 
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
  FROM prod.product_npii.product_master pm
  where pm.banner_id in ('81','98')
);
 create or replace temp view product_master_div as
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

CREATE OR REPLACE TEMP VIEW orders AS
(
with fct_mao_ord_pymt_line as (select * from (
    SELECT org_id,ord_id,
    addr_firstname as bill_addr_firstname,
    addr_lastname as bill_addr_lastname,
    addr_addr1 as bill_addr_addr1,
    addr_addr2 as bill_addr_addr2,
    addr_addr3 as bill_addr_addr3,
    addr_city as bill_addr_city,
    addr_postal_cd as bill_addr_postal_cd,
    addr_state as bill_addr_state,
    addr_country as bill_addr_country,
    addr_email as bill_addr_email,
    addr_phone as bill_addr_phone,
	  pymt_gateway_id,
    row_number() over (
					partition by org_id,ord_id
					order by src_load_ts desc
				) as rnk
FROM ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_pymt_line_v
where ord_id is not null)
where rnk=1),
fct_mao_fulfillment_line as (
	select org_id,ord_id,max(fulflmnt_id) fulflmnt_id from ${dom_gold_db}.${dom_gold_schema}.fct_mao_fulfillment_line_v group by all
),
fct_exchange_orders as (
    select distinct ol.org_id,ol.prnt_ord_id,ol.is_even_exchg
    from ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_line_v ol
        join ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v  oh on (ol.org_id = oh.org_id and ol.ord_id = oh.ord_id)
    where oh.doc_type_id = 'CustomerOrder'
    and ol.is_even_exchg = 1
    and ol.prnt_ord_id is not null
    and ol.max_fulflmnt_status_id is not null
),
dim_location as (
    select * from
        (select
            loc_snum,loc_num,row_number() over (partition by lpad(loc_snum, 5, 0) order by loc_sk desc,loc_seq_num desc) loc_rnk
        from sf_gold_prod_db.location_gold_prod.dim_location_v where UPPER(banner_geo)='NA')
        where loc_rnk=1
),
fct_mao_ord_line_with_fv as (
    select * from (
    select
        ord_line.*,
        -- FIRST_VALUE window functions: always capture financial amounts from the earliest (original) status
		first_value(ord_line.orig_unit_price) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_orig_unit_price,
        first_value(ord_line.unit_price) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_unit_price,
        first_value(ord_line.cnlled_orig_ord_shipping_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_shipping_amt,
        first_value(ord_line.orig_ord_shipping_amt)        over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_shipping_amt,
        first_value(ord_line.cnlled_orig_ord_sales_tax_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_sales_tax_amt,
        first_value(ord_line.orig_ord_sales_tax_amt)       over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_sales_tax_amt,
        first_value(ord_line.cnlled_orig_ord_shipping_tax_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_shipping_tax_amt,
        first_value(ord_line.orig_ord_shipping_tax_amt)    over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_shipping_tax_amt,
        first_value(ord_line.gift_card_value)              over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_gift_card_value,
        first_value(ord_line.cnlled_total_disc)            over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_total_disc,
        first_value(ord_line.total_disc)                   over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_total_disc,
        first_value(ord_line.cnlled_ord_ln_total)          over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_ln_total,
        first_value(ord_line.ord_ln_total)                 over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_ln_total,
        first_value(ord_line.cnlled_ord_ln_sub_total)      over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_ln_sub_total,
        first_value(ord_line.ord_ln_sub_total)             over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_ln_sub_total,
        first_value(ord_line.cnlled_ord_coupon_amt)        over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_coupon_amt,
        first_value(ord_line.ord_coupon_amt)               over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_coupon_amt,
        first_value(ord_line.cnlled_total_charges)         over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_total_charges,
        first_value(ord_line.total_charges)                over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_total_charges,
        first_value(ord_line.cnlled_total_taxes)           over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_total_taxes,
        first_value(ord_line.total_taxes)                  over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_total_taxes,
        row_number() over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id desc, ord_line.updated_ts desc) as fv_rn
    from ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_line_hist_v ord_line
    join ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v ord_hdr on ord_line.org_id = ord_hdr.org_id and ord_line.ord_id = ord_hdr.ord_id
    where ord_hdr.doc_type_id = 'CustomerOrder'
        and not (ord_line.max_fulflmnt_status_id is null or ord_line.max_fulflmnt_status_id > 9000)
        and (ord_line.prnt_ord_id is null or ord_line.is_even_exchg = 1)
    ) where fv_rn = 1
),
fct_mao_ord_line_agg as (
    select
        ord_id,
        org_id,
        max_by(cnl_reason_id, updated_ts) as cnl_reason_id,
        max_by(cnl_reason_desc, updated_ts) as cnl_reason_desc,
        max_by(physical_org_id, updated_ts) as physical_org_id,
        max_by(is_gift, updated_ts) as is_gift,
        max_by(is_base_shipping_charged, updated_ts) as is_base_shipping_charged,
        sum(coalesce(pkg_cnt, 0)) as pkg_cnt,
        max_by(cart_shpmnt_method, updated_ts) as cart_shpmnt_method,
        max_by(shpmnt_method, updated_ts) as shpmnt_method,
        max_by(appeasment_reason_cd, updated_ts) as appeasment_reason_cd,
        max_by(relate_ord_num_csa, updated_ts) as relate_ord_num_csa,
        max_by(etl_updt_ts, updated_ts) as etl_updt_ts,
        max_by(loyalty_reward_id, updated_ts) as loyalty_reward_id,
        max_by(is_loyalty_disc, updated_ts) as is_loyalty_disc,
        max_by(RUSH_FLG, updated_ts) as RUSH_FLG,
        max_by(SHIP_FROM_LOC_ID, updated_ts) as SHIP_FROM_LOC_ID,
        max(coalesce(IS_APPEASEMENT, 0)) as IS_APPEASEMENT,
        max(is_gift_card) as is_gift_card,
        max_by(is_refund_gift_card, updated_ts) as is_refund_gift_card,
        max_by(is_even_exchg, updated_ts) as is_even_exchg,
        max_by(ship_to_addr_addr1, updated_ts) as ship_to_addr_addr1,
        max_by(ship_to_addr_addr2, updated_ts) as ship_to_addr_addr2,
        max_by(ship_to_addr_city, updated_ts) as ship_to_addr_city,
        max_by(ship_to_addr_country, updated_ts) as ship_to_addr_country,
        max_by(ship_to_addr_state, updated_ts) as ship_to_addr_state,
        max_by(ship_to_addr_postal_cd, updated_ts) as ship_to_addr_postal_cd,
        max_by(ship_to_addr_email, updated_ts) as ship_to_addr_email,
        max_by(ship_to_addr_first_name, updated_ts) as ship_to_addr_first_name,
        max_by(ship_to_addr_last_name, updated_ts) as ship_to_addr_last_name,
        max_by(ship_to_addr_phone, updated_ts) as ship_to_addr_phone,
        max_by(prnt_ord_id, updated_ts) as prnt_ord_id,
        max_by(updated_by, updated_ts) as updated_by,
        max_by(created_by, updated_ts) as created_by,
        sum(fv_cnlled_ord_coupon_amt) as ol_cnlled_ord_coupon_amt,
        sum(fv_ord_coupon_amt) as ol_ord_coupon_amt,
        sum(fv_cnlled_ord_sales_tax_amt) as ol_cnlled_ord_sales_tax_amt,
        sum(fv_ord_sales_tax_amt) as ol_ord_sales_tax_amt,
        sum(fv_cnlled_ord_shipping_tax_amt) as ol_cnlled_ord_shipping_tax_amt,
        sum(fv_ord_shipping_tax_amt) as ol_ord_shipping_tax_amt,
        sum(fv_gift_card_value) as ol_gift_card_value,
        sum(fv_cnlled_total_disc) as ol_cnlled_total_disc,
        sum(fv_total_disc) as ol_total_disc,
        sum(fv_cnlled_ord_ln_total) as ol_cnlled_ord_ln_total,
        sum(fv_ord_ln_total) as ol_ord_ln_total,
        sum(fv_cnlled_ord_ln_sub_total) as ol_cnlled_ord_ln_sub_total,
        sum(fv_ord_ln_sub_total) as ol_ord_ln_sub_total,
        sum(fv_cnlled_total_charges) as ol_cnlled_total_charges,
        sum(fv_total_charges) as ol_total_charges,
        sum(fv_cnlled_total_taxes) as ol_cnlled_total_taxes,
        sum(fv_total_taxes) as ol_total_taxes,
        sum(fv_cnlled_ord_shipping_amt) as ol_cnlled_ord_shipping_amt,
        sum(fv_ord_shipping_amt) as ol_ord_shipping_amt
    from fct_mao_ord_line_with_fv
    group by ord_id, org_id
)
SELECT
    ord_line.ORD_ID AS order_id,
    cast(case trim(ord_line.org_id)
      when 'FL-US' then '21'
      when 'FL-CA' then '45'
      when 'KFL-US' then '22'
      when 'CH-CA' then '77'
      when 'CH-US' then '20'
    end as string) as company_number,
    case
        when ord_line.max_fulflmnt_status_id = '1000' then 'SUBMITTED'
        when ord_line.max_fulflmnt_status_id = '1500' then 'SUBMITTED'
        when ord_hdr.is_fraud_service_failed = 1 then 'FRAUD_CHECK_FAILED'
        when ord_line.max_fulflmnt_status_id = '1600' then 'FULFILMENT_PROCESSING'
        when ord_line.max_fulflmnt_status_id = '2000' then 'FULFILMENT_PROCESSING'
        when ord_line.max_fulflmnt_status_id = '3000' then 'FULFILMENT_PROCESSING'
        when ord_line.max_fulflmnt_status_id = '3500' then 'FULFILMENT_PROCESSING'
        when ord_line.max_fulflmnt_status_id = '3600' then 'FULFILMENT_PROCESSING'
        when ord_line.max_fulflmnt_status_id = '3700' then 'FULFILMENT_PROCESSING'
        when ord_line.max_fulflmnt_status_id = '7000' then 'FULFILMENT_COMPLETE'
        when ord_line.max_fulflmnt_status_id = '7500' then 'FULFILMENT_COMPLETE'
        when ord_line.max_fulflmnt_status_id = '8000' then 'FULFILMENT_COMPLETE'
        when ord_line.max_fulflmnt_status_id = '8500' then 'FULFILMENT_COMPLETE'
        when ord_line.max_fulflmnt_status_id = '9000' then 'CANCELLED'
        when ord_line.max_fulflmnt_status_id = '13000' then 'WAIT_FRAUD_SYSTEM_CHECK'
    else upper(ord_line.max_fulflmnt_status_desc)
    end as order_status,
    cast(cnl_reason.oms_cancel_code as string) as cancel_code,
    cast(ord_line.cnl_reason_desc as string) cancelReason,
    CASE
        WHEN UPPER(ord_line.DLVRY_METHOD_ID) IN ('PICKUPATSTORE', 'PICKUP_IN_STORE', 'SHIPTORETURNCENTER') THEN 'PICK'
        WHEN UPPER(ord_line.DLVRY_METHOD_ID) IN ('SHIPTOADDRESS', 'SHIPTOSTORE') THEN 'SHIP'
        WHEN UPPER(ord_line.DLVRY_METHOD_ID) = 'EMAIL' THEN 'ELECTRONIC'
        WHEN UPPER(ord_line.DLVRY_METHOD_ID)  = 'STORERETURN' THEN 'STORE_RETURN'
        WHEN UPPER(ord_line.DLVRY_METHOD_ID)  = 'STORESALE' THEN 'XSTORE'
    else UPPER(ord_line.DLVRY_METHOD_ID)
    END AS fullfillment_type,
    from_json(upper(ord_hdr.pymt_type), "array<string>") as payment_type,
    CAST(case when IS_FREE_SHIPPING=0 then true
         when IS_FREE_SHIPPING=1 then false
         else null end AS STRING)  AS free_shipping,
    ord_line.ORD_LN_ID AS order_lineNumber,
    ord_hdr.CCY_CD AS order_currency,
    CAST(NULL AS STRING) order_salecode,
    CAST(ord_line.PRICE_OVERRIDE_REASON AS STRING) order_priceOverrideReason,
   CAST(ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) AS STRING) AS ORDER_DISCOUNTAMOUNT,
   CAST(CASE
        WHEN ABS(coalesce(ord_line.fv_ord_ln_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ord_line.fv_gift_card_value,0))
        ELSE 
			(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) + 
			ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) +
			ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) +
			ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0))) -
			ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0))
   END AS STRING) AS ORDER_DISCOUNTED_TOTALAMOUNT,
   CAST(ABS(coalesce(ord_line.fv_orig_unit_price, ord_line.fv_unit_price,0)) AS STRING) AS ORDER_ORIGINAL_RETAILPRICE,
   CAST(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) AS STRING) AS ORDER_SHIPPINGAMOUNT,
   CAST(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) AS STRING) AS ORDER_SHIPPINGTAXAMOUNT,
   CAST(CASE
        WHEN ABS(coalesce(ord_line.fv_ord_ln_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ord_line.fv_gift_card_value,0))
        ELSE ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
   END AS STRING) AS ORDER_SUBTOTALAMOUNT,
   CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS STRING) AS ORDER_TAXAMOUNT,
   CAST(NULL AS STRING) AS ORDER_GIFTBOXAMOUNT,
   CAST(NULL AS STRING) AS ORDER_GIFTBOXTAXAMOUNT,
   CAST(CASE
        WHEN ABS(coalesce(ord_line.fv_ord_ln_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ord_line.fv_gift_card_value,0))
        ELSE 
			ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) + 
			ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) +
			ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) +
			ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0))
   END AS STRING) AS ORDER_TOTALAMOUNT,
   CAST(ABS(coalesce(ord_line.fv_gift_card_value,ord_line.fv_unit_price,0)) AS STRING) AS ORDER_UNITPRICE,
    CAST(NULL AS STRING) order_giftCardNum,
    CAST(CASE upper(LOC.LOC_TYPE_ID)
            WHEN 'DC' THEN 'WHSE'
            WHEN 'STORE' THEN 'STORE'
            WHEN 'SUPPLIER' THEN 'DROPSHIP'
        END AS STRING) AS order_inventoryLocation,
    cast(ord_line.itm_desc as STRING) AS order_product_name,
    cast(ord_line.small_image_u_r_i as STRING) order_product_image,
    CAST(case
            when ord_line.is_backorderFlg=1 then 'true'
            when ord_line.is_backorderFlg=0 then 'false'
        end AS STRING) as order_backorderFlag,
    CAST(upper(case when ord_line.is_gift_card=1 then ord_line.itm_brand else pm.global_brand_desc end) AS STRING) AS order_product_brand, --CAST(ord_line.itm_brand as STRING) AS order_product_brand,
    CAST(case when ord_line.is_gift_card=1 then ord_line.itm_dept_name else pm.fob_desc end AS STRING) AS order_product_category,
    CAST(case when ord_line.is_gift_card=1 then ord_line.itm_color_desc else pm.desc_long_2 end AS STRING) as order_product_color, --CAST(ord_line.itm_color_desc AS STRING) AS order_product_color,
    CAST(pm.desc AS STRING) AS order_product_description,
    CAST(case
            when ord_hdr.is_prepaid=1 then 'true'
            when ord_hdr.is_prepaid=0 then 'false'
    end AS STRING) AS order_product_isCollectUpFront,
    CAST(CASE
            WHEN ord_line.is_launch_sku_flg = 1 THEN 'true'
            WHEN ord_line.is_launch_sku_flg = 0 THEN 'false'
    END AS STRING) AS order_product_launch_SkuFlag,
    CAST(case when ord_line.is_gift_card=1 then 'GFT' else pm.designator_id end AS STRING) AS order_product_designator,
    cast(trim(CASE
                WHEN ord_line.item_id = 'ECARD20' THEN '2138264'
                WHEN ord_line.item_id = 'ECARD21' THEN '2138265'
                WHEN ord_line.item_id = 'ECARD22' THEN '2138266'
                WHEN ord_line.item_id = 'ECARD45' THEN '20'
                WHEN ord_line.item_id = 'ECARD77' THEN '2000003'
                WHEN ord_line.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
                WHEN ord_line.org_id in ('FL-US','KFL-US','CH-US') THEN pm.internal_product_number
                else ord_line.item_id
    END) as string) AS order_product_number,
    CAST(ord_line.product_type AS STRING) AS order_product_type,
    CAST(case when ord_line.is_gift_card=1 then ord_line.itm_size else pm.legacy_size_desc end AS STRING) AS order_product_size, --CAST(ORD_LINE.itm_size AS STRING) AS order_product_size,
    cast(case
              when ord_line.is_gift_card=1 then ord_line.item_id
             WHEN ord_hdr.CHANNEL !='XSTORE' AND  ord_line.org_id in ('FL-US','KFL-US','CH-US') then pm.online_us_sku
              WHEN ord_hdr.CHANNEL !='XSTORE' AND  ord_line.org_id in ('FL-CA','CH-CA') then pm.online_ca_sku
              WHEN ord_hdr.CHANNEL ='XSTORE' THEN PM_DIV.legacy_sku_size
              else itm.COLOR
    end as string) AS order_product_sku,
    CAST(case when ord_line.is_gift_card=1 then ord_line.itm_tax_cd else pm.tax_code end AS STRING) AS order_product_taxCode, --CAST(ord_line.itm_tax_cd AS STRING) AS order_product_taxCode,
    CAST(case when ord_line.max_fulflmnt_status_id in ('9000','19000') then ord_line.orig_ord_qty else ord_line.QTY end AS STRING) AS order_quantity,
    CAST(ord_line.cart_shpmnt_method AS STRING) AS order_shipMethod,
    CAST(ord_line.tax_cd AS STRING) AS order_taxCode,
    CAST(ord_line.cart_shpmnt_method AS STRING) AS store_fulfillment_shipMethod,
    CAST(dim_loc.loc_snum AS STRING) AS store_fulfillment_storeNumber,
    CAST(NULL AS STRING) store_fulfillment_fulfillmentType,
    CASE
        WHEN UPPER(DLVRY_METHOD_ID) IN ('PICKUPATSTORE','PICKUP_IN_STORE') then ord_hdr.CUST_EMAIL
    END AS store_fulfillment_pickupPersonEmail,
    CAST(NULL AS STRING) store_fulfillment_storeCostOfGoods,
    CAST(NULL AS STRING) AS store_fulfillment_deliveryEstimateId,  --old mapping --case when ord_line.ESTIMATED_DLVRY_TS is not null then '1' when ord_line.ESTIMATED_DLVRY_TS is null then '0'  end
    cast(case when exch.is_even_exchg is not null then 'true' when ord_line.is_even_exchg = 1 then 'true' end as string) as exchangeOrder_flag,
	  pymt.bill_addr_city as billing_city,
	  pymt.bill_addr_country as billing_country,
	  pymt.bill_addr_email as billing_email,
	  pymt.bill_addr_postal_cd as billing_postal_code,
	  pymt.bill_addr_state as billing_postal_state,
	  cast('false' as string) as order_giftbox_flag,
	  cast(case ord_line.IS_GIFT
      when 1 then 'true'
      when 0 then 'false'
    end as string) as order_giftorder_flag,
	  CAST(ord_hdr.ORD_LOCALE AS STRING) as order_langid,
    CASE
    WHEN ord_line.ord_coupons IS NOT NULL THEN
      to_json(
        transform(
          from_json(ord_line.ord_coupons, 'array<struct<amount:int, couponCode:string, promoCode:string, promoGroup:string>>'),
          x -> struct(
            x.amount        AS amount,
            x.couponCode    AS couponCode,
            x.promoCode     AS promoCode,
            'FIXED'         AS DiscountType,
            x.promoGroup    AS promoGroup
          )
        )
      )
    ELSE '[]'
  END AS coupons,
	CAST(case when ord_hdr.channel ='XSTORE' then 'XSTORE' else lower(ord_hdr.channel) end AS STRING) as channel,
	CAST(case when ord_line.IS_APPEASEMENT=1 then 'true' end AS STRING) AS appeasementorder,
  CAST(case when ord_hdr.ord_total=0 and ord_hdr.ord_type_id='CallCenter' and ord_line.is_gift_card!=1 then 'true' else 'false' end AS STRING) as noChargeOrder,
  CAST(ord_hdr.created_ts AS STRING) AS order_datetime,
  CAST(CASE WHEN ord_line.is_base_shipping_charged = 1 THEN true ELSE false END AS STRING) AS baseShippingCharged,
  CAST(ABS(ord_line.pkg_cnt) AS STRING) as shippableConsignmentCount,
  CAST(NULL AS STRING) referralSite,
  CAST(ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_amt,0) + coalesce(ol_agg.ol_ord_shipping_amt,0)) AS STRING) AS oh_base_shipping_amount,
  CAST(ord_hdr.CCY_CD AS STRING) AS oh_currency,
  CAST(ABS(coalesce(ol_agg.ol_cnlled_ord_coupon_amt,0) + coalesce(ol_agg.ol_ord_coupon_amt,0)) AS STRING) oh_couponamount,
  CAST(NULL AS STRING) oh_giftBoxAmount,
  CAST(NULL AS STRING) oh_giftBoxTaxAmount,
  CAST(ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_tax_amt,0) + coalesce(ol_agg.ol_ord_shipping_tax_amt,0)) AS STRING) AS oh_baseShippingTaxAmount,
  CAST(NULL AS STRING) oh_settledAmount,
  cast(ABS(coalesce(ol_agg.ol_cnlled_total_disc,0) + coalesce(ol_agg.ol_total_disc,0)) as string) AS oh_discounted_amount,
  CAST(CASE
        WHEN ABS(coalesce(ol_agg.ol_ord_ln_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ol_agg.ol_gift_card_value,0))
        ELSE 
			(ABS(coalesce(ol_agg.ol_cnlled_ord_ln_sub_total,0) + coalesce(ol_agg.ol_ord_ln_sub_total,0)) + 
			ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_amt,0) + coalesce(ol_agg.ol_ord_shipping_amt,0)) +
			ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_tax_amt,0) + coalesce(ol_agg.ol_ord_shipping_tax_amt,0)) +
			ABS(coalesce(ol_agg.ol_cnlled_ord_sales_tax_amt,0) + coalesce(ol_agg.ol_ord_sales_tax_amt,0))) -
			ABS(coalesce(ol_agg.ol_cnlled_total_disc,0) + coalesce(ol_agg.ol_total_disc,0))
  END AS STRING) AS oh_discounted_total_amount,
  case when pymt.PYMT_GATEWAY_ID ='FootLockerPaymentGateway' then 'INTERNAL' else pymt.PYMT_GATEWAY_ID end AS oh_gateway,
  CAST(ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_amt,0) + coalesce(ol_agg.ol_ord_shipping_amt,0)) + ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_tax_amt,0) + coalesce(ol_agg.ol_ord_shipping_tax_amt,0)) AS STRING) AS oh_shipping_amount,
  CAST(CASE
        WHEN ABS(coalesce(ol_agg.ol_ord_ln_sub_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ol_agg.ol_gift_card_value,0))
        ELSE ABS(coalesce(ol_agg.ol_cnlled_ord_ln_sub_total,0) + coalesce(ol_agg.ol_ord_ln_sub_total,0))
  END AS STRING) AS oh_subTotal_amount,
  CAST(ABS(coalesce(ol_agg.ol_cnlled_ord_sales_tax_amt,0) + coalesce(ol_agg.ol_ord_sales_tax_amt,0)) AS STRING) AS oh_tax_amount,
  CAST(CASE
        WHEN ABS(coalesce(ol_agg.ol_ord_ln_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ol_agg.ol_gift_card_value,0))
        ELSE 
			ABS(coalesce(ol_agg.ol_cnlled_ord_ln_sub_total,0) + coalesce(ol_agg.ol_ord_ln_sub_total,0)) + 
			ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_amt,0) + coalesce(ol_agg.ol_ord_shipping_amt,0)) +
			ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_tax_amt,0) + coalesce(ol_agg.ol_ord_shipping_tax_amt,0)) +
			ABS(coalesce(ol_agg.ol_cnlled_ord_sales_tax_amt,0) + coalesce(ol_agg.ol_ord_sales_tax_amt,0))
  END AS STRING) AS oh_total_amount,
  ord_hdr.CUST_EMAIL AS email,
	lower(ord_hdr.cust_id) as user_id,
	lower(ord_hdr.cust_type_id) as user_type,
	cast(null as string) as controllercustomerid,
	cast(null as string) as relatecustomerid,
	ord_hdr.FLX_ID as flxid,
  CAST(NULL AS STRING) order_affiliateIdTime,
  CAST(NULL AS STRING) order_affiliate_id,
  cast(ord_hdr.prnt_resrv_req_id as string) order_flrequest_id,
  cast(ord_hdr.prnt_resrv_req_id as string) order_request_id,
  CAST( DATE_FORMAT(
            TO_TIMESTAMP(ord_hdr.captured_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000Z'"
        )AS STRING) AS order_request_date,
  CAST(NULL AS STRING) order_request_type,
  CAST(ord_hdr.cust_id AS STRING) as order_requester,
  CAST(CASE WHEN ord_line.RUSH_FLG = 1 THEN true ELSE false END AS STRING) as rush_flag,
  ord_hdr.ASSOCIATE_NUM as sales_personID,
  lower(ord_line.cart_shpmnt_method) as ship_method,
  ord_line.shpmnt_method as ship_method_desc,
  ord_hdr.VENDOR_ID as vendorId,
  ord_line.ord_id as web_order_number,
  CAST(NULL AS STRING) as migratedOrder,
  ord_line.appeasment_reason_cd as order_overrideReasoncd,
 CASE
        WHEN LOWER(ord_hdr.ord_type_id) in ('web','return') THEN 'CAS'
        WHEN LOWER(ord_hdr.ord_type_id) = 'callcenter' THEN 'CUSTOMER_SVC'
        WHEN LOWER(ord_hdr.ord_type_id) = 'savethesale' THEN 'XSTORE'
        WHEN LOWER(ord_hdr.ord_type_id) = 'launch' THEN 'LAUNCH_RESERVATION'
        ELSE UPPER(ord_hdr.ord_type_id)
  END as order_source,
  ord_line.RELATE_ORD_NUM_CSA as relatedordernumber_csa,
  CAST(NULL AS STRING) as controllerOrderNumber,
	ord_line.ship_to_addr_city as shipping_city,
	ord_line.ship_to_addr_country as shipping_country,
	ord_line.ship_to_addr_state as shipping_state,
	ord_line.ship_to_addr_postal_cd as shipping_postal_code,
	ord_line.ship_to_addr_email as shipping_email,
  CAST(CASE WHEN ord_hdr.channel='customer_service' THEN ord_hdr.associate_num ELSE NULL END AS STRING) as csaAgentId,
  CAST(ord_hdr.csa_ord_note AS STRING) as csaOrderNote,
  CAST(NULL AS STRING) order_division,
  CAST(ord_line.ORD_ID AS STRING) omsOrderId,
  cast(ord_line.UPDATED_BY as string) as postedBy,
  CAST( DATE_FORMAT(
            TO_TIMESTAMP(ord_line.UPDATED_TS, 'yyyy-MM-dd HH:mm:ss.SSS'),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000Z'"
        ) as string) AS postedAt,
  CAST(ord_line.UPDATED_TS as timestamp) as load_time_kafka,
  CAST(DATE(ord_hdr.created_ts)  AS STRING) AS order_date,
  CAST(ord_line.etl_updt_ts as timestamp) as load_time_adls,
  CAST(ROUND(ABS(coalesce(pm.cost, pm.size_default_established_cost, pm.size_default_established_cost_flca) * ord_line.QTY),2) AS DOUBLE) AS cogs,
  cast(ord_line.UPDATED_TS as timestamp) as updated_datetime,
  CAST(
        CASE
            WHEN ord_line.DLVRY_METHOD_ID = 'ShipToStore' AND ord_line.Ship_from_loc_id IS NOT NULL THEN true
            ELSE false
        END AS STRING
    ) AS s2s,
  cast(ord_line.ORD_LN_PK as string) AS lineId,
  case upper(ord_line.UOM) when 'EA' then 'EACH' when 'U' then 'UNIT' end AS uom,
  ord_line.ITEM_ID AS productId,
  cast(case when ord_line.cnl_reason_id = 'FRAUD' then 'false' else 'true' end as STRING) AS obfOrder,
  ARRAY(
    STRUCT(
    cast(dim_loc.loc_num as string) AS locationId,
    cast(UPPER(LOC.LOC_TYPE_ID) as string) AS locationType
    )
  ) AS locationReservationDetails,
  struct(
    loc.loc_addr1 AS addressLine1,
    loc.loc_addr2 AS addressLine2,
    loc.loc_addr_city AS city,
    loc.PRNT_ORG_ID AS companyName,
    loc.loc_addr_country AS country,
    loc.loc_addr_country AS countryCode,
    loc.loc_addr_email AS email,
    loc.loc_addr_first_name AS firstName,
    loc.loc_addr_last_name AS lastName,
    loc.loc_addr_phone_no AS phoneNumber,
    loc.loc_addr_postal_cd AS postalCode,
    loc.loc_addr_state AS state,
    null as stateCode
   ) AS storeAddress,
  coalesce(ord_line.ord_coupons,'[]') as OrderLineDiscounts,
  cast(case ord_line.is_loyalty_disc when 1 then 'true' end as string) as loyaltyDiscount,
  CAST(NULL AS STRING) loyaltyPointsRedeemed,
  ord_line.loyalty_reward_id as loyaltyRewardId,
  CAST(NULL AS STRING) loyaltyRedemptionId,
  CAST(ord_hdr.usr_agnt AS STRING) userAgent,
  CAST(parse_user_agent(ord_hdr.usr_agnt, 'info') AS STRING) user_agent_info,
  CAST(parse_user_agent(ord_hdr.usr_agnt, 'type') AS STRING) device_type,
  'MAO' As ref_source
from
  fct_mao_ord_line_with_fv ord_line
  join ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v ord_hdr on ord_line.org_id = ord_hdr.org_id and ord_line.ord_id = ord_hdr.ord_id
  left join fct_mao_ord_line_agg ol_agg on (ol_agg.org_id = ord_line.org_id and ol_agg.ord_id = ord_line.ord_id)
  left join fct_mao_ord_pymt_line pymt on (pymt.org_id = ord_line.org_id and  pymt.ord_id = ord_line.ord_id)
  left join fct_mao_fulfillment_line fl on (ord_line.org_id = fl.org_id and ord_line.ord_id = fl.ord_id)
  left join product_master pm ON ((ord_line.is_gift_card!=1 and trim(ord_line.item_id) = trim(pm.global_size_id) AND (CASE WHEN ord_line.ORG_ID IN ('FL-CA','CH-CA') THEN '98' ELSE '81' END)=pm.banner_id))
  left join product_master_div pm_div ON (ord_line.is_gift_card!=1 and trim(ord_line.item_id) = trim(pm_div.global_size_id) AND ord_line.ORG_ID = pm_div.org_desc)
  left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_item_v itm on (trim(ord_line.ITEM_ID) = trim(itm.ITEM_ID))
  left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v loc on  (coalesce(ord_line.ship_from_loc_id,ord_line.physical_org_id) = loc.loc_id)
  left join dim_location dim_loc on (lpad(dim_loc.loc_snum,5,0) = lpad(coalesce(ord_line.ship_from_loc_id,ord_line.physical_org_id),5,0))
  left join fct_exchange_orders exch on (ord_line.ORG_ID = exch.ORG_ID and  ord_line.ORD_ID = exch.PRNT_ORD_ID)
  left join ${dom_gold_db}.${dom_gold_schema}.lkp_cancel_code_reason_v  cnl_reason on (ord_line.cnl_reason_id =cnl_reason.cancel_reason_id)
 WHERE  ord_hdr.doc_type_id = 'CustomerOrder'
        AND NOT (ord_line.max_fulflmnt_status_id IS NULL OR ord_line.max_fulflmnt_status_id > 9000)
        AND (ord_line.prnt_ord_id IS NULL OR ord_line.is_even_exchg = 1)
        AND ord_line.etl_updt_ts > '${lookback_date}'
);


CREATE OR REPLACE TEMP VIEW orders_landing AS
(
with
  fct_mao_ord_line_stg as (
    select ord_line.*,
      case
          when ord_line.max_fulflmnt_status_id = '1000' then 'SUBMITTED'
          when ord_line.max_fulflmnt_status_id = '1500' then 'SUBMITTED'
          when ord_hdr.is_fraud_service_failed = 1 then 'FRAUD_CHECK_FAILED'
          when ord_line.max_fulflmnt_status_id = '1600' then 'FULFILMENT_PROCESSING'
          when ord_line.max_fulflmnt_status_id = '2000' then 'FULFILMENT_PROCESSING'
          when ord_line.max_fulflmnt_status_id = '3000' then 'FULFILMENT_PROCESSING'
          when ord_line.max_fulflmnt_status_id = '3500' then 'FULFILMENT_PROCESSING'
          when ord_line.max_fulflmnt_status_id = '3600' then 'FULFILMENT_PROCESSING'
          when ord_line.max_fulflmnt_status_id = '3700' then 'FULFILMENT_PROCESSING'
          when ord_line.max_fulflmnt_status_id = '7000' then 'FULFILMENT_COMPLETE'
          when ord_line.max_fulflmnt_status_id = '7500' then 'FULFILMENT_COMPLETE'
          when ord_line.max_fulflmnt_status_id = '8000' then 'FULFILMENT_COMPLETE'
          when ord_line.max_fulflmnt_status_id = '8500' then 'FULFILMENT_COMPLETE'
          when ord_line.max_fulflmnt_status_id = '9000' then 'CANCELLED'
          when ord_line.max_fulflmnt_status_id = '13000' then 'WAIT_FRAUD_SYSTEM_CHECK'
      else upper(ord_line.max_fulflmnt_status_desc)
      end as order_status,
	  first_value(ord_line.orig_unit_price) OVER (partition BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_orig_unit_price,
      first_value(ord_line.unit_price) OVER (partition BY ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id ORDER BY ord_line.max_fulflmnt_status_id ASC, ord_line.updated_ts ASC) AS fv_unit_price,
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
      first_value(ord_line.gift_card_value) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_gift_card_value,
      first_value(ord_line.cnlled_ord_coupon_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_cnlled_ord_coupon_amt,
      first_value(ord_line.ord_coupon_amt) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_ord_coupon_amt
    from
  	  ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_line_hist_v ord_line
      join ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v ord_hdr on ord_line.org_id = ord_hdr.org_id and ord_line.ord_id = ord_hdr.ord_id
      WHERE ord_hdr.doc_type_id = 'CustomerOrder'
      AND NOT (ord_line.max_fulflmnt_status_id IS NULL OR ord_line.max_fulflmnt_status_id > 9000)
      AND (ord_line.prnt_ord_id IS NULL OR ord_line.is_even_exchg = 1)
  ),
  fct_mao_ord_line as (
    select *
    from
      (select ord_line.*,
        row_number() over (partition by org_id,ord_id,ord_ln_id,order_status order by updated_ts desc) as ord_ln_status_rnk
      from fct_mao_ord_line_stg ord_line )
    where ord_ln_status_rnk=1 and etl_updt_ts > '${lookback_date}'
  ),
  fct_mao_ord_line_agg as (
    -- Same pattern as orders (refined): pick ONE row per (org_id, ord_id, ord_ln_id),
    -- then SUM fv_ amounts per order — each line counted exactly once regardless of statuses.
    select
        ord_id,
        org_id,
        sum(fv_cnlled_ord_coupon_amt) as ol_cnlled_ord_coupon_amt,
        sum(fv_ord_coupon_amt) as ol_ord_coupon_amt,
        sum(fv_cnlled_ord_sales_tax_amt) as ol_cnlled_ord_sales_tax_amt,
        sum(fv_ord_sales_tax_amt) as ol_ord_sales_tax_amt,
        sum(fv_cnlled_ord_shipping_tax_amt) as ol_cnlled_ord_shipping_tax_amt,
        sum(fv_ord_shipping_tax_amt) as ol_ord_shipping_tax_amt,
        sum(fv_gift_card_value) as ol_gift_card_value,
        sum(fv_cnlled_total_disc) as ol_cnlled_total_disc,
        sum(fv_total_disc) as ol_total_disc,
        sum(fv_cnlled_ord_ln_total) as ol_cnlled_ord_ln_total,
        sum(fv_ord_ln_total) as ol_ord_ln_total,
        sum(fv_cnlled_ord_ln_sub_total) as ol_cnlled_ord_ln_sub_total,
        sum(fv_ord_ln_sub_total) as ol_ord_ln_sub_total,
        sum(fv_cnlled_total_charges) as ol_cnlled_total_charges,
        sum(fv_total_charges) as ol_total_charges,
        sum(fv_cnlled_total_taxes) as ol_cnlled_total_taxes,
        sum(fv_total_taxes) as ol_total_taxes,
        sum(fv_cnlled_ord_shipping_amt) as ol_cnlled_ord_shipping_amt,
        sum(fv_ord_shipping_amt) as ol_ord_shipping_amt
    from (
        select *
        from (
            select *,
                row_number() over (partition by org_id, ord_id, ord_ln_id order by updated_ts asc) as agg_rn
            from fct_mao_ord_line_stg
        ) where agg_rn = 1
    )
    group by ord_id, org_id
  ),
fct_mao_fulfillment_line as (
	select org_id,ord_id,max(fulflmnt_id) fulflmnt_id from ${dom_gold_db}.${dom_gold_schema}.fct_mao_fulfillment_line_v group by all
),
fct_mao_ord_pymt_line as (
  select *
  from (
    SELECT org_id,ord_id,
      addr_firstname as bill_addr_firstname,
      addr_lastname as bill_addr_lastname,
      addr_addr1 as bill_addr_addr1,
      addr_addr2 as bill_addr_addr2,
      addr_addr3 as bill_addr_addr3,
      addr_city as bill_addr_city,
      addr_postal_cd as bill_addr_postal_cd,
      addr_state as bill_addr_state,
      addr_country as bill_addr_country,
      addr_email as bill_addr_email,
      addr_phone as bill_addr_phone,
	    pymt_gateway_id,
      row_number() over (partition by org_id,ord_id order by src_load_ts desc ) as ord_pymt_rnk
    FROM ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_pymt_line_v
    where ord_id is not null)
  where ord_pymt_rnk=1
),
fct_exchange_orders as (
    select distinct ol.org_id,ol.prnt_ord_id,ol.is_even_exchg
    from ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_line_v ol
        join ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v  oh on (ol.org_id = oh.org_id and ol.ord_id = oh.ord_id)
    where oh.doc_type_id = 'CustomerOrder'
    and ol.is_even_exchg = 1
    and ol.prnt_ord_id is not null
    and ol.max_fulflmnt_status_id is not null
),
dim_location as (
    select * from
        (select
            loc_snum,loc_num,row_number() over (partition by lpad(loc_snum, 5, 0) order by loc_sk desc,loc_seq_num desc) loc_rnk
        from sf_gold_prod_db.location_gold_prod.dim_location_v where UPPER(banner_geo)='NA')
    where loc_rnk=1
)
SELECT
    ord_line.ORD_ID AS order_id,
    cast(case trim(ord_line.org_id)
      when 'FL-US' then '21'
      when 'FL-CA' then '45'
      when 'KFL-US' then '22'
      when 'CH-CA' then '77'
      when 'CH-US' then '20'
    end as string) as company_number,
    ord_line.order_status,
    cast(cnl_reason.oms_cancel_code as string) as cancel_code,
    cast(ord_line.cnl_reason_desc as string) cancelReason,
    CASE
        WHEN UPPER(ord_line.DLVRY_METHOD_ID) IN ('PICKUPATSTORE', 'PICKUP_IN_STORE', 'SHIPTORETURNCENTER') THEN 'PICK'
        WHEN UPPER(ord_line.DLVRY_METHOD_ID) IN ('SHIPTOADDRESS', 'SHIPTOSTORE') THEN 'SHIP'
        WHEN UPPER(ord_line.DLVRY_METHOD_ID) = 'EMAIL' THEN 'ELECTRONIC'
        WHEN UPPER(ord_line.DLVRY_METHOD_ID)  = 'STORERETURN' THEN 'STORE_RETURN'
        WHEN UPPER(ord_line.DLVRY_METHOD_ID)  = 'STORESALE' THEN 'XSTORE'
        else UPPER(ord_line.DLVRY_METHOD_ID)
    END AS fullfillment_type,
    from_json(upper(ord_hdr.pymt_type), "array<string>") as payment_type,
    CAST(case when IS_FREE_SHIPPING=0 then true
         when IS_FREE_SHIPPING=1 then false
         else null end AS STRING)  AS free_shipping,
    ord_line.ORD_LN_ID AS order_lineNumber,
    ord_hdr.CCY_CD AS order_currency,
    CAST(NULL AS STRING) order_salecode,
    CAST(ord_line.PRICE_OVERRIDE_REASON AS STRING) order_priceOverrideReason,
    CAST(ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0)) AS STRING) AS ORDER_DISCOUNTAMOUNT,
    CAST(CASE
        WHEN ABS(coalesce(ord_line.fv_ord_ln_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ord_line.fv_gift_card_value,0))
        ELSE 
			(ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) + 
			ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) +
			ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) +
			ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0))) -
			ABS(coalesce(ord_line.fv_cnlled_total_disc,0) + coalesce(ord_line.fv_total_disc,0))
    END AS STRING) AS ORDER_DISCOUNTED_TOTALAMOUNT,
    CAST(ABS(coalesce(ord_line.fv_orig_unit_price, ord_line.fv_unit_price,0)) AS STRING) AS ORDER_ORIGINAL_RETAILPRICE,
    CAST(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) AS STRING) AS ORDER_SHIPPINGAMOUNT,
    CAST(ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) AS STRING) AS ORDER_SHIPPINGTAXAMOUNT,
    CAST(CASE
        WHEN ABS(coalesce(ord_line.fv_ord_ln_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ord_line.fv_gift_card_value,0))
        ELSE ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0))
    END AS STRING) AS ORDER_SUBTOTALAMOUNT,
    CAST(ABS(COALESCE(ord_line.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(ord_line.fv_ord_sales_tax_amt, 0)) AS STRING) AS ORDER_TAXAMOUNT,
    CAST(NULL AS STRING) AS ORDER_GIFTBOXAMOUNT,
    CAST(NULL AS STRING) AS ORDER_GIFTBOXTAXAMOUNT,
    CAST(CASE
        WHEN ABS(coalesce(ord_line.fv_ord_ln_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ord_line.fv_gift_card_value,0))
        ELSE 
			ABS(coalesce(ord_line.fv_cnlled_ord_ln_sub_total,0) + coalesce(ord_line.fv_ord_ln_sub_total,0)) + 
			ABS(coalesce(ord_line.fv_cnlled_ord_shipping_amt,0) + coalesce(ord_line.fv_ord_shipping_amt,0)) +
			ABS(coalesce(ord_line.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ord_line.fv_ord_shipping_tax_amt,0)) +
			ABS(coalesce(ord_line.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ord_line.fv_ord_sales_tax_amt,0))
    END AS STRING) AS ORDER_TOTALAMOUNT,
    CAST(ABS(coalesce(ord_line.fv_gift_card_value,ord_line.fv_unit_price,0)) AS STRING) AS ORDER_UNITPRICE,
    CAST(NULL AS STRING) order_giftCardNum,
    CAST(CASE upper(LOC.LOC_TYPE_ID)
            WHEN 'DC' THEN 'WHSE'
            WHEN 'STORE' THEN 'STORE'
            WHEN 'SUPPLIER' THEN 'DROPSHIP'
        END AS STRING) AS order_inventoryLocation,
    cast(ord_line.itm_desc as STRING) AS order_product_name,
    cast(ord_line.small_image_u_r_i as STRING) order_product_image,
    CAST(case
            when ord_line.is_backorderFlg=1 then 'true'
            when ord_line.is_backorderFlg=0 then 'false'
        end AS STRING) as order_backorderFlag,
    CAST(upper(case when ord_line.is_gift_card=1 then ord_line.itm_brand else pm.global_brand_desc end) AS STRING) AS order_product_brand, --CAST(ord_line.itm_brand as STRING) AS order_product_brand,
    CAST(case when ord_line.is_gift_card=1 then ord_line.itm_dept_name else pm.fob_desc end AS STRING) AS order_product_category,
    CAST(case when ord_line.is_gift_card=1 then ord_line.itm_color_desc else pm.desc_long_2 end AS STRING) as order_product_color, --CAST(ord_line.itm_color_desc AS STRING) AS order_product_color,
    CAST(pm.desc AS STRING) AS order_product_description,
    CAST(case
            when ord_hdr.is_prepaid=1 then 'true'
            when ord_hdr.is_prepaid=0 then 'false'
    end AS STRING) AS order_product_isCollectUpFront,
    CAST(CASE
            WHEN ord_line.is_launch_sku_flg = 1 THEN 'true'
            WHEN ord_line.is_launch_sku_flg = 0 THEN 'false'
    END AS STRING) AS order_product_launch_SkuFlag,
    CAST(case when ord_line.is_gift_card=1 then 'GFT' else pm.designator_id end AS STRING) AS order_product_designator,
    cast(trim(CASE
                WHEN ord_line.item_id = 'ECARD20' THEN '2138264'
                WHEN ord_line.item_id = 'ECARD21' THEN '2138265'
                WHEN ord_line.item_id = 'ECARD22' THEN '2138266'
                WHEN ord_line.item_id = 'ECARD45' THEN '20'
                WHEN ord_line.item_id = 'ECARD77' THEN '2000003'
                WHEN ord_line.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
                WHEN ord_line.org_id in ('FL-US','KFL-US','CH-US') THEN pm.internal_product_number
                else ord_line.item_id
    END) as string) AS order_product_number,
    CAST(ord_line.product_type AS STRING) AS order_product_type,
    CAST(case when ord_line.is_gift_card=1 then ord_line.itm_size else pm.legacy_size_desc end AS STRING) AS order_product_size, --CAST(ORD_LINE.itm_size AS STRING) AS order_product_size,
    cast(case
              when ord_line.is_gift_card=1 then ord_line.item_id
              WHEN ord_hdr.CHANNEL !='XSTORE' AND  ord_line.org_id in ('FL-US','KFL-US','CH-US') then pm.online_us_sku
              WHEN ord_hdr.CHANNEL !='XSTORE' AND  ord_line.org_id in ('FL-CA','CH-CA') then pm.online_ca_sku
              WHEN ord_hdr.CHANNEL ='XSTORE' THEN PM_DIV.legacy_sku_size
              else itm.COLOR
    end as string) AS order_product_sku,
    CAST(case when ord_line.is_gift_card=1 then ord_line.itm_tax_cd else pm.tax_code end AS STRING) AS order_product_taxCode, --CAST(ord_line.itm_tax_cd AS STRING) AS order_product_taxCode,
    CAST(case when ord_line.max_fulflmnt_status_id in ('9000','19000') then ord_line.orig_ord_qty else ord_line.QTY end AS STRING) AS order_quantity,
    CAST(ord_line.cart_shpmnt_method AS STRING) AS order_shipMethod,
    CAST(ord_line.tax_cd AS STRING) AS order_taxCode,
    CAST(ord_line.cart_shpmnt_method AS STRING) AS store_fulfillment_shipMethod,
    CAST(dim_loc.loc_snum AS STRING) AS store_fulfillment_storeNumber,
    CAST(NULL AS STRING) store_fulfillment_fulfillmentType,
    CASE
        WHEN UPPER(DLVRY_METHOD_ID) IN ('PICKUPATSTORE','PICKUP_IN_STORE') then ord_hdr.CUST_EMAIL
    END AS store_fulfillment_pickupPersonEmail,
    CAST(NULL AS STRING) AS store_fulfillment_pickupPersonMobile,
    CAST(NULL AS STRING) AS store_fulfillment_storeCostOfGoods,
    CAST(NULL AS STRING) AS store_fulfillment_deliveryEstimateId,  --old mapping --case when ord_line.ESTIMATED_DLVRY_TS is not null then '1' when ord_line.ESTIMATED_DLVRY_TS is null then '0'  end
    CAST(NULL AS STRING) AS store_fulfillment_deliveryInstructions,
    CAST(NULL AS STRING) AS store_fulfillment_deliverycustomerphone,
    cast(case when exch.is_even_exchg is not null then 'true' when ord_line.is_even_exchg = 1 then 'true' end as string) as exchangeOrder_flag,
    pymt.bill_addr_addr1 as billing_address_line1,
    pymt.bill_addr_addr2 as billing_address_line2,
	  pymt.bill_addr_city as billing_city,
	  pymt.bill_addr_country as billing_country,
	  pymt.bill_addr_email as billing_email,
    pymt.bill_addr_firstname as billing_first_name,
    pymt.bill_addr_lastname as billing_last_name,
	  pymt.bill_addr_postal_cd as billing_postal_code,
	  pymt.bill_addr_state as billing_postal_state,
    pymt.bill_addr_phone as billing_phoneNumber,
    CAST(NULL AS STRING) as order_deviceID,
	  cast('false' as string) as order_giftbox_flag,
	  cast(case ord_line.IS_GIFT
      when 1 then 'true'
      when 0 then 'false'
    end as string) as order_giftorder_flag,
	CAST(ord_hdr.ORD_LOCALE AS STRING) as order_langid,
  CASE
    WHEN ord_line.ord_coupons IS NOT NULL THEN
      to_json(
        transform(
          from_json(ord_line.ord_coupons, 'array<struct<amount:int, couponCode:string, promoCode:string, promoGroup:string>>'),
          x -> struct(
            x.amount        AS amount,
            x.couponCode    AS couponCode,
            x.promoCode     AS promoCode,
            'FIXED'         AS DiscountType,
            x.promoGroup    AS promoGroup
          )
        )
      )
    ELSE '[]'
  END AS coupons,
	CAST(case when ord_hdr.channel ='XSTORE' then 'XSTORE' else lower(ord_hdr.channel) end AS STRING) as channel,
	CAST(case
    when ord_line.IS_APPEASEMENT=1 then 'true'
  end AS STRING) AS appeasementorder,
  CAST(case when ord_hdr.ord_total=0 and ord_hdr.ord_type_id='CallCenter' and ord_line.is_gift_card!=1 then 'true' else 'false' end AS STRING) as noChargeOrder,
  CAST(ord_hdr.created_ts AS STRING) AS order_datetime,
  CAST(CASE WHEN ord_line.is_base_shipping_charged = 1 THEN true ELSE false END AS STRING) AS baseShippingCharged,
  CAST(ABS(ord_line.pkg_cnt) AS STRING) as shippableConsignmentCount,
  CAST(NULL AS STRING) referralSite,
  CAST(ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_amt,0) + coalesce(ol_agg.ol_ord_shipping_amt,0)) AS STRING) AS oh_base_shipping_amount,
  CAST(ord_hdr.CCY_CD AS STRING) AS oh_currency,
  CAST(ABS(coalesce(ol_agg.ol_cnlled_ord_coupon_amt,0) + coalesce(ol_agg.ol_ord_coupon_amt,0)) AS STRING) oh_couponamount,
  CAST(NULL AS STRING) oh_giftBoxAmount,
  CAST(NULL AS STRING) oh_giftBoxTaxAmount,
  CAST(ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_tax_amt,0) + coalesce(ol_agg.ol_ord_shipping_tax_amt,0)) AS STRING) AS oh_baseShippingTaxAmount,
  CAST(NULL AS STRING) oh_settledAmount,
  cast(ABS(coalesce(ol_agg.ol_cnlled_total_disc,0) + coalesce(ol_agg.ol_total_disc,0)) as string) AS oh_discounted_amount,
  CAST(CASE
        WHEN ABS(coalesce(ol_agg.ol_ord_ln_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ol_agg.ol_gift_card_value,0))
        ELSE
            (ABS(coalesce(ol_agg.ol_cnlled_ord_ln_sub_total,0) + coalesce(ol_agg.ol_ord_ln_sub_total,0)) +
            ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_amt,0) + coalesce(ol_agg.ol_ord_shipping_amt,0)) +
            ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_tax_amt,0) + coalesce(ol_agg.ol_ord_shipping_tax_amt,0)) +
            ABS(coalesce(ol_agg.ol_cnlled_ord_sales_tax_amt,0) + coalesce(ol_agg.ol_ord_sales_tax_amt,0))) -
            ABS(coalesce(ol_agg.ol_cnlled_total_disc,0) + coalesce(ol_agg.ol_total_disc,0))
  END AS STRING) AS oh_discounted_total_amount,
  case when pymt.PYMT_GATEWAY_ID ='FootLockerPaymentGateway' then 'INTERNAL' else pymt.PYMT_GATEWAY_ID end AS oh_gateway,
  CAST(ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_amt,0) + coalesce(ol_agg.ol_ord_shipping_amt,0)) + ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_tax_amt,0) + coalesce(ol_agg.ol_ord_shipping_tax_amt,0)) AS STRING) AS oh_shipping_amount,
  CAST(CASE
        WHEN ABS(coalesce(ol_agg.ol_ord_ln_sub_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ol_agg.ol_gift_card_value,0))
        ELSE ABS(coalesce(ol_agg.ol_cnlled_ord_ln_sub_total,0) + coalesce(ol_agg.ol_ord_ln_sub_total,0))
  END AS STRING) AS oh_subTotal_amount,
  CAST(ABS(coalesce(ol_agg.ol_cnlled_ord_sales_tax_amt,0) + coalesce(ol_agg.ol_ord_sales_tax_amt,0)) AS STRING) AS oh_tax_amount,
  CAST(CASE
        WHEN ABS(coalesce(ol_agg.ol_ord_ln_total,0)) = 0 AND lower(ord_hdr.ord_type_id) = 'callcenter' THEN ABS(coalesce(ol_agg.ol_gift_card_value,0))
        ELSE
            ABS(coalesce(ol_agg.ol_cnlled_ord_ln_sub_total,0) + coalesce(ol_agg.ol_ord_ln_sub_total,0)) +
            ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_amt,0) + coalesce(ol_agg.ol_ord_shipping_amt,0)) +
            ABS(coalesce(ol_agg.ol_cnlled_ord_shipping_tax_amt,0) + coalesce(ol_agg.ol_ord_shipping_tax_amt,0)) +
            ABS(coalesce(ol_agg.ol_cnlled_ord_sales_tax_amt,0) + coalesce(ol_agg.ol_ord_sales_tax_amt,0))
  END AS STRING) AS oh_total_amount,
  ord_hdr.CUST_EMAIL AS email,
  ord_line.ship_to_addr_first_name as first_name,
  ord_line.ship_to_addr_last_name as last_name,
	lower(ord_hdr.cust_id) as user_id,
	lower(ord_hdr.cust_type_id) as user_type,
	cast(null as string) as controllercustomerid,
	cast(null as string) as relatecustomerid,
	ord_hdr.FLX_ID as flxid,
  ord_line.ship_to_addr_phone AS order_phoneNumber,
  CAST(NULL AS STRING) order_affiliateIdTime,
  CAST(NULL AS STRING) order_affiliate_id,
  cast(ord_hdr.prnt_resrv_req_id as string) order_flrequest_id,
  cast(ord_line.resrv_req_id as string) order_request_id,
  CAST( DATE_FORMAT(
            TO_TIMESTAMP(ord_hdr.captured_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000Z'"
        )AS STRING) AS order_request_date,
  CAST(NULL AS STRING) order_request_type,
  CAST(ord_hdr.cust_id AS STRING) as order_requester,
  CAST(CASE WHEN ord_line.RUSH_FLG = 1 THEN true ELSE false END AS STRING) as rush_flag,
  ord_hdr.ASSOCIATE_NUM as sales_personID,
  lower(ord_line.cart_shpmnt_method) as ship_method,
  ord_line.shpmnt_method as ship_method_desc,
	CAST(ord_hdr.browser_ip AS STRING) AS user_ip_address,
  ord_hdr.VENDOR_ID as vendorId,
  ord_line.ord_id as web_order_number,
  CAST(NULL AS STRING) as migratedOrder,
  ord_line.appeasment_reason_cd as order_overrideReasoncd,
  CASE
        WHEN LOWER(ord_hdr.ord_type_id) in ('web','return') THEN 'CAS'
        WHEN LOWER(ord_hdr.ord_type_id) = 'callcenter' THEN 'CUSTOMER_SVC'
        WHEN LOWER(ord_hdr.ord_type_id) = 'savethesale' THEN 'XSTORE'
        WHEN LOWER(ord_hdr.ord_type_id) = 'launch' THEN 'LAUNCH_RESERVATION'
        ELSE UPPER(ord_hdr.ord_type_id)
  END as order_source,
  ord_line.RELATE_ORD_NUM_CSA as relatedordernumber_csa,
  CAST(NULL AS STRING) as controllerOrderNumber,
  ord_line.ship_to_addr_addr1 AS shipping_addressline1,
  ord_line.ship_to_addr_addr2 AS shipping_addressline2,
	ord_line.ship_to_addr_city as shipping_city,
	ord_line.ship_to_addr_country as shipping_country,
	ord_line.ship_to_addr_state as shipping_state,
	ord_line.ship_to_addr_postal_cd as shipping_postal_code,
	ord_line.ship_to_addr_email as shipping_email,
  ord_line.ship_to_addr_first_name AS shipping_first_name,
  ord_line.ship_to_addr_last_name AS shipping_last_name,
  ord_line.ship_to_addr_phone AS shipping_phoneNumber,
  CAST(CASE WHEN ord_hdr.channel='customer_service' THEN ord_hdr.associate_num ELSE NULL END AS STRING) as csaAgentId,
  CAST(ord_hdr.csa_ord_note AS STRING) csaOrderNote,
  CAST(NULL AS STRING) order_division,
  CAST(ord_line.ORD_ID AS STRING) omsOrderId,
  cast(ord_line.UPDATED_BY as string) as postedBy,
  CAST( DATE_FORMAT(
            TO_TIMESTAMP(ord_line.UPDATED_TS, 'yyyy-MM-dd HH:mm:ss.SSS'),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000Z'"
        ) as string) AS postedAt,
  CAST(ord_line.UPDATED_TS as timestamp) as load_time_kafka,
  CAST(DATE(ord_hdr.created_ts) AS STRING) AS order_date,
  CAST(ord_line.etl_updt_TS as timestamp) as load_time_adls,
  CAST(ABS(coalesce(pm.cost, pm.size_default_established_cost, pm.size_default_established_cost_flca) * ord_line.QTY) as DOUBLE) AS cogs,
  CAST(
        CASE
            WHEN ord_line.DLVRY_METHOD_ID = 'ShipToStore' AND ord_line.Ship_from_loc_id IS NOT NULL THEN true
            ELSE false
        END AS STRING
    ) AS s2s,
  cast(ord_line.ORD_LN_PK as string) AS lineId,
  case upper(ord_line.UOM) when 'EA' then 'EACH' when 'U' then 'UNIT' end AS uom,
  ord_line.ITEM_ID AS productId,
  cast(case when ord_line.cnl_reason_id = 'FRAUD' then 'false' else 'true' end as STRING) AS obfOrder,
  ARRAY(
    STRUCT(
    cast(dim_loc.loc_num as string) AS locationId,
    cast(UPPER(LOC.LOC_TYPE_ID) as string) AS locationType
    )
  ) AS locationReservationDetails,
  struct(
    loc.loc_addr1 AS addressLine1,
    loc.loc_addr2 AS addressLine2,
    loc.loc_addr_city AS city,
    loc.PRNT_ORG_ID AS companyName,
    loc.loc_addr_country AS country,
    loc.loc_addr_country AS countryCode,
    loc.loc_addr_email AS email,
    loc.loc_addr_first_name AS firstName,
    loc.loc_addr_last_name AS lastName,
    loc.loc_addr_phone_no AS phoneNumber,
    loc.loc_addr_postal_cd AS postalCode,
    loc.loc_addr_state AS state,
    cast(null as string) as stateCode
   ) AS storeAddress,
  array(cast(coalesce(ord_line.ord_coupons,'[]') as string)) as OrderLineDiscounts,
  cast(case ord_line.is_loyalty_disc
      when 1 then 'true'
  end as string) as loyaltyDiscount,
  CAST(NULL AS STRING) loyaltyPointsRedeemed,
  ord_line.loyalty_reward_id as loyaltyRewardId,
  CAST(NULL AS STRING) loyaltyRedemptionId,
  CAST(ord_hdr.usr_agnt AS STRING) userAgent,
  CAST(parse_user_agent(ord_hdr.usr_agnt, 'info') AS STRING) user_agent_info,
  CAST(parse_user_agent(ord_hdr.usr_agnt, 'type') AS STRING) device_type,
  'MAO' As ref_source
  from
	  fct_mao_ord_line ord_line
    join ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v ord_hdr on ord_line.org_id = ord_hdr.org_id and ord_line.ord_id = ord_hdr.ord_id
    left join fct_mao_ord_line_agg ol_agg on (ol_agg.org_id = ord_line.org_id and ol_agg.ord_id = ord_line.ord_id)
    left join fct_mao_ord_pymt_line pymt on (pymt.org_id = ord_line.org_id and  pymt.ord_id = ord_line.ord_id)
    left join fct_mao_fulfillment_line fl on (ord_line.org_id = fl.org_id and ord_line.ord_id = fl.ord_id)
    left join product_master pm ON ((ord_line.is_gift_card!=1 and trim(ord_line.item_id) = trim(pm.global_size_id) AND (CASE WHEN ord_line.ORG_ID IN ('FL-CA','CH-CA') THEN '98' ELSE '81' END)=pm.banner_id))
    left join product_master_div pm_div ON (ord_line.is_gift_card!=1 and trim(ord_line.item_id) = trim(pm_div.global_size_id) AND ord_line.ORG_ID = pm_div.org_desc)
	left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_item_v itm on (trim(ord_line.ITEM_ID) = trim(itm.ITEM_ID))
	left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v loc on  (coalesce(ord_line.ship_from_loc_id,ord_line.physical_org_id) = loc.loc_id)
    left join dim_location dim_loc on (lpad(dim_loc.loc_snum,5,0) = lpad(coalesce(ord_line.ship_from_loc_id,ord_line.physical_org_id),5,0))
    left join fct_exchange_orders exch on (ord_line.ORG_ID = exch.ORG_ID and  ord_line.ORD_ID = exch.PRNT_ORD_ID)
    left join ${dom_gold_db}.${dom_gold_schema}.lkp_cancel_code_reason_v  cnl_reason on (ord_line.cnl_reason_id =cnl_reason.cancel_reason_id)
)
