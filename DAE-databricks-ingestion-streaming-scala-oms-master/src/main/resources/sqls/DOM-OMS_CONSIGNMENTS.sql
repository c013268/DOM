create or replace temp view product_master as
(
  SELECT distinct
  pm.internal_product_number_flca,
  pm.internal_product_number,
  pm.legacy_size_desc,
  pm.online_us_sku,
  pm.online_ca_sku,
  pm.global_size_id,
  pm.banner_id
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

create or replace temp view consignments as
(
with
pymt_txn_status as (
		select *
		from
			(select org_id,ord_id,pymt_txn_status_desc,row_number() over (partition by org_id,ord_id order by src_load_ts desc) rnk
			from   ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_pymt_line_v )
		where rnk=1
),
dim_location as (
    select * from
       (select
            loc_snum,loc_num,row_number() over (partition by lpad(loc_snum, 5, 0) order by loc_sk desc,loc_seq_num desc) loc_rnk
        from sf_gold_prod_db.location_gold_prod.dim_location_v where UPPER(banner_geo)='NA')
        where loc_rnk=1
),
mao_ord_fulfillment_detail as (
    SELECT * FROM (
        SELECT
            fd.*,
            ROW_NUMBER() OVER (
                PARTITION BY fd.org_id, fd.ord_id, fd.ord_ln_id, fd.rel_id, fd.rel_ln_id
                ORDER BY fd.updated_ts DESC
            ) AS ful_det_rnk
        FROM ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_fulfillment_dtl_v fd
        WHERE fd.etl_updt_ts > '${lookback_date}'
    )
    WHERE ful_det_rnk = 1
),
fct_mao_ord_line as (
    select
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
        ord_line.org_id,
        ord_line.ord_id,
        ord_line.ord_ln_id,
        ord_line.item_id,
        ord_line.qty,
        ord_line.unit_price,
        ord_line.orig_unit_price,
        ord_line.ord_ln_total,
        ord_line.ord_ln_sub_total,
        ord_line.total_disc,
        ord_line.total_disc_on_item,
        ord_line.total_taxes,
        ord_line.ord_shipping_amt,
        ord_line.ord_shipping_tax_amt,
        ord_line.ord_sales_tax_amt,
        ord_line.total_charges,
        ord_line.cnlled_total_disc,
        ord_line.cnlled_ord_ln_total,
        ord_line.cnlled_ord_shipping_amt,
        ord_line.cnlled_ord_shipping_tax_amt,
        ord_line.cnlled_ord_ln_sub_total,
        ord_line.cnlled_total_taxes,
        ord_line.cnlled_total_charges,
        ord_line.cnlled_ord_sales_tax_amt,
        ord_line.max_fulflmnt_status_id,
        ord_line.max_fulflmnt_status_desc,
        ord_line.cnl_reason_id,
        ord_line.cnl_reason_desc,
        ord_line.product_type,
        ord_line.is_gift_card,
        ord_line.is_pre_sale,
        ord_line.is_base_shipping_charged,
        ord_line.is_even_exchg,
        ord_line.prnt_ord_id,
        ord_line.physical_org_id,
        ord_line.ship_from_loc_id,
        ord_line.ship_to_addr_first_name,
        ord_line.ship_to_addr_last_name,
        ord_line.ship_to_addr_email,
        ord_line.ship_to_addr_phone,
        ord_line.ship_to_addr_addr1,
        ord_line.ship_to_addr_addr2,
        ord_line.ship_to_addr_city,
        ord_line.ship_to_addr_country,
        ord_line.ship_to_addr_state,
        ord_line.ship_to_addr_postal_cd,
        ord_line.cart_shpmnt_method,
        ord_line.shpmnt_method,
        ord_line.itm_size,
        ord_line.ord_note,
        ord_line.dlvry_method_id,
        ord_line.created_ts,
        ord_line.created_by,
        ord_line.updated_ts,
        ord_line.updated_by,
        ord_line.etl_updt_ts,
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
        first_value(ord_line.gift_card_value) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_gift_card_value,
        row_number() over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.updated_ts desc) as ord_ln_rnk
    from ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_line_hist_v ord_line
    JOIN  ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v oh
        ON oh.org_id = ord_line.org_id AND oh.ord_id = ord_line.ord_id
    WHERE oh.doc_type_id = 'CustomerOrder'
        AND NOT (ord_line.max_fulflmnt_status_id IS NULL OR ord_line.max_fulflmnt_status_id > 9000)
        AND (ord_line.prnt_ord_id IS NULL OR ord_line.is_even_exchg = 1)
        AND ord_line.max_fulflmnt_status_id NOT IN ('8000', '8500')
),
fct_mao_ful_line as (
    select ol.doc_type_id,ol.ccy_cd,ol.ord_locale,ol.cust_id,ol.flx_id,ol.pymt_status_id,ol.ord_total,ol.confirmed_ts,ol.captured_ts,ol.created_ts,
		ol.is_base_shipping_charged,ol.ord_note,ol.total_disc_on_item,ol.total_disc,ol.orig_unit_price,ol.ord_shipping_amt,ol.ord_shipping_tax_amt,ol.ord_ln_sub_total,ol.total_taxes,
    ol.cnlled_total_disc,ol.cnlled_ord_ln_total,ol.cnlled_ord_shipping_amt,ol.cnlled_ord_shipping_tax_amt,ol.cnlled_ord_ln_sub_total,ol.cnlled_total_taxes,
		ol.physical_org_id,ol.ord_ln_total,ol.unit_price,ol.product_type,ol.qty,ol.is_gift_card,ol.is_pre_sale,
		ol.ship_to_addr_first_name,ol.ship_to_addr_last_name,ol.ship_to_addr_email,ol.ship_to_addr_phone,ol.ship_to_addr_addr1,
		ol.ship_to_addr_addr2,ol.ship_to_addr_city,ol.ship_to_addr_country,ol.ship_to_addr_state,ol.ship_to_addr_postal_cd,
		ol.cart_shpmnt_method,ol.shpmnt_method,ol.itm_size,ol.total_charges,ol.cnlled_total_charges,ol.ord_sales_tax_amt,ol.cnlled_ord_sales_tax_amt,
		ol.fv_orig_unit_price,ol.fv_unit_price,ol.fv_cnlled_total_disc,ol.fv_total_disc,ol.fv_cnlled_ord_ln_total,ol.fv_ord_ln_total,
		ol.fv_cnlled_total_charges,ol.fv_total_charges,ol.fv_cnlled_total_taxes,ol.fv_total_taxes,
		ol.fv_cnlled_ord_shipping_amt,ol.fv_ord_shipping_amt,
		ol.fv_cnlled_ord_shipping_tax_amt,ol.fv_ord_shipping_tax_amt,
		ol.fv_cnlled_ord_ln_sub_total,ol.fv_ord_ln_sub_total,
		ol.fv_cnlled_ord_sales_tax_amt,ol.fv_ord_sales_tax_amt,
		fl.* except(created_ts)
    from ${dom_gold_db}.${dom_gold_schema}.fct_mao_fulfillment_line_hist_v fl
		  join ${dom_gold_db}.${dom_gold_schema}.fct_mao_fulfillment_hdr_v fh on (fh.org_id = fl.org_id and fh.fulflmnt_id= fl.fulflmnt_id)
		  join fct_mao_ord_line ol on (fl.ord_id=ol.ord_id and fl.ord_ln_id = ol.ord_ln_id and ol.ord_ln_rnk=1)
	where fl.etl_updt_ts > '${lookback_date}'
),
store_fulflmnts as (
	select distinct rel_id,rel_ln_id from fct_mao_ful_line
),
mao_rejections AS (
    SELECT
        fd.org_id,
        fd.ord_id,
        fd.ord_ln_id,
        fd.rel_id,
        fd.rel_ln_id
    FROM mao_ord_fulfillment_detail fd
    JOIN fct_mao_ord_line ol
        ON fd.org_id = ol.org_id AND fd.ord_id = ol.ord_id AND fd.ord_ln_id = ol.ord_ln_id
    WHERE fd.is_rejected = 1 AND fd.status_id <= '3500' AND ol.max_fulflmnt_status_id < '3500'
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
        CAST(NULL AS INT) AS picked_qty,
        CAST(NULL AS INT) AS pked_qty,
        CAST(NULL AS INT) AS shipped_qty,
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
        fd.created_ts AS rel_created_ts,
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
        AND NOT EXISTS (SELECT 'x' FROM mao_rejections r WHERE fd.org_id = r.org_id AND fd.ord_id = r.ord_id AND fd.ord_ln_id = r.ord_ln_id AND fd.rel_id = r.rel_id AND fd.rel_ln_id = r.rel_ln_id)
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
    WHERE NOT EXISTS (SELECT 'x' FROM mao_rejections r WHERE fd.org_id = r.org_id AND fd.ord_id = r.ord_id AND fd.ord_ln_id = r.ord_ln_id AND fd.rel_id = r.rel_id AND fd.rel_ln_id = r.rel_ln_id)
),
consignments as (
  select c.*,
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
consignments_main as (
    select * from (
      select con.*,row_number() over (partition by con.org_id,con.ord_id,con.consignment_id order by con.updated_ts desc,con.consignment_status) as consignment_rnk
    from consignments con
  )
  where consignment_rnk=1
),
consignment_entries as (
  select
      ent.org_id,ent.ord_id,ent.consignment_id,ent.consignment_status,
      cast(collect_list(
        struct(
            cast(case when ent.is_base_shipping_charged=1 then 'true' else 'false' end as string) as baseShippingCharged,
            cast(cnl_reason.oms_cancel_code as string) as cancelCode,
            cast(ent.cnl_reason_desc as string) as cancelReason,
            cast(ABS(coalesce(ent.cnlled_qty,0)) as string) AS cancelledQty,
            cast(ent.carrier_cd as string) as carrier,
            cast(ent.pkg_id as string) as cartonNum,
            cast(ent.inv_id as string) as cegrRefId,
            cast(ent.fulflmnt_dtl_id as string) as entryId,
            cast(ent.entryStatus as string) AS entryStatus,
            cast(ABS(coalesce(ent.fulfld_qty,0)) as string) as fulfilledQty,
            cast(ent.ord_note as string) as metadata,
            cast(DATE_FORMAT(
                TO_TIMESTAMP(ent.updated_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
                )as string) as modifiedDate,
            struct(
                cast(ent.ccy_cd as string) as currencyIso,
              cast(ABS(coalesce(ent.fv_cnlled_total_disc,0) + coalesce(ent.fv_total_disc,0)) as string) as discountAmount,
              CAST((ABS(coalesce(ent.fv_cnlled_ord_ln_sub_total,0) + coalesce(ent.fv_ord_ln_sub_total,0)) + 
				ABS(coalesce(ent.fv_cnlled_ord_shipping_amt,0) + coalesce(ent.fv_ord_shipping_amt,0)) + 
				ABS(coalesce(ent.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ent.fv_ord_shipping_tax_amt,0)) + 
				ABS(coalesce(ent.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ent.fv_ord_sales_tax_amt,0))) - 
				ABS(coalesce(ent.fv_cnlled_total_disc,0) + coalesce(ent.fv_total_disc,0)) AS STRING) as discountedTotalAmount,
              cast(null as string) giftBoxAmount,
              cast(null as string) giftBoxTaxAmount,
              cast(ABS(coalesce(ent.fv_orig_unit_price, ent.fv_unit_price,0)) as string) as originalRetailPrice,
              cast(null as string) priceOverrideReason,
              CAST(ABS(coalesce(ent.fv_cnlled_ord_shipping_amt,0) + coalesce(ent.fv_ord_shipping_amt,0)) AS STRING) as shippingAmount,
              cast(ABS(coalesce(ent.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ent.fv_ord_shipping_tax_amt,0)) as string) as shippingTaxAmount,
              cast(ABS(coalesce(ent.fv_cnlled_ord_ln_sub_total,0) + coalesce(ent.fv_ord_ln_sub_total,0)) as string) as subTotalAmount,
              cast(ABS(coalesce(ent.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ent.fv_ord_sales_tax_amt,0)) as string) as taxAmount,
              CAST(ABS(coalesce(ent.fv_cnlled_ord_ln_sub_total,0) + coalesce(ent.fv_ord_ln_sub_total,0)) + 
				ABS(coalesce(ent.fv_cnlled_ord_shipping_amt,0) + coalesce(ent.fv_ord_shipping_amt,0)) + 
				ABS(coalesce(ent.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ent.fv_ord_shipping_tax_amt,0)) + 
				ABS(coalesce(ent.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ent.fv_ord_sales_tax_amt,0)) AS STRING) as totalAmount,
              cast(ABS(coalesce(ent.fv_unit_price,0)) as string) as unitPrice
            ) as pricing,
            cast( trim(CASE
                WHEN ent.item_id = 'ECARD20' THEN '2138264'
                WHEN ent.item_id = 'ECARD21' THEN '2138265'
                WHEN ent.item_id = 'ECARD22' THEN '2138266'
                WHEN ent.item_id = 'ECARD45' THEN '20'
                WHEN ent.item_id = 'ECARD77' THEN '2000003'
                WHEN ent.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
                WHEN ent.org_id in ('FL-US','KFL-US','CH-US') THEN pm.internal_product_number
                else ent.item_id
            END) as string) as productCode,
            CAST(ent.product_type AS STRING) as productType,
            cast(null as string) as requestSystem,
            cast(ent.created_by as string) as requestedBy,
            cast(ABS(coalesce(ent.qty,0)) as string) as requestedQty,
            cast(ent.fulflmnt_ln_id as string) as requestingSystemLineNo,
            cast(ent.shpd_dt as string) as shippedDate,
            CAST(case when ent.is_gift_card=1 then ent.itm_size else pm.legacy_size_desc end AS STRING) as size,
            cast(case
              when ent.is_gift_card=1 then ent.item_id
              WHEN ent.CHANNEL !='XSTORE' AND  ent.org_id in ('FL-US','KFL-US','CH-US') then pm.online_us_sku
              WHEN ent.CHANNEL !='XSTORE' AND  ent.org_id in ('FL-CA','CH-CA') then pm.online_ca_sku
              WHEN ent.CHANNEL ='XSTORE' THEN PM_DIV.legacy_sku_size
              else itm.COLOR
            end as string) as sku,
            cast(null  as string) as storeNumber,
            cast(null as string) as ticketNumber,
            cast(ent.tracking_num as string) as trackingId,
			      case when ent.tracking_num is not null then cast(concat('https://',
			        case trim(ent.org_id) when 'FL-US' then 'footlocker' when 'FL-CA' then 'footlocker' when 'KFL-US' then 'kidsfootlocker' when 'CH-CA' then 'champs' when 'CH-US' then 'champs' end,
			        '.narvar.com/',
			        case trim(ent.org_id) when 'FL-US' then 'footlocker' when 'FL-CA' then 'footlocker' when 'KFL-US' then 'kidsfootlocker' when 'CH-CA' then 'champs' when 'CH-US' then 'champs' end,
			        '/tracking/',
			        ent.carrier_cd,
			        '/?order_number=',
			        ent.ord_id,
			        '-DOLBL48HRW',
			        '&tracking_numbers=',
			        ent.tracking_num,
			        '&locale=',
			        ent.ord_locale,
			        'ℴdate=',
			        ent.created_ts,
			        '&ozip=',
			        loc.loc_addr_postal_cd,
			        '&origin_country=',
			        'US',
			        '&dzip=',
			        substring(ent.ship_to_addr_postal_cd, 1, 5),
			        '&destination_country=',
			        ent.ship_to_addr_country,
			        '&product_category=',
			        'WHSE',
			        '&service=HD%22') as string) end as trackingUrl,
            case
                when ent.updated_by is not null then 'true'
                else 'false'
            end as updated
        )
    ) as ARRAY<STRUCT<
        baseShippingCharged: STRING,
        cancelCode: STRING,
        cancelReason: STRING,
        cancelledQty: STRING,
        carrier: STRING,
        cartonNum: STRING,
        cegrRefId: STRING,
        entryId: STRING,
        entryStatus: STRING,
        fulfilledQty: STRING,
        metadata: STRING,
        modifiedDate: STRING,
        pricing: STRUCT<
            currencyIso: STRING,
            discountAmount: STRING,
            discountedTotalAmount: STRING,
            giftBoxAmount: STRING,
            giftBoxTaxAmount: STRING,
            originalRetailPrice: STRING,
            priceOverrideReason: STRING,
            shippingAmount: STRING,
            shippingTaxAmount: STRING,
            subTotalAmount: STRING,
            taxAmount: STRING,
            totalAmount: STRING,
            unitPrice: STRING
        >,
        productCode: STRING,
        productType: STRING,
        requestSystem: STRING,
        requestedBy: STRING,
        requestedQty: STRING,
        requestingSystemLineNo: STRING,
        shippedDate: STRING,
        size: STRING,
        sku: STRING,
        storeNumber: STRING,
        ticketNumber: STRING,
        trackingId: STRING,
        trackingUrl: STRING,
        updated: STRING
    >>) as consignmentEntries
  from (
    select stg.*, row_number() over (partition by stg.org_id,stg.ord_id,stg.ord_ln_id,stg.consignment_id,stg.consignment_status order by stg.updated_ts desc) as entry_rnk
    from consignments stg
        join consignments_main main on (stg.org_id=main.org_id and stg.ord_id=main.ord_id and stg.consignment_id=main.consignment_id  and stg.consignment_status=main.consignment_status and stg.updated_ts<=main.updated_ts)
    ) ent
    left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v loc on (coalesce(ent.ship_from_loc_id,ent.physical_org_id) = loc.loc_id)
    left join dim_location dim_loc on (lpad(dim_loc.loc_snum,5,0) = lpad(coalesce(ent.ship_from_loc_id,ent.physical_org_id),5,0))
    left join product_master pm ON ((ent.is_gift_card!=1 and trim(ent.item_id) = trim(pm.global_size_id) AND (CASE WHEN ORG_ID IN ('FL-CA','CH-CA') THEN '98' ELSE '81' END)=pm.banner_id))
    left join product_master_div pm_div ON (ent.is_gift_card!=1 and trim(ent.item_id) = trim(pm_div.global_size_id) AND ent.ORG_ID = pm_div.org_desc)
    left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_item_v itm on (trim(ent.ITEM_ID) = trim(itm.ITEM_ID))
    left join ${dom_gold_db}.${dom_gold_schema}.lkp_cancel_code_reason_v  cnl_reason on (ent.cnl_reason_id =cnl_reason.cancel_reason_id)
  where ent.entry_rnk=1
  group by all
),
order_line_tax as (
    select org_id con_org_id,ord_id con_ord_id,ord_ln_id as con_ord_ln_id,
    	ABS(SUM(COALESCE(fv.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(fv.fv_ord_shipping_tax_amt, 0))) AS shippingtaxamount,
        ABS(SUM(COALESCE(fv.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(fv.fv_ord_sales_tax_amt, 0))) AS taxamount
    from 
		fct_mao_ord_line fv
    WHERE ord_ln_rnk = 1
    group by all
),
-- payment CTE
payment AS (
    SELECT 
        pymt.* EXCEPT(pymt_txn_amt),
        CASE 
            WHEN pymt.pymt_txn_cnt = 1 THEN COALESCE(pymt.inv_ln_total, pymt.pymt_txn_amt, 0)
            ELSE COALESCE(pymt.pymt_txn_amt, 0)
        END AS pymt_txn_amt
    FROM (
        SELECT
            p.*,
            ROW_NUMBER() OVER (
                PARTITION BY p.org_id, p.ord_id, p.ord_ln_id, p.pymt_txn_id, p.pymt_txn_dtl_id, p.inv_id, p.inv_ln_id
                ORDER BY p.inv_ln_updated_ts DESC, p.src_load_ts DESC
            ) AS pymt_rnk,
            cnt.pymt_txn_cnt
        FROM ${dom_gold_db}.${dom_gold_schema}.FCT_MAO_ORD_PYMT_LINE_V p
        LEFT JOIN (
            SELECT
                org_id, ord_id, ord_ln_id, inv_id, inv_ln_id,
                COUNT(DISTINCT pymt_txn_id || '~' || COALESCE(pymt_txn_dtl_id, '')) AS pymt_txn_cnt
            FROM ${dom_gold_db}.${dom_gold_schema}.FCT_MAO_ORD_PYMT_LINE_V
            WHERE LOWER(pymt_txn_type) NOT IN ('authorization', 'authorization reversal','refund')
            GROUP BY org_id, ord_id, ord_ln_id, inv_id, inv_ln_id
        ) cnt
            ON  p.org_id    = cnt.org_id
            AND p.ord_id    = cnt.ord_id
            AND p.ord_ln_id = cnt.ord_ln_id
            AND p.inv_id    = cnt.inv_id
            AND p.inv_ln_id = cnt.inv_ln_id
        WHERE LOWER(p.pymt_txn_type) NOT IN ('authorization', 'authorization reversal','refund')
    ) pymt
    WHERE pymt.pymt_rnk = 1
),
payment_grouped AS (
   SELECT
    org_id,
    ord_id,
    ord_ln_id,
    COLLECT_LIST(
      STRUCT(
        CAST(ABS(COALESCE(pymt_txn_amt,0)) AS STRING) AS amount,
        CAST(auth_cd AS STRING) AS authCode,
        CAST(txn_card_last4 AS STRING) AS cardLast4,
        CAST(inv_id AS STRING) AS cegrRefId,
        CAST(PYMT_TXN_ID AS STRING) AS paymentTransactionId,
        CAST(NULL AS STRING) AS paymentTransactionSubType,
        CAST(PYMT_TXN_TYPE AS STRING) AS paymentTransactionType,
        CAST(CASE WHEN pymt_type = 'Gift Card' THEN 'GIFTCARD' WHEN pymt_type = 'Credit Card' THEN 'CREDITCARD' ELSE UPPER(pymt_type) END AS STRING) AS paymentType,
        CAST(PYMT_CARD_TYPE AS STRING) AS creditCardType,
        CAST(created_ts AS STRING) AS date,
        --CAST(ccy_code AS STRING) as currencyIso,
        CAST(shippingtaxamount AS STRING) AS shippingTaxAmount,
        CAST(taxamount AS STRING) AS taxAmount,
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
          CAST(ABS(COALESCE(pymt_txn_req_amt,0)) AS STRING) AS authAmount,
          CAST(auth_cd AS STRING) AS authCode,
          CAST(NULL AS STRING) AS errorMessage,
          CAST(NULL AS STRING) AS id,
          CAST(ord_id AS STRING) AS originalOrderNumber,
          CAST(pymt_type AS STRING) AS paymentType,
          CAST(NULL AS STRING) AS preSettled,
          CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt, created_ts) AS STRING) AS transactionDate,
          CAST(pymt_txn_id AS STRING) AS transactionId
        ) AS authorization,
        -- creditCard struct: only populate if pymt_type = 'Credit Card', else null
        CASE WHEN pymt_type = 'Credit Card' THEN
          STRUCT(
            STRUCT(
              CAST(ABS(COALESCE(pymt_txn_req_amt,0)) AS STRING) AS authAmount,
              CAST(auth_cd AS STRING) AS authCode,
              CAST(pymt_txn_status_desc AS STRING) AS authResponse,
              CAST(COALESCE(pymt_txn_dt, pymt_txn_req_dt, created_ts) AS STRING) AS authTime,
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
            CAST(ABS(COALESCE(pymt_txn_amt,0)) AS STRING) AS amount,
            CAST(auth_cd AS STRING) AS authCode,
            CAST(attrib_card_last4 AS STRING) AS giftCardNumber,
            CAST(ord_id AS STRING) AS originalOrderNumber,
            CAST(NULL AS STRING) AS preSettled,
            CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt, created_ts) AS STRING) AS transactionDate,
            CAST(pymt_txn_id AS STRING) AS transactionId
          )
        ELSE NULL END AS giftCard,
        CASE WHEN pymt_type = 'PayPal' THEN
          STRUCT(
            CAST(ABS(COALESCE(pymt_txn_amt,0)) AS STRING) AS amount,
            STRUCT(
              CAST(ABS(COALESCE(pymt_txn_req_amt,0)) AS STRING) AS authAmount,
              CAST(auth_cd AS STRING) AS authCode,
              CAST(pymt_txn_status_desc AS STRING) AS authResponse,
              CAST(COALESCE(pymt_txn_dt, pymt_txn_req_dt, created_ts) AS STRING) AS authTime,
              CAST(attrib_avs_code AS STRING) AS avsCode,
              CAST(attrib_cvv_response AS STRING) AS cvvResponse,
              CAST(ord_id AS STRING) AS originalOrderNumber,
              CAST(NULL AS STRING) AS preSettled,
              CAST(transaction_ref_id AS STRING) AS referenceNumber,
              CAST(pymt_txn_id AS STRING) AS transactionId
            ) AS authInfo,
            CAST(addr_email AS STRING) AS paypalEmailId,
            CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt,created_ts) AS STRING) AS transactionDate,
            CAST(pymt_txn_id AS STRING) AS transactionId
        )
        ELSE NULL END AS paypal
      )
    ) AS payments_info,
    ARRAY_AGG(pymt_txn_type) as pymt_txn
  FROM payment pymt left join order_line_tax con on  (pymt.org_id=con.con_org_id and pymt.ord_id=con.con_ord_id and pymt.ord_ln_id=con.con_ord_ln_id)
  WHERE pymt_rnk = 1
  GROUP BY all
)
select
    con.ord_id as order_id,
    cast(case trim(con.org_id)
        when 'FL-US' then '21'
        when 'FL-CA' then '45'
        when 'KFL-US' then '22'
        when 'CH-CA' then '77'
        when 'CH-US' then '20'
    end as string) as company_number,
    cast(DATE_FORMAT(
            TO_TIMESTAMP(con.created_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
        )as string) as orderDateTime,
    cast(con.consignment_id as string) as consignment_id,
    case upper(trim(con.dlvry_method_id))
    when 'EMAIL' then 'EGC'
    else  'OBF' end     as  consignment_fullfillment_system,
    case upper(trim(con.dlvry_method_id))
      when 'PICKUPATSTORE' then 'PICK'
      when 'SHIPTOADDRESS' then 'SHIP'
      when 'SHIPTOSTORE' then 'SHIP'
      when 'SHIPTORETURNCENTER' then 'SHIP'
      when 'EMAIL' then 'ELECTRONIC'
      when 'STORESALE' then 'XSTORE'
      when 'STORERETURN'then 'XSTORE'
    else upper(trim(con.dlvry_method_id))
    end as consignment_fullfillment_type,
    cast(null as string) as consignment_fulfillmentSystemRequestId,
    cast(null as string) as consignment_fulfillmentSystemResponseId,
    cast(con.inv_id as string) as invoice_Num,
    ent.consignmentEntries,
	coalesce(dim_loc.loc_snum, con.ship_from_loc_id,con.physical_org_id) as consignment_storeNumber,   --Brining the Location from MAO if the DIM_LOCATION is null.
    con.consignment_status AS consignment_status,
    pymt_line.Payments_Info as Payments_Info,
    cast(DATE_FORMAT(
                TO_TIMESTAMP(con.updated_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
          )as string) as postedAt,
    con.updated_ts as load_time_kafka,
    con.etl_updt_ts as load_time_adls,
    date(con.created_ts) as orderdate,
    cast(con.gift_card_no as string)  as gift_card_number,
    cast(ABS(con.gift_card_value) as Decimal(26, 2)) as gift_card_amount,
    cast(case con.is_pre_sale
        when 1 then 'true'
        when 0 then 'false'
    end as string) as presell,
    pymt_line.pymt_txn as payment_transaction_subtype,
    con.updated_ts as update_date,
    "MAO" as ref_source
  from
    consignments_main con
    join consignment_entries ent on (con.org_id=ent.org_id and con.ord_id=ent.ord_id and con.consignment_id=ent.consignment_id and con.consignment_status=ent.consignment_status)
	  left join payment_grouped pymt_line ON con.org_id = pymt_line.org_id AND con.ord_id = pymt_line.ord_id AND con.ord_ln_id = pymt_line.ord_ln_id
    left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v loc on (coalesce(con.ship_from_loc_id,con.physical_org_id) = loc.loc_id)
    left join dim_location dim_loc on (lpad(dim_loc.loc_snum,5,0) = lpad(coalesce(con.ship_from_loc_id,con.physical_org_id),5,0))
  where
    con.doc_type_id = 'CustomerOrder'
);


create or replace temp view consignments_landing_stg as
(
with
pymt_txn_status as (
		select *
		from
			(select org_id,ord_id,pymt_txn_status_desc,row_number() over (partition by org_id,ord_id order by src_load_ts desc) rnk
			from   ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_pymt_line_v )
		where rnk=1
),
dim_location as (
    select * from
       (select
            loc_snum,loc_num,row_number() over (partition by lpad(loc_snum, 5, 0) order by loc_sk desc,loc_seq_num desc) loc_rnk
        from sf_gold_prod_db.location_gold_prod.dim_location_v where UPPER(banner_geo)='NA')
        where loc_rnk=1
),
mao_ord_fulfillment_detail as (
    SELECT * FROM (
        SELECT
            fd.*,
            ROW_NUMBER() OVER (
                PARTITION BY fd.org_id, fd.ord_id, fd.ord_ln_id, fd.rel_id, fd.rel_ln_id
                ORDER BY fd.updated_ts DESC
            ) AS ful_det_rnk
        FROM ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_fulfillment_dtl_v fd
        WHERE fd.etl_updt_ts > '${lookback_date}'
    )
    WHERE ful_det_rnk = 1
),
fct_mao_ord_line as (
    select
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
        ord_line.org_id,
        ord_line.ord_id,
        ord_line.ord_ln_id,
        ord_line.item_id,
        ord_line.qty,
        ord_line.unit_price,
        ord_line.orig_unit_price,
        ord_line.ord_ln_total,
        ord_line.ord_ln_sub_total,
        ord_line.total_disc,
        ord_line.total_disc_on_item,
        ord_line.total_taxes,
        ord_line.ord_shipping_amt,
        ord_line.ord_shipping_tax_amt,
        ord_line.ord_sales_tax_amt,
        ord_line.total_charges,
        ord_line.cnlled_total_disc,
        ord_line.cnlled_ord_ln_total,
        ord_line.cnlled_ord_shipping_amt,
        ord_line.cnlled_ord_shipping_tax_amt,
        ord_line.cnlled_ord_ln_sub_total,
        ord_line.cnlled_total_taxes,
        ord_line.cnlled_total_charges,
        ord_line.cnlled_ord_sales_tax_amt,
        ord_line.max_fulflmnt_status_id,
        ord_line.max_fulflmnt_status_desc,
        ord_line.cnl_reason_id,
        ord_line.cnl_reason_desc,
        ord_line.product_type,
        ord_line.is_gift_card,
        ord_line.is_pre_sale,
        ord_line.is_base_shipping_charged,
        ord_line.is_even_exchg,
        ord_line.prnt_ord_id,
        ord_line.physical_org_id,
        ord_line.ship_from_loc_id,
        ord_line.ship_to_addr_first_name,
        ord_line.ship_to_addr_last_name,
        ord_line.ship_to_addr_email,
        ord_line.ship_to_addr_phone,
        ord_line.ship_to_addr_addr1,
        ord_line.ship_to_addr_addr2,
        ord_line.ship_to_addr_city,
        ord_line.ship_to_addr_country,
        ord_line.ship_to_addr_state,
        ord_line.ship_to_addr_postal_cd,
        ord_line.cart_shpmnt_method,
        ord_line.shpmnt_method,
        ord_line.itm_size,
        ord_line.ord_note,
        ord_line.dlvry_method_id,
        ord_line.created_ts,
        ord_line.created_by,
        ord_line.updated_ts,
        ord_line.updated_by,
        ord_line.etl_updt_ts,
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
        first_value(ord_line.gift_card_value) over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.max_fulflmnt_status_id asc, ord_line.updated_ts asc) as fv_gift_card_value,
        row_number() over (partition by ord_line.org_id, ord_line.ord_id, ord_line.ord_ln_id order by ord_line.updated_ts desc) as ord_ln_rnk
    from ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_line_hist_v ord_line
    JOIN  ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v oh
        ON oh.org_id = ord_line.org_id AND oh.ord_id = ord_line.ord_id
    WHERE oh.doc_type_id = 'CustomerOrder'
        AND NOT (ord_line.max_fulflmnt_status_id IS NULL OR ord_line.max_fulflmnt_status_id > 9000)
        AND (ord_line.prnt_ord_id IS NULL OR ord_line.is_even_exchg = 1)
        AND ord_line.max_fulflmnt_status_id NOT IN ('8000', '8500')
),
fct_mao_ful_line as (
    select ol.doc_type_id,ol.ccy_cd,ol.ord_locale,ol.cust_id,ol.flx_id,ol.pymt_status_id,ol.ord_total,ol.confirmed_ts,ol.captured_ts,ol.created_ts,
		ol.is_base_shipping_charged,ol.ord_note,ol.total_disc_on_item,ol.total_disc,ol.orig_unit_price,ol.ord_shipping_amt,ol.ord_shipping_tax_amt,ol.ord_ln_sub_total,ol.total_taxes,
    ol.cnlled_total_disc,ol.cnlled_ord_ln_total,ol.cnlled_ord_shipping_amt,ol.cnlled_ord_shipping_tax_amt,ol.cnlled_ord_ln_sub_total,ol.cnlled_total_taxes,
		ol.physical_org_id,ol.ord_ln_total,ol.unit_price,ol.product_type,ol.qty,ol.is_gift_card,ol.is_pre_sale,
		ol.ship_to_addr_first_name,ol.ship_to_addr_last_name,ol.ship_to_addr_email,ol.ship_to_addr_phone,ol.ship_to_addr_addr1,
		ol.ship_to_addr_addr2,ol.ship_to_addr_city,ol.ship_to_addr_country,ol.ship_to_addr_state,ol.ship_to_addr_postal_cd,
		ol.cart_shpmnt_method,ol.shpmnt_method,ol.itm_size,ol.total_charges,ol.cnlled_total_charges,ol.ord_sales_tax_amt,ol.cnlled_ord_sales_tax_amt,
		ol.fv_orig_unit_price,ol.fv_unit_price,ol.fv_cnlled_total_disc,ol.fv_total_disc,ol.fv_cnlled_ord_ln_total,ol.fv_ord_ln_total,
		ol.fv_cnlled_total_charges,ol.fv_total_charges,ol.fv_cnlled_total_taxes,ol.fv_total_taxes,
		ol.fv_cnlled_ord_shipping_amt,ol.fv_ord_shipping_amt,
		ol.fv_cnlled_ord_shipping_tax_amt,ol.fv_ord_shipping_tax_amt,
		ol.fv_cnlled_ord_ln_sub_total,ol.fv_ord_ln_sub_total,
		ol.fv_cnlled_ord_sales_tax_amt,ol.fv_ord_sales_tax_amt,
		fl.* except(created_ts)
    from ${dom_gold_db}.${dom_gold_schema}.fct_mao_fulfillment_line_hist_v fl
		  join ${dom_gold_db}.${dom_gold_schema}.fct_mao_fulfillment_hdr_v fh on (fh.org_id = fl.org_id and fh.fulflmnt_id= fl.fulflmnt_id)
		  join fct_mao_ord_line ol on (fl.ord_id=ol.ord_id and fl.ord_ln_id = ol.ord_ln_id and ol.ord_ln_rnk=1)
	where fl.etl_updt_ts > '${lookback_date}'
),
store_fulflmnts as (
	select distinct rel_id,rel_ln_id from fct_mao_ful_line
),
mao_rejections AS (
    SELECT
        fd.org_id,
        fd.ord_id,
        fd.ord_ln_id,
        fd.rel_id,
        fd.rel_ln_id
    FROM mao_ord_fulfillment_detail fd
    JOIN fct_mao_ord_line ol
        ON fd.org_id = ol.org_id AND fd.ord_id = ol.ord_id AND fd.ord_ln_id = ol.ord_ln_id
    WHERE fd.is_rejected = 1 AND fd.status_id <= '3500' AND ol.max_fulflmnt_status_id < '3500'
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
        CAST(NULL AS INT) AS picked_qty,
        CAST(NULL AS INT) AS pked_qty,
        CAST(NULL AS INT) AS shipped_qty,
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
        fd.created_ts AS rel_created_ts,
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
        AND NOT EXISTS (SELECT 'x' FROM mao_rejections r WHERE fd.org_id = r.org_id AND fd.ord_id = r.ord_id AND fd.ord_ln_id = r.ord_ln_id AND fd.rel_id = r.rel_id AND fd.rel_ln_id = r.rel_ln_id)
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
    WHERE NOT EXISTS (SELECT 'x' FROM mao_rejections r WHERE fd.org_id = r.org_id AND fd.ord_id = r.ord_id AND fd.ord_ln_id = r.ord_ln_id AND fd.rel_id = r.rel_id AND fd.rel_ln_id = r.rel_ln_id)
),
consignments as (
  select c.*,
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
consignments_main as (
    select * from (
      select con.*,row_number() over (partition by con.org_id,con.ord_id,con.consignment_id,con.consignment_status order by con.updated_ts desc,con.consignment_status) as consignment_rnk
    from consignments con
  )
  where consignment_rnk=1
),
consignment_entries as (
  select
      ent.org_id,ent.ord_id,ent.consignment_id,ent.consignment_status,
      cast(collect_list(
        struct(
            cast(case when ent.is_base_shipping_charged=1 then 'true' else 'false' end as string) as baseShippingCharged,
            cast(cnl_reason.oms_cancel_code as string) as cancelCode,
            cast(ent.cnl_reason_desc as string) as cancelReason,
            cast(ABS(coalesce(ent.cnlled_qty,0)) as string) AS cancelledQty,
            cast(ent.carrier_cd as string) as carrier,
            cast(ent.pkg_id as string) as cartonNum,
            cast(ent.inv_id as string) as cegrRefId,
            cast(ent.fulflmnt_dtl_id as string) as entryId,
            cast(ent.entryStatus as string) AS entryStatus,
            cast(ABS(coalesce(ent.fulfld_qty,0)) as string) as fulfilledQty,
            cast(ent.ord_note as string) as metadata,
            cast(DATE_FORMAT(
                TO_TIMESTAMP(ent.updated_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
                )as string) as modifiedDate,
            struct(
              cast(ent.ccy_cd as string) as currencyIso,
              cast(ABS(coalesce(ent.fv_cnlled_total_disc,0) + coalesce(ent.fv_total_disc,0)) as string) as discountAmount,
              CAST((ABS(coalesce(ent.fv_cnlled_ord_ln_sub_total,0) + coalesce(ent.fv_ord_ln_sub_total,0)) + ABS(coalesce(ent.fv_cnlled_ord_shipping_amt,0) + coalesce(ent.fv_ord_shipping_amt,0)) + ABS(coalesce(ent.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ent.fv_ord_shipping_tax_amt,0)) + ABS(coalesce(ent.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ent.fv_ord_sales_tax_amt,0))) - ABS(coalesce(ent.fv_cnlled_total_disc,0) + coalesce(ent.fv_total_disc,0)) AS STRING) as discountedTotalAmount,
              cast(null as string) giftBoxAmount,
              cast(null as string) giftBoxTaxAmount,
              cast(ABS(coalesce(ent.fv_orig_unit_price, ent.fv_unit_price,0)) as string) as originalRetailPrice,
              cast(null as string) priceOverrideReason,
              CAST(ABS(coalesce(ent.fv_cnlled_ord_shipping_amt,0) + coalesce(ent.fv_ord_shipping_amt,0)) AS STRING) as shippingAmount,
              cast(ABS(coalesce(ent.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ent.fv_ord_shipping_tax_amt,0)) as string) as shippingTaxAmount,
              cast(ABS(coalesce(ent.fv_cnlled_ord_ln_sub_total,0) + coalesce(ent.fv_ord_ln_sub_total,0)) as string) as subTotalAmount,
              cast(ABS(coalesce(ent.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ent.fv_ord_sales_tax_amt,0)) as string) as taxAmount,
              CAST(ABS(coalesce(ent.fv_cnlled_ord_ln_sub_total,0) + coalesce(ent.fv_ord_ln_sub_total,0)) + ABS(coalesce(ent.fv_cnlled_ord_shipping_amt,0) + coalesce(ent.fv_ord_shipping_amt,0)) + ABS(coalesce(ent.fv_cnlled_ord_shipping_tax_amt,0) + coalesce(ent.fv_ord_shipping_tax_amt,0)) + ABS(coalesce(ent.fv_cnlled_ord_sales_tax_amt,0) + coalesce(ent.fv_ord_sales_tax_amt,0)) AS STRING) as totalAmount,
              cast(ABS(coalesce(ent.fv_unit_price,0)) as string) as unitPrice
            ) as pricing,
            cast( trim(CASE
                WHEN ent.item_id = 'ECARD20' THEN '2138264'
                WHEN ent.item_id = 'ECARD21' THEN '2138265'
                WHEN ent.item_id = 'ECARD22' THEN '2138266'
                WHEN ent.item_id = 'ECARD45' THEN '20'
                WHEN ent.item_id = 'ECARD77' THEN '2000003'
                WHEN ent.org_id IN ('FL-CA', 'CH-CA') THEN pm.internal_product_number_flca
                WHEN ent.org_id in ('FL-US','KFL-US','CH-US') THEN pm.internal_product_number
                else ent.item_id
            END) as string) as productCode,
            CAST(ent.product_type AS STRING) as productType,
            cast(null as string) as requestSystem,
            cast(ent.created_by as string) as requestedBy,
            cast(ABS(coalesce(ent.qty,0)) as string) as requestedQty,
            cast(ent.fulflmnt_ln_id as string) as requestingSystemLineNo,
            cast(ent.shpd_dt as string) as shippedDate,
            CAST(case when ent.is_gift_card=1 then ent.itm_size else pm.legacy_size_desc end AS STRING) as size,
            cast(case
              when ent.is_gift_card=1 then ent.item_id
              WHEN ent.CHANNEL !='XSTORE' AND  ent.org_id in ('FL-US','KFL-US','CH-US') then pm.online_us_sku
              WHEN ent.CHANNEL !='XSTORE' AND  ent.org_id in ('FL-CA','CH-CA') then pm.online_ca_sku
              WHEN ent.CHANNEL ='XSTORE' THEN PM_DIV.legacy_sku_size
              else itm.COLOR
            end as string) as sku,
            cast(null  as string) as storeNumber,
            cast(null as string) as ticketNumber,
            cast(ent.tracking_num as string) as trackingId,
			      case when ent.tracking_num is not null then cast(concat('https://',
			        case trim(ent.org_id) when 'FL-US' then 'footlocker' when 'FL-CA' then 'footlocker' when 'KFL-US' then 'kidsfootlocker' when 'CH-CA' then 'champs' when 'CH-US' then 'champs' end,
			        '.narvar.com/',
			        case trim(ent.org_id) when 'FL-US' then 'footlocker' when 'FL-CA' then 'footlocker' when 'KFL-US' then 'kidsfootlocker' when 'CH-CA' then 'champs' when 'CH-US' then 'champs' end,
			        '/tracking/',
			        ent.carrier_cd,
			        '/?order_number=',
			        ent.ord_id,
			        '-DOLBL48HRW',
			        '&tracking_numbers=',
			        ent.tracking_num,
			        '&locale=',
			        ent.ord_locale,
			        'ℴdate=',
			        ent.created_ts,
			        '&ozip=',
			        loc.loc_addr_postal_cd,
			        '&origin_country=',
			        'US',
			        '&dzip=',
			        substring(ent.ship_to_addr_postal_cd, 1, 5),
			        '&destination_country=',
			        ent.ship_to_addr_country,
			        '&product_category=',
			        'WHSE',
			        '&service=HD%22') as string) end as trackingUrl,
            case
                when ent.updated_by is not null then 'true'
                else 'false'
            end as updated
        )
    ) as ARRAY<STRUCT<
        baseShippingCharged: STRING,
        cancelCode: STRING,
        cancelReason: STRING,
        cancelledQty: STRING,
        carrier: STRING,
        cartonNum: STRING,
        cegrRefId: STRING,
        entryId: STRING,
        entryStatus: STRING,
        fulfilledQty: STRING,
        metadata: STRING,
        modifiedDate: STRING,
        pricing: STRUCT<
            currencyIso: STRING,
            discountAmount: STRING,
            discountedTotalAmount: STRING,
            giftBoxAmount: STRING,
            giftBoxTaxAmount: STRING,
            originalRetailPrice: STRING,
            priceOverrideReason: STRING,
            shippingAmount: STRING,
            shippingTaxAmount: STRING,
            subTotalAmount: STRING,
            taxAmount: STRING,
            totalAmount: STRING,
            unitPrice: STRING
        >,
        productCode: STRING,
        productType: STRING,
        requestSystem: STRING,
        requestedBy: STRING,
        requestedQty: STRING,
        requestingSystemLineNo: STRING,
        shippedDate: STRING,
        size: STRING,
        sku: STRING,
        storeNumber: STRING,
        ticketNumber: STRING,
        trackingId: STRING,
        trackingUrl: STRING,
        updated: STRING
    >>) as consignmentEntries
  from (
    select stg.*, row_number() over (partition by stg.org_id,stg.ord_id,stg.ord_ln_id,stg.consignment_id,stg.consignment_status order by stg.updated_ts desc) as entry_rnk
    from consignments stg
        join consignments_main main on (stg.org_id=main.org_id and stg.ord_id=main.ord_id and stg.consignment_id=main.consignment_id and stg.consignment_status=main.consignment_status and stg.updated_ts<=main.updated_ts)
    ) ent
    left join  ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v loc on (coalesce(ent.ship_from_loc_id,ent.physical_org_id) = loc.loc_id)
    left join dim_location dim_loc on (lpad(dim_loc.loc_snum,5,0) = lpad(coalesce(ent.ship_from_loc_id,ent.physical_org_id),5,0))
    left join product_master pm ON ((ent.is_gift_card!=1 and trim(ent.item_id) = trim(pm.global_size_id) AND (CASE WHEN ORG_ID IN ('FL-CA','CH-CA') THEN '98' ELSE '81' END)=pm.banner_id))
    left join product_master_div pm_div ON (ent.is_gift_card!=1 and trim(ent.item_id) = trim(pm_div.global_size_id) AND ent.ORG_ID = pm_div.org_desc)
    left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_item_v itm on (trim(ent.ITEM_ID) = trim(itm.ITEM_ID))
    left join ${dom_gold_db}.${dom_gold_schema}.lkp_cancel_code_reason_v  cnl_reason on (ent.cnl_reason_id =cnl_reason.cancel_reason_id)
  where ent.entry_rnk=1
  group by all
),
order_line_tax as (
    select org_id con_org_id,ord_id con_ord_id,ord_ln_id as con_ord_ln_id,
    	ABS(SUM(COALESCE(fv.fv_cnlled_ord_shipping_tax_amt, 0) + COALESCE(fv.fv_ord_shipping_tax_amt, 0))) AS shippingtaxamount,
        ABS(SUM(COALESCE(fv.fv_cnlled_ord_sales_tax_amt, 0) + COALESCE(fv.fv_ord_sales_tax_amt, 0))) AS taxamount
    from 
		fct_mao_ord_line fv
    WHERE ord_ln_rnk = 1
    group by all
),
-- payment CTE
payment AS (
    SELECT 
        pymt.* EXCEPT(pymt_txn_amt),
        CASE 
            WHEN pymt.pymt_txn_cnt = 1 THEN COALESCE(pymt.inv_ln_total, pymt.pymt_txn_amt, 0)
            ELSE COALESCE(pymt.pymt_txn_amt, 0)
        END AS pymt_txn_amt
    FROM (
        SELECT
            p.*,
            ROW_NUMBER() OVER (
                PARTITION BY p.org_id, p.ord_id, p.ord_ln_id, p.pymt_txn_id, p.pymt_txn_dtl_id, p.inv_id, p.inv_ln_id
                ORDER BY p.inv_ln_updated_ts DESC, p.src_load_ts DESC
            ) AS pymt_rnk,
            cnt.pymt_txn_cnt
        FROM ${dom_gold_db}.${dom_gold_schema}.FCT_MAO_ORD_PYMT_LINE_V p
        LEFT JOIN (
            SELECT
                org_id, ord_id, ord_ln_id, inv_id, inv_ln_id,
                COUNT(DISTINCT pymt_txn_id || '~' || COALESCE(pymt_txn_dtl_id, '')) AS pymt_txn_cnt
            FROM ${dom_gold_db}.${dom_gold_schema}.FCT_MAO_ORD_PYMT_LINE_V
            WHERE LOWER(pymt_txn_type) NOT IN ('authorization', 'authorization reversal','refund')
            GROUP BY org_id, ord_id, ord_ln_id, inv_id, inv_ln_id
        ) cnt
            ON  p.org_id    = cnt.org_id
            AND p.ord_id    = cnt.ord_id
            AND p.ord_ln_id = cnt.ord_ln_id
            AND p.inv_id    = cnt.inv_id
            AND p.inv_ln_id = cnt.inv_ln_id
        WHERE LOWER(p.pymt_txn_type) NOT IN ('authorization', 'authorization reversal','refund')
    ) pymt
    WHERE pymt.pymt_rnk = 1
),
payment_grouped AS (
   SELECT
    org_id,
    ord_id,
    ord_ln_id,
    COLLECT_LIST(
      STRUCT(
        CAST(ABS(COALESCE(pymt_txn_amt,0)) AS STRING) AS amount,
        CAST(auth_cd AS STRING) AS authCode,
        CAST(txn_card_last4 AS STRING) AS cardLast4,
        CAST(inv_id AS STRING) AS cegrRefId,
        CAST(PYMT_TXN_ID AS STRING) AS paymentTransactionId,
        CAST(NULL AS STRING) AS paymentTransactionSubType,
        CAST(PYMT_TXN_TYPE AS STRING) AS paymentTransactionType,
        CAST(CASE WHEN pymt_type = 'Gift Card' THEN 'GIFTCARD' WHEN pymt_type = 'Credit Card' THEN 'CREDITCARD' ELSE UPPER(pymt_type) END AS STRING) AS paymentType,
        CAST(PYMT_CARD_TYPE AS STRING) AS creditCardType,
        CAST(created_ts AS STRING) AS date,
        --CAST(ccy_code AS STRING) as currencyIso,
        CAST(shippingtaxamount AS STRING) AS shippingTaxAmount,
        CAST(taxamount AS STRING) AS taxAmount,
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
          CAST(ABS(COALESCE(pymt_txn_req_amt,0)) AS STRING) AS authAmount,
          CAST(auth_cd AS STRING) AS authCode,
          CAST(NULL AS STRING) AS errorMessage,
          CAST(NULL AS STRING) AS id,
          CAST(ord_id AS STRING) AS originalOrderNumber,
          CAST(pymt_type AS STRING) AS paymentType,
          CAST(NULL AS STRING) AS preSettled,
          CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt, created_ts) AS STRING) AS transactionDate,
          CAST(pymt_txn_id AS STRING) AS transactionId
        ) AS authorization,
        -- creditCard struct: only populate if pymt_type = 'Credit Card', else null
        CASE WHEN pymt_type = 'Credit Card' THEN
          STRUCT(
            STRUCT(
              CAST(ABS(COALESCE(pymt_txn_req_amt,0)) AS STRING) AS authAmount,
              CAST(auth_cd AS STRING) AS authCode,
              CAST(pymt_txn_status_desc AS STRING) AS authResponse,
              CAST(COALESCE(pymt_txn_dt, pymt_txn_req_dt, created_ts) AS STRING) AS authTime,
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
            CAST(ABS(COALESCE(pymt_txn_amt,0)) AS STRING) AS amount,
            CAST(auth_cd AS STRING) AS authCode,
            CAST(attrib_card_last4 AS STRING) AS giftCardNumber,
            CAST(ord_id AS STRING) AS originalOrderNumber,
            CAST(NULL AS STRING) AS preSettled,
            CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt, created_ts) AS STRING) AS transactionDate,
            CAST(pymt_txn_id AS STRING) AS transactionId
          )
        ELSE NULL END AS giftCard,
        CASE WHEN pymt_type = 'PayPal' THEN
          STRUCT(
            CAST(ABS(COALESCE(pymt_txn_amt,0)) AS STRING) AS amount,
            STRUCT(
              CAST(ABS(COALESCE(pymt_txn_req_amt,0)) AS STRING) AS authAmount,
              CAST(auth_cd AS STRING) AS authCode,
              CAST(pymt_txn_status_desc AS STRING) AS authResponse,
              CAST(COALESCE(pymt_txn_dt, pymt_txn_req_dt, created_ts) AS STRING) AS authTime,
              CAST(attrib_avs_code AS STRING) AS avsCode,
              CAST(attrib_cvv_response AS STRING) AS cvvResponse,
              CAST(ord_id AS STRING) AS originalOrderNumber,
              CAST(NULL AS STRING) AS preSettled,
              CAST(transaction_ref_id AS STRING) AS referenceNumber,
              CAST(pymt_txn_id AS STRING) AS transactionId
            ) AS authInfo,
            CAST(addr_email AS STRING) AS paypalEmailId,
            CAST(coalesce(PYMT_TXN_DT,pymt_txn_req_dt,created_ts) AS STRING) AS transactionDate,
            CAST(pymt_txn_id AS STRING) AS transactionId
        )
        ELSE NULL END AS paypal
      )
    ) AS payments_info,
    ARRAY_AGG(pymt_txn_type) as pymt_txn
  FROM payment pymt left join order_line_tax con on  (pymt.org_id=con.con_org_id and pymt.ord_id=con.con_ord_id and pymt.ord_ln_id=con.con_ord_ln_id)
  WHERE pymt_rnk = 1
  GROUP BY all
)
select
    con.ord_id as order_id,
    cast(case trim(con.org_id)
        when 'FL-US' then '21'
        when 'FL-CA' then '45'
        when 'KFL-US' then '22'
        when 'CH-CA' then '77'
        when 'CH-US' then '20'
    end as string) as company_number,
    cast(DATE_FORMAT(
            TO_TIMESTAMP(con.created_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
            "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
        )as string) as orderDateTime,
    cast(con.consignment_id as string) as consignment_id,
    case upper(trim(con.dlvry_method_id))
    when 'EMAIL' then 'EGC'
    else  'OBF' end     as  consignment_fullfillment_system,
    case upper(trim(con.dlvry_method_id))
      when 'PICKUPATSTORE' then 'PICK'
      when 'SHIPTOADDRESS' then 'SHIP'
      when 'SHIPTOSTORE' then 'SHIP'
      when 'SHIPTORETURNCENTER' then 'SHIP'
      when 'EMAIL' then 'ELECTRONIC'
      when 'STORESALE' then 'XSTORE'
      when 'STORERETURN'then 'XSTORE'
    else upper(trim(con.dlvry_method_id))
    end as consignment_fullfillment_type,
    cast(null as string) as consignment_fulfillmentSystemRequestId,
    cast(null as string) as consignment_fulfillmentSystemResponseId,
    cast(con.inv_id as string) as invoice_Num,
    ent.consignmentEntries,
    coalesce(dim_loc.loc_snum, con.ship_from_loc_id,con.physical_org_id) as consignment_storeNumber,   --Brining the Location from MAO if the DIM_LOCATION is null.
    con.consignment_status AS consignment_status,
    case when con.consignment_status ='CONSIGNMENT_PAYMENT_SETTLED' then pymt_line.Payments_Info end as Payments_Info,
    cast(DATE_FORMAT(
                TO_TIMESTAMP(con.updated_ts, 'yyyy-MM-dd HH:mm:ss.SSS'),
                "yyyy-MM-dd'T'HH:mm:ss.SSS'000000Z'"
          )as string) as postedAt,
    con.updated_ts as load_time_kafka,
    con.etl_updt_ts as load_time_adls,
    date(con.created_ts) as orderdate,
    cast(con.gift_card_no as string)  as gift_card_number,
    cast(ABS(con.gift_card_value) as Decimal(26, 2)) as gift_card_amount,
    cast(case con.is_pre_sale
        when 1 then 'true'
        when 0 then 'false'
    end as string) as presell,
    pymt_line.pymt_txn as payment_transaction_subtype,
    --con.updated_ts as update_date,
    "MAO" as ref_source
from
    consignments_main con
    join consignment_entries ent on (con.org_id=ent.org_id and con.ord_id=ent.ord_id and con.consignment_id=ent.consignment_id and con.consignment_status=ent.consignment_status)
	  left join payment_grouped pymt_line ON con.org_id = pymt_line.org_id AND con.ord_id = pymt_line.ord_id AND con.ord_ln_id = pymt_line.ord_ln_id
    left join  ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v loc on (coalesce(con.ship_from_loc_id,con.physical_org_id) = loc.loc_id)
    left join dim_location dim_loc on (lpad(dim_loc.loc_snum,5,0) = lpad(coalesce(con.ship_from_loc_id,con.physical_org_id),5,0))
where
    con.doc_type_id = 'CustomerOrder'
);
