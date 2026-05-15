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
  pm.fob_desc,
  pm.desc_long_2,
  pm.designator_id,
  pm.legacy_sku_key,
  pm.legacy_sku
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
  tax_code,
  legacy_sku_key,
  legacy_sku,
  regexp_extract(legacy_sku_key, '^([^-]+-[^-]+-[^-]+)', 1) || '-' || calculateCheckDigit(replace(legacy_sku, '-', '')) || '-' || substr(split_part(legacy_sku_key, '-', -1), 1, 2) || '-' || substr(split_part(legacy_sku_key, '-', -1), 3, 3) as sku_size_with_check_digit
  FROM prod.product_npii.product_master
  where banner_id in ('03','16','18','76','77')
);

CREATE  OR REPLACE TEMP VIEW obf_final AS
(
with pymt_txn_status as (
		select * 
		from
			(select org_id,ord_id,pymt_txn_status_desc,row_number() over (partition by org_id,ord_id order by src_load_ts desc) rnk 
			from   ${dom_gold_db}.${dom_gold_schema}.FCT_MAO_ORD_PYMT_LINE_V )
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
    select * from (
        select fd.*,
            row_number() over (partition by fd.org_id,fd.ord_id,fd.ord_ln_id,fd.rel_id,fd.rel_ln_id order by updated_ts desc) ful_det_rnk
        from ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_fulfillment_dtl_v fd 
        ) where ful_det_rnk = 1
),
fct_mao_ord_line as (
    select oh.doc_type_id, oh.ccy_cd, oh.ord_locale, oh.cust_id, oh.flx_id, oh.pymt_status_id, oh.ord_total, oh.confirmed_ts, oh.captured_ts,oh.created_ts,oh.channel,
		ol.* except(created_ts),
        row_number() over (partition by ol.org_id, ol.ord_id, ol.ord_ln_id order by ol.updated_ts desc) ord_ln_rnk
    from ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_line_hist_v ol
        join ${dom_gold_db}.${dom_gold_schema}.fct_mao_ord_hdr_v oh on (oh.org_id = ol.org_id and oh.ord_id = ol.ord_id)
    where oh.doc_type_id = 'CustomerOrder'
        and not(ol.max_fulflmnt_status_id is null or ol.max_fulflmnt_status_id>9000)
		and ol.prnt_ord_id is null
		and ol.is_even_exchg = 0
        and ol.max_fulflmnt_status_id not in ('8000','8500')
        and ol.etl_updt_ts > '${lookback_date}'
),
fct_mao_ful_line as (
    select
        ol.doc_type_id, ol.ccy_cd, ol.ord_locale, ol.cust_id, ol.flx_id, ol.pymt_status_id, ol.ord_total, ol.confirmed_ts, ol.captured_ts,ol.created_ts,ol.channel,
        ol.is_base_shipping_charged, ol.ord_note, ol.total_disc_on_item, ol.total_disc, ol.orig_unit_price, ol.ord_shipping_amt, ol.ord_shipping_tax_amt, ol.ord_ln_sub_total, ol.total_taxes,
        ol.physical_org_id, ol.ship_from_loc_id, ol.ord_ln_total, ol.unit_price, ol.product_type, ol.qty, ol.is_gift_card, ol.is_pre_sale,
        ol.ship_to_addr_first_name, ol.ship_to_addr_last_name, ol.ship_to_addr_email, ol.ship_to_addr_phone, ol.ship_to_addr_addr1,
        ol.ship_to_addr_addr2, ol.ship_to_addr_city, ol.ship_to_addr_country, ol.ship_to_addr_state, ol.ship_to_addr_postal_cd,
        ol.cart_shpmnt_method, ol.shpmnt_method, ol.itm_size, ol.is_backorderFlg,ol.promised_dlvry_dt,
        fl.* except(created_ts)
    from ${dom_gold_db}.${dom_gold_schema}.fct_mao_fulfillment_line_hist_v fl
        join ${dom_gold_db}.${dom_gold_schema}.fct_mao_fulfillment_hdr_v fh on (fh.org_id = fl.org_id and fh.fulflmnt_id = fl.fulflmnt_id)
        join fct_mao_ord_line ol on (fl.ord_id = ol.ord_id and fl.ord_ln_id = ol.ord_ln_id and ol.ord_ln_rnk = 1)
        where fl.etl_updt_ts > '${lookback_date}'
),
store_fulflmnts as (
    select distinct rel_id, rel_ln_id from fct_mao_ful_line
),
mao_consignments as (
    select 'DCFulfillment' as fulflmnt_type, fd.org_id, fd.fulflmnt_dtl_pk, fd.fulflmnt_dtl_id, fd.ord_id, fd.ord_ln_id, fd.rel_id as fulflmnt_id, fd.rel_ln_id as fulflmnt_ln_id,
        fd.rel_id, fd.rel_ln_id, fd.shipment_id, fd.item_id, fd.pkg_id, fd.pkg_dtl_id, fd.inv_id, fd.dlvry_method_id, fd.dlvry_method_sub_type, fd.ship_via_id, fd.gift_card_no,
        fd.gift_card_pin, fd.gift_card_value, fd.carrier_cd, fd.tracking_num, fd.serial_num, fd.sgtin, ol.channel,
        ol.max_fulflmnt_status_id as fulflmnt_ln_status_id, ol.max_fulflmnt_status_desc as fulflmnt_ln_status_desc, ol.cnl_reason_id, ol.cnl_reason_desc,
        ol.unit_price as item_unit_price, fd.ord_qty as odrd_qty, null as picked_qty, null as pked_qty, null as shipped_qty, fd.cnl_qty as cnlled_qty, fd.fulfld_qty,
        fd.fulflmnt_dt, fd.shpd_dt, ol.created_ts, ol.created_by, ol.updated_ts, ol.updated_by, ol.is_base_shipping_charged, ol.ord_note, ol.ccy_cd,
        ol.total_disc_on_item, ol.total_disc, ol.orig_unit_price, ol.ord_shipping_amt, ol.ord_shipping_tax_amt, ol.ord_ln_sub_total, ol.total_taxes,
        ol.ord_ln_total, ol.unit_price, concat(fd.rel_id, fd.rel_ln_id) as consignment_id, ol.product_type, ol.qty, ol.is_gift_card, ol.is_pre_sale,
        ol.physical_org_id, fd.ship_from_loc_id, ol.doc_type_id, ol.confirmed_ts, ol.captured_ts, ol.etl_updt_ts,
        ol.ship_to_addr_first_name, ol.ship_to_addr_last_name, ol.ship_to_addr_email, ol.ship_to_addr_phone, ol.ship_to_addr_addr1, ol.ship_to_addr_phone,
        ol.ship_to_addr_addr2, ol.ship_to_addr_city, ol.ship_to_addr_country, ol.ship_to_addr_state, ol.ship_to_addr_postal_cd, ol.ship_to_addr_email,
        ol.cust_id, ol.flx_id, ol.ord_total, ol.cart_shpmnt_method, ol.shpmnt_method, ol.ord_locale, ol.ship_to_addr_postal_cd, ol.itm_size,
        fd.fulflmnt_dtl_id as entryId, ol.pymt_status_id, ol.is_backorderFlg,ol.promised_dlvry_dt,fd.is_rejected,null as short_reason_id, null rejected_flg,coalesce(fd.rel_created_ts,fd.created_ts) as rel_created_ts
    from mao_ord_fulfillment_detail fd
        join fct_mao_ord_line ol on (fd.org_id = ol.org_id and fd.ord_id = ol.ord_id and fd.ord_ln_id = ol.ord_ln_id)
		left anti join store_fulflmnts sf ON (fd.rel_id = sf.rel_id AND fd.rel_ln_id = sf.rel_ln_id)
    union all
    select 'StoreFulfillment' as fulflmnt_type, fd.org_id, fd.fulflmnt_dtl_pk, fd.fulflmnt_dtl_id, fd.ord_id, fd.ord_ln_id, fd.rel_id as fulflmnt_id, fd.rel_ln_id as fulflmnt_ln_id,
        fd.rel_id, fd.rel_ln_id, fd.shipment_id, fd.item_id, fd.pkg_id, fd.pkg_dtl_id, fd.inv_id, fd.dlvry_method_id, fd.dlvry_method_sub_type, fd.ship_via_id, fd.gift_card_no,
        fd.gift_card_pin, fd.gift_card_value, fd.carrier_cd, fd.tracking_num, fd.serial_num, fd.sgtin, fl.channel,
        fl.fulflmnt_ln_status_id as fulflmnt_ln_status_id, fl.fulflmnt_ln_status_desc as fulflmnt_ln_status_desc, fl.cnl_reason_id, fl.cnl_reason_desc,
        fl.item_unit_price, fl.odrd_qty, fl.picked_qty, fl.pked_qty, fl.shipped_qty, fl.cnlled_qty, fd.fulfld_qty,
        fd.fulflmnt_dt, fd.shpd_dt, fl.created_ts, fl.created_by, fl.updated_ts, fl.updated_by, fl.is_base_shipping_charged, fl.ord_note, fl.ccy_cd,
        fl.total_disc_on_item, fl.total_disc, fl.orig_unit_price, fl.ord_shipping_amt, fl.ord_shipping_tax_amt, fl.ord_ln_sub_total, fl.total_taxes,
        fl.ord_ln_total, fl.unit_price, concat(fd.rel_id, fd.rel_ln_id) as consignment_id, fl.product_type, fl.qty, fl.is_gift_card, fl.is_pre_sale,
        fl.physical_org_id, fd.ship_from_loc_id, fl.doc_type_id, fl.confirmed_ts, fl.captured_ts, fl.etl_updt_ts,
        fl.ship_to_addr_first_name, fl.ship_to_addr_last_name, fl.ship_to_addr_email, fl.ship_to_addr_phone, fl.ship_to_addr_addr1, fl.ship_to_addr_phone,
        fl.ship_to_addr_addr2, fl.ship_to_addr_city, fl.ship_to_addr_country, fl.ship_to_addr_state, fl.ship_to_addr_postal_cd, fl.ship_to_addr_email,
        fl.cust_id, fl.flx_id, fl.ord_total, fl.cart_shpmnt_method, fl.shpmnt_method, fl.ord_locale, fl.ship_to_addr_postal_cd, fl.itm_size,
        fd.fulflmnt_dtl_id as entryId, fl.pymt_status_id, fl.is_backorderFlg,fl.promised_dlvry_dt,fd.is_rejected,fl.short_reason_id,fl.rejected_flg,coalesce(fd.rel_created_ts,fd.created_ts) as rel_created_ts
    from mao_ord_fulfillment_detail fd
        join fct_mao_ful_line fl on (fd.rel_id = fl.rel_id and fd.rel_ln_id = fl.rel_ln_id)
),
consignments_stg as (
    select c.*,
        CASE
            WHEN c.fulflmnt_type = 'DCFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id BETWEEN 1000 AND 3000 THEN 1000  --'NEW_ORDER'
                    WHEN c.fulflmnt_ln_status_id = 3500 THEN 1200  --'ACCEPTED'
                    WHEN c.fulflmnt_ln_status_id = 3600 AND COALESCE(C.cnlled_qty,0)=0 THEN 1300  --'PICKED'
                    WHEN c.fulflmnt_ln_status_id = 3600 AND COALESCE(C.cnlled_qty,0)!=0 THEN 1350  --'PARTIALLY_PICKED'
                    WHEN c.fulflmnt_ln_status_id = 3700 THEN  1500  --'PACKED'
                    WHEN c.fulflmnt_ln_status_id = 7000 AND COALESCE(C.cnlled_qty,0)=0 THEN 2000 --'FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 7000 AND COALESCE(C.cnlled_qty,0)!=0 THEN 2050  --'PARTIALLY_FULFILLED'
                    WHEN c.is_rejected = 1 AND c.fulflmnt_ln_status_id = 9000 THEN 2900  --'REJECTED'
                    WHEN c.is_rejected = 0 AND c.fulflmnt_ln_status_id = 9000 THEN 2100  --'CANCELLED'
                    ELSE upper(c.fulflmnt_ln_status_id)
                END
            WHEN c.fulflmnt_type = 'StoreFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id = 1000 THEN 1000  --'NEW_ORDER'
                    WHEN c.fulflmnt_ln_status_id = 2000 THEN 1200  --'ACCEPTED'
                    WHEN c.fulflmnt_ln_status_id = 3000 AND COALESCE(C.cnlled_qty,0)=0 THEN 1300  --'PICKED'
                    WHEN c.fulflmnt_ln_status_id = 3000 AND COALESCE(C.cnlled_qty,0)!=0 THEN 1350  --'PARTIALLY_PICKED'
                    WHEN c.fulflmnt_ln_status_id BETWEEN 3500 AND 4000 THEN 1500  --'PACKED'
                    WHEN c.fulflmnt_ln_status_id = 5000 AND COALESCE(C.cnlled_qty,0)=0 THEN 2000  --'FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 6000 THEN 2000 --'FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 4500 THEN 2050 --'PARTIALLY_FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 5000 AND COALESCE(C.cnlled_qty,0)!=0 THEN 2050  --'PARTIALLY_FULFILLED'
                    WHEN (c.is_rejected = 1 or c.rejected_flg = 1) AND c.fulflmnt_ln_status_id = 9000 THEN 2900  --'REJECTED'
                    WHEN NOT(c.is_rejected = 1 or c.rejected_flg = 1) AND c.fulflmnt_ln_status_id = 9000 THEN 2100  --'CANCELLED'
                    ELSE upper(c.fulflmnt_ln_status_id)
                END
            ELSE upper(c.fulflmnt_ln_status_id)
        END AS StatusCode,
        CASE
            WHEN c.fulflmnt_type = 'DCFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id BETWEEN 1000 AND 3000 THEN 'NEW_ORDER'
                    WHEN c.fulflmnt_ln_status_id = 3500 THEN 'ACCEPTED'
                    WHEN c.fulflmnt_ln_status_id = 3600 AND COALESCE(C.cnlled_qty,0)=0 THEN 'PICKED'
                    WHEN c.fulflmnt_ln_status_id = 3600 AND COALESCE(C.cnlled_qty,0)!=0 THEN 'PARTIALLY_PICKED'
                    WHEN c.fulflmnt_ln_status_id = 3700 THEN 'PACKED'
                    WHEN c.fulflmnt_ln_status_id = 7000 AND COALESCE(C.cnlled_qty,0)=0 THEN 'FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 7000 AND COALESCE(C.cnlled_qty,0)!=0 THEN 'PARTIALLY_FULFILLED'
                    WHEN c.is_rejected = 1 AND c.fulflmnt_ln_status_id = 9000 THEN 'REJECTED'
                    WHEN c.is_rejected = 0 AND c.fulflmnt_ln_status_id = 9000 THEN 'CANCELLED'
                    ELSE upper(c.fulflmnt_ln_status_desc)
                END
            WHEN c.fulflmnt_type = 'StoreFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id = 1000 THEN 'NEW_ORDER'
                    WHEN c.fulflmnt_ln_status_id = 2000 THEN 'ACCEPTED'
                    WHEN c.fulflmnt_ln_status_id = 3000 AND COALESCE(C.cnlled_qty,0)=0 THEN 'PICKED'
                    WHEN c.fulflmnt_ln_status_id = 3000 AND COALESCE(C.cnlled_qty,0)!=0 THEN 'PARTIALLY_PICKED'
                    WHEN c.fulflmnt_ln_status_id BETWEEN 3500 AND 4000 THEN 'PACKED'
                    WHEN c.fulflmnt_ln_status_id = 5000 AND COALESCE(C.cnlled_qty,0)=0 THEN 'FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 6000 THEN 'FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 4500 THEN 'PARTIALLY_FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 5000 AND COALESCE(C.cnlled_qty,0)!=0 THEN 'PARTIALLY_FULFILLED'
                    WHEN (c.is_rejected = 1 or c.rejected_flg = 1) AND c.fulflmnt_ln_status_id = 9000 THEN 'REJECTED'
                    WHEN NOT(c.is_rejected = 1 or c.rejected_flg = 1) AND c.fulflmnt_ln_status_id = 9000 THEN 'CANCELLED'
                    ELSE upper(c.fulflmnt_ln_status_desc)
                END
            ELSE upper(c.fulflmnt_ln_status_desc)
        END AS Status
  from mao_consignments c
  where c.fulflmnt_ln_status_id not in (8000,8500)
  union all
  select c.*,
    CASE 
      WHEN c.is_rejected = 1 then 2900  --'REJECTED'
      WHEN c.rejected_flg = 1 then 2900  --'REJECTED'
    END AS statusCode,
    CASE 
      WHEN c.is_rejected = 1 then 'REJECTED'
      WHEN c.rejected_flg = 1 then 'REJECTED'
    END AS status
  from mao_consignments c
  where (c.is_rejected = 1 or c.rejected_flg = 1)
    and c.fulflmnt_ln_status_id != 9000
),
consignments as (
    select con.*, row_number() over (partition by con.org_id, con.ord_id, con.ord_ln_id, con.consignment_id, Status order by con.updated_ts desc) consignment_rnk
    from consignments_stg con
),
consignments_main as (
  select * from consignments where consignment_rnk=1
)
select
con.consignment_id AS consignmentId,
con.ORD_ID AS orderNumber,
case upper(trim(con.dlvry_method_id))
  when 'PICKUPATSTORE' then 'PICK'
  when 'SHIPTOADDRESS' then 'SHIP'
  when 'SHIPTOSTORE' then 'SHIP'
  when 'SHIPTORETURNCENTER' then 'SHIP'
  when 'EMAIL' then 'ELECTRONIC'
  when 'STORESALE' then 'XSTORE'
  when 'STORERETURN'then 'XSTORE'
  else upper(trim(con.dlvry_method_id))
end AS fulfillmentType,
con.fulflmnt_id AS fulfillmentOrderNumber,
con.fulflmnt_ln_id AS fulfillmentOrderLineNumber,
cast(con.statuscode as bigint) AS statusCode,
cast(con.Status as string) AS status,
coalesce(dim_loc.loc_num, con.ship_from_loc_id,con.physical_org_id) AS location,   --adding original location if the dim_location is null
CASE
    WHEN UPPER(loc.LOC_TYPE_ID) = 'DC' THEN 'WHSE'
    WHEN UPPER(loc.LOC_TYPE_ID) = 'SUPPLIER' THEN 'DROPSHIP'
    WHEN UPPER(loc.LOC_TYPE_ID) = 'STORE' THEN 'STORE'
    else UPPER(loc.LOC_TYPE_ID)
END AS locationType,
cast(NULL as string) AS newLocation,
cast(ABS(coalesce(con.odrd_qty,0)) as bigint) AS orderedQuantity,
cast(ABS(coalesce(con.picked_qty,0)) as bigint) AS pickedQuantity,
cast(ABS(coalesce(con.pked_qty,0)) as bigint) AS packedQuantity,
cast(ABS(coalesce(con.shipped_qty,0)) as bigint) AS shippedQuantity,
cast(ABS(coalesce(con.shipped_qty,0)) as bigint) AS currentShippedQuantity,
cast(ABS(coalesce(con.cnlled_qty,0)) as bigint) AS cancelledQuantity,
cast(cnl_reason.oms_cancel_code as string) as cancelReasonCode,  --review required
case when con.cnl_reason_id is not null then con.updated_by end AS cancelledby,
con.rel_created_ts AS createdDateTime,
con.updated_ts AS updatedDateTime,
con.pkg_id AS containerNumber,
con.TRACKING_NUM AS trackingNumber,
cast(ABS(coalesce(con.qty,0)) as bigint) AS quantity,
con.shpd_dt AS shipDate,
UPPER(trim(con.ship_via_id)) AS carrier,
CAST(
        case
            when con.is_backorderFlg = 1 then true
            when con.is_backorderFlg = 0 then false
        end AS boolean
    ) as backOrdered,--not mapped
cast(case
    WHEN con.CHANNEL !='XSTORE' AND  con.org_id in ('FL-US','KFL-US','CH-US') then concat_ws('-', pm.online_us_sku, pm.legacy_size_desc)
    WHEN con.CHANNEL !='XSTORE' AND  con.org_id in ('FL-CA','CH-CA') then concat_ws('-', pm.online_ca_sku, pm.legacy_size_desc)
    WHEN con.CHANNEL ='XSTORE' THEN pm_div.sku_size_with_check_digit
end as string) AS cpid,
cast(con.odrd_qty as string) as originalOrderQuantity,
con.ORG_ID AS organizationCode,
cast(ABS(coalesce(con.unit_price,0)) as string) AS unitPrice,
cast(con.promised_dlvry_dt as TIMESTAMP) AS expectedDeliveryDate,
con.created_ts AS orderDate,
case when con.is_pre_sale=0 then 'false' end AS presell,
date(con.updated_ts) AS load_date,
"MAO" as ref_source
from
    consignments_main con
    left join ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v loc on (coalesce(con.ship_from_loc_id,con.physical_org_id) = loc.loc_id)
    left join dim_location dim_loc on (lpad(dim_loc.loc_snum,5,0) = lpad(coalesce(con.ship_from_loc_id,con.physical_org_id),5,0))
    left join product_master pm ON ((con.is_gift_card!=1 and trim(con.item_id) = trim(pm.global_size_id) AND (CASE WHEN con.ORG_ID IN ('FL-CA','CH-CA') THEN '98' ELSE '81' END)=pm.banner_id))
    left join product_master_div pm_div ON (con.is_gift_card!=1 and trim(con.item_id) = trim(pm_div.global_size_id) AND con.ORG_ID = pm_div.org_desc)
	left join ${dom_gold_db}.${dom_gold_schema}.lkp_cancel_code_reason_v cnl_reason on (coalesce(con.cnl_reason_id,con.short_reason_id) =cnl_reason.cancel_reason_id)
);
