{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , v_batch_id ) %}

{{ config(
    materialized="incremental",
    unique_key=["consignmentid", "ordernumber", "fulfillmentorderlinenumber", "status"],
    post_hook=["{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_refactored', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'updateddatetime' ) }}"],
    meta={'strategy': "merge"}
) }}


WITH product_master AS (
    SELECT
        pm.internal_product_number_flca, pm.internal_product_number, pm.legacy_size_desc,
        pm.online_us_sku, pm.online_ca_sku, pm.global_size_id, pm.banner_id,
        pm.fob_desc, pm.desc_long_2, pm.designator_id, pm.legacy_sku_key, pm.legacy_sku
    FROM {{ ref('product_master_v') }} pm
    WHERE pm.banner_id IN ('81', '98')
    GROUP BY ALL
),
product_master_div AS (
    SELECT
        internal_product_number_flca, internal_product_number, legacy_size_desc,
        online_us_sku, online_ca_sku,
        CONCAT(TRIM(legacy_sku), '-', TRIM(legacy_size_code)) AS legacy_sku_size,
        global_size_id, banner_id,
        CASE WHEN banner_id = '03' THEN 'FL-US' WHEN banner_id = '16' THEN 'KFL-US' WHEN banner_id = '18' THEN 'CH-US' WHEN banner_id = '76' THEN 'FL-CA' WHEN banner_id = '77' THEN 'CH-CA' END AS org_desc,
        global_brand_desc, fob_desc, desc_long_2, "DESC", designator_id, cost, size_default_established_cost, size_default_established_cost_flca, tax_code, legacy_sku_key, legacy_sku,
        REGEXP_SUBSTR(legacy_sku_key, '^([^-]+-[^-]+-[^-]+)', 1, 1, 'e', 1) || '-' || {{ env_var('DBT_GOLD_DATABASE') }}.{{ env_var('DBT_GOLD_SCHEMA') }}.calculateCheckDigit(REPLACE(legacy_sku, '-', ''))::STRING || '-' || SUBSTR(SPLIT_PART(legacy_sku_key, '-', -1), 1, 2) || '-' || SUBSTR(SPLIT_PART(legacy_sku_key, '-', -1), 3, 3) AS sku_size_with_check_digit
    FROM {{ ref('product_master_v') }}
    WHERE banner_id IN ('03', '16', '18', '76', '77')
    GROUP BY ALL
),
pymt_txn_status AS (
    SELECT * FROM (SELECT org_id, ord_id, pymt_txn_status_desc, ROW_NUMBER() OVER (PARTITION BY org_id, ord_id ORDER BY src_load_ts DESC) AS rnk FROM {{ source('dom_gold', 'fct_mao_ord_pymt_line_v') }}) WHERE rnk = 1
),
dim_location AS (
    SELECT * FROM (SELECT loc_snum, loc_num, ROW_NUMBER() OVER (PARTITION BY LPAD(loc_snum, 5, '0') ORDER BY loc_sk DESC, loc_seq_num DESC) AS loc_rnk FROM {{ source('location_gold', 'dim_location_v') }} WHERE UPPER(banner_geo) = 'NA') WHERE loc_rnk = 1
),
mao_ord_fulfillment_detail AS (
    SELECT * 
    FROM 
        (SELECT fd.*, ROW_NUMBER() OVER (PARTITION BY fd.org_id, fd.ord_id, fd.ord_ln_id, fd.rel_id, fd.rel_ln_id ORDER BY fd.updated_ts DESC) AS ful_det_rnk 
        from {{ source('dom_gold', 'fct_mao_ord_fulfillment_dtl_v') }} fd
        )
    WHERE ful_det_rnk = 1
        {% if is_incremental() %}
            and etl_updt_ts >= {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL_DBIMART") | as_text }}'
        {% endif %}
),
fct_mao_ord_line as (
    select oh.doc_type_id, oh.ccy_cd, oh.ord_locale, oh.cust_id, oh.flx_id, oh.pymt_status_id, oh.ord_total, oh.confirmed_ts, oh.captured_ts, oh.created_ts, oh.channel,
        ol.* EXCLUDE (created_ts),
        row_number() over (partition by ol.org_id, ol.ord_id, ol.ord_ln_id order by ol.updated_ts desc) ord_ln_rnk
    from {{ source('dom_gold', 'fct_mao_ord_line_hist_v') }} ol
        join {{ source('dom_gold', 'fct_mao_ord_hdr_v') }} oh on (oh.org_id = ol.org_id and oh.ord_id = ol.ord_id)
    where oh.doc_type_id = 'CustomerOrder'
        and not(ol.max_fulflmnt_status_id is null or ol.max_fulflmnt_status_id > 9000)
        and (ol.prnt_ord_id is null
        or ol.is_even_exchg = 1)
        and ol.max_fulflmnt_status_id not in ('8000', '8500')
),
fct_mao_ful_line as (
    select
        ol.doc_type_id, ol.ccy_cd, ol.ord_locale, ol.cust_id, ol.flx_id, ol.pymt_status_id, ol.ord_total, ol.confirmed_ts, ol.captured_ts, ol.created_ts, ol.channel,
        ol.is_base_shipping_charged, ol.ord_note, ol.total_disc_on_item, ol.total_disc, ol.orig_unit_price, ol.ord_shipping_amt, ol.ord_shipping_tax_amt, ol.ord_ln_sub_total, ol.total_taxes,
        ol.physical_org_id, ol.ship_from_loc_id, ol.ord_ln_total, ol.unit_price, ol.product_type, ol.qty, ol.is_gift_card, ol.is_pre_sale,
        ol.ship_to_addr_first_name, ol.ship_to_addr_last_name, ol.ship_to_addr_email, ol.ship_to_addr_phone, ol.ship_to_addr_addr1,
        ol.ship_to_addr_addr2, ol.ship_to_addr_city, ol.ship_to_addr_country, ol.ship_to_addr_state, ol.ship_to_addr_postal_cd,
        ol.cart_shpmnt_method, ol.shpmnt_method, ol.itm_size, ol.is_backorderFlg, ol.promised_dlvry_dt,
        fl.* EXCLUDE (created_ts)
    from {{ source('dom_gold', 'fct_mao_fulfillment_line_hist_v') }} fl
        join {{ source('dom_gold', 'fct_mao_fulfillment_hdr_v') }} fh on (fh.org_id = fl.org_id and fh.fulflmnt_id = fl.fulflmnt_id)
        join fct_mao_ord_line ol on (fl.ord_id = ol.ord_id and fl.ord_ln_id = ol.ord_ln_id and ol.ord_ln_rnk = 1)
),
store_fulflmnts as (
    select distinct rel_id, rel_ln_id from fct_mao_ful_line
),
mao_rejections as (
    select
        'Rejections' as fulflmnt_type,
        fd.org_id,
        fd.fulflmnt_dtl_pk,
        fd.fulflmnt_dtl_id,
        fd.ord_id,
        fd.ord_ln_id,
        fd.rel_id as fulflmnt_id,
        fd.rel_ln_id as fulflmnt_ln_id,
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
        ol.channel,
        ol.max_fulflmnt_status_id as fulflmnt_ln_status_id,
        ol.max_fulflmnt_status_desc as fulflmnt_ln_status_desc,
        ol.cnl_reason_id,
        ol.cnl_reason_desc,
        ol.unit_price as item_unit_price,
        fd.ord_qty as odrd_qty,
        null as picked_qty,
        null as pked_qty,
        null as shipped_qty,
        fd.cnl_qty as cnlled_qty,
        fd.fulfld_qty,
        fd.fulflmnt_dt,
        fd.shpd_dt,
        ol.created_ts,
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
        ol.ord_ln_total,
        ol.unit_price,
        concat(fd.rel_id, fd.rel_ln_id) as consignment_id,
        ol.product_type,
        ol.qty,
        ol.is_gift_card,
        ol.is_pre_sale,
        ol.physical_org_id,
        fd.ship_from_loc_id,
        ol.doc_type_id,
        ol.confirmed_ts,
        ol.captured_ts,
        ol.etl_updt_ts,
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
        fd.fulflmnt_dtl_id as entryId,
        ol.pymt_status_id,
        ol.is_backorderFlg,
        ol.promised_dlvry_dt,
        fd.is_rejected,
        null as short_reason_id,
        null as rejected_flg,
        fd.created_ts as rel_created_ts
    from mao_ord_fulfillment_detail fd
        join fct_mao_ord_line ol on (fd.org_id = ol.org_id and fd.ord_id = ol.ord_id and fd.ord_ln_id = ol.ord_ln_id)
    WHERE fd.is_rejected = 1 and fd.status_id <= '3500' and ol.max_fulflmnt_status_id<'3500'
),
mao_consignments as (
    select
        'DCFulfillment' as fulflmnt_type,
        fd.org_id,
        fd.fulflmnt_dtl_pk,
        fd.fulflmnt_dtl_id,
        fd.ord_id,
        fd.ord_ln_id,
        fd.rel_id as fulflmnt_id,
        fd.rel_ln_id as fulflmnt_ln_id,
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
        ol.channel,
        ol.max_fulflmnt_status_id as fulflmnt_ln_status_id,
        ol.max_fulflmnt_status_desc as fulflmnt_ln_status_desc,
        ol.cnl_reason_id,
        ol.cnl_reason_desc,
        ol.unit_price as item_unit_price,
        fd.ord_qty as odrd_qty,
        null as picked_qty,
        null as pked_qty,
        null as shipped_qty,
        fd.cnl_qty as cnlled_qty,
        fd.fulfld_qty,
        fd.fulflmnt_dt,
        fd.shpd_dt,
        ol.created_ts,
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
        ol.ord_ln_total,
        ol.unit_price,
        concat(fd.rel_id, fd.rel_ln_id) as consignment_id,
        ol.product_type,
        ol.qty,
        ol.is_gift_card,
        ol.is_pre_sale,
        ol.physical_org_id,
        fd.ship_from_loc_id,
        ol.doc_type_id,
        ol.confirmed_ts,
        ol.captured_ts,
        ol.etl_updt_ts,
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
        fd.fulflmnt_dtl_id as entryId,
        ol.pymt_status_id,
        ol.is_backorderFlg,
        ol.promised_dlvry_dt,
        fd.is_rejected,
        null as short_reason_id,
        null as rejected_flg,
        fd.created_ts as rel_created_ts
    from mao_ord_fulfillment_detail fd
        join fct_mao_ord_line ol on (fd.org_id = ol.org_id and fd.ord_id = ol.ord_id and fd.ord_ln_id = ol.ord_ln_id)
    WHERE 
        NOT EXISTS (SELECT 'x' FROM store_fulflmnts sf WHERE fd.rel_id = sf.rel_id AND fd.rel_ln_id = sf.rel_ln_id)
        AND NOT EXISTS (SELECT 'x' FROM mao_rejections r WHERE fd.org_id = r.org_id AND fd.ord_id = r.ord_id AND fd.ord_ln_id = r.ord_ln_id AND fd.rel_id = r.rel_id AND fd.rel_ln_id = r.rel_ln_id)
    union all
    select
        'StoreFulfillment' as fulflmnt_type,
        fd.org_id,
        fd.fulflmnt_dtl_pk,
        fd.fulflmnt_dtl_id,
        fd.ord_id,
        fd.ord_ln_id,
        fd.rel_id as fulflmnt_id,
        fd.rel_ln_id as fulflmnt_ln_id,
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
        fl.channel,
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
        fl.ord_ln_total,
        fl.unit_price,
        concat(fd.rel_id, fd.rel_ln_id) as consignment_id,
        fl.product_type,
        fl.qty,
        fl.is_gift_card,
        fl.is_pre_sale,
        fl.physical_org_id,
        fd.ship_from_loc_id,
        fl.doc_type_id,
        fl.confirmed_ts,
        fl.captured_ts,
        fl.etl_updt_ts,
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
        fd.fulflmnt_dtl_id as entryId,
        fl.pymt_status_id,
        fl.is_backorderFlg,
        fl.promised_dlvry_dt,
        fd.is_rejected,
        fl.short_reason_id,
        fl.rejected_flg,
        fd.created_ts as rel_created_ts
    from mao_ord_fulfillment_detail fd
        join fct_mao_ful_line fl on (fd.rel_id = fl.rel_id and fd.rel_ln_id = fl.rel_ln_id)
    where NOT EXISTS (SELECT 'x' FROM mao_rejections r WHERE fd.org_id = r.org_id AND fd.ord_id = r.ord_id AND fd.ord_ln_id = r.ord_ln_id AND fd.rel_id = r.rel_id AND fd.rel_ln_id = r.rel_ln_id)
    union all
    select * from mao_rejections
),
consignments_stg as (
    select c.*,
        CASE
            WHEN c.fulflmnt_type = 'Rejections' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id BETWEEN 1000 AND 3000 THEN 1000
                    ELSE UPPER(c.fulflmnt_ln_status_id)
                END
            WHEN c.fulflmnt_type = 'DCFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id BETWEEN 1000 AND 3000 THEN 1000
                    WHEN c.fulflmnt_ln_status_id = 3500 THEN 1200
                    WHEN c.fulflmnt_ln_status_id = 3600 AND COALESCE(c.cnlled_qty, 0) = 0 THEN 1300
                    WHEN c.fulflmnt_ln_status_id = 3600 AND COALESCE(c.cnlled_qty, 0) != 0 THEN 1350
                    WHEN c.fulflmnt_ln_status_id = 3700 THEN 1500
                    WHEN c.fulflmnt_ln_status_id = 7000 AND COALESCE(c.cnlled_qty, 0) = 0 THEN 2000
                    WHEN c.fulflmnt_ln_status_id = 7000 AND COALESCE(c.cnlled_qty, 0) != 0 THEN 2050
                    WHEN c.fulflmnt_ln_status_id = 9000 THEN 2100
                    ELSE UPPER(c.fulflmnt_ln_status_id)
                END
            WHEN c.fulflmnt_type = 'StoreFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id = 1000 THEN 1000
                    WHEN c.fulflmnt_ln_status_id = 2000 THEN 1200
                    WHEN c.fulflmnt_ln_status_id = 3000 AND COALESCE(c.cnlled_qty, 0) = 0 THEN 1300
                    WHEN c.fulflmnt_ln_status_id = 3000 AND COALESCE(c.cnlled_qty, 0) != 0 THEN 1350
                    WHEN c.fulflmnt_ln_status_id BETWEEN 3500 AND 4000 THEN 1500
                    WHEN c.fulflmnt_ln_status_id = 5000 AND COALESCE(c.cnlled_qty, 0) = 0 THEN 2000
                    WHEN c.fulflmnt_ln_status_id = 6000 THEN 2000
                    WHEN c.fulflmnt_ln_status_id = 4500 THEN 2050
                    WHEN c.fulflmnt_ln_status_id = 5000 AND COALESCE(c.cnlled_qty, 0) != 0 THEN 2050
                    WHEN c.fulflmnt_ln_status_id = 9000 THEN 2100
                    ELSE UPPER(c.fulflmnt_ln_status_id)
                END
            ELSE UPPER(c.fulflmnt_ln_status_id)
        END AS StatusCode,
        CASE
            WHEN c.fulflmnt_type = 'Rejections' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id BETWEEN 1000 AND 3000 THEN 'NEW_ORDER'
                    ELSE UPPER(c.fulflmnt_ln_status_desc)
                END
            WHEN c.fulflmnt_type = 'DCFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id BETWEEN 1000 AND 3000 THEN 'NEW_ORDER'
                    WHEN c.fulflmnt_ln_status_id = 3500 THEN 'ACCEPTED'
                    WHEN c.fulflmnt_ln_status_id = 3600 AND COALESCE(c.cnlled_qty, 0) = 0 THEN 'PICKED'
                    WHEN c.fulflmnt_ln_status_id = 3600 AND COALESCE(c.cnlled_qty, 0) != 0 THEN 'PARTIALLY_PICKED'
                    WHEN c.fulflmnt_ln_status_id = 3700 THEN 'PACKED'
                    WHEN c.fulflmnt_ln_status_id = 7000 AND COALESCE(c.cnlled_qty, 0) = 0 THEN 'FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 7000 AND COALESCE(c.cnlled_qty, 0) != 0 THEN 'PARTIALLY_FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 9000 THEN 'CANCELLED'
                    ELSE UPPER(c.fulflmnt_ln_status_desc)
                END
            WHEN c.fulflmnt_type = 'StoreFulfillment' THEN
                CASE
                    WHEN c.fulflmnt_ln_status_id = 1000 THEN 'NEW_ORDER'
                    WHEN c.fulflmnt_ln_status_id = 2000 THEN 'ACCEPTED'
                    WHEN c.fulflmnt_ln_status_id = 3000 AND COALESCE(c.cnlled_qty, 0) = 0 THEN 'PICKED'
                    WHEN c.fulflmnt_ln_status_id = 3000 AND COALESCE(c.cnlled_qty, 0) != 0 THEN 'PARTIALLY_PICKED'
                    WHEN c.fulflmnt_ln_status_id BETWEEN 3500 AND 4000 THEN 'PACKED'
                    WHEN c.fulflmnt_ln_status_id = 5000 AND COALESCE(c.cnlled_qty, 0) = 0 THEN 'FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 6000 THEN 'FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 4500 THEN 'PARTIALLY_FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 5000 AND COALESCE(c.cnlled_qty, 0) != 0 THEN 'PARTIALLY_FULFILLED'
                    WHEN c.fulflmnt_ln_status_id = 9000 THEN 'CANCELLED'
                    ELSE UPPER(c.fulflmnt_ln_status_desc)
                END
            ELSE UPPER(c.fulflmnt_ln_status_desc)
        END AS Status
    from mao_consignments c
    where c.fulflmnt_ln_status_id not in (8000, 8500)
    union all
    select c.*,
        2900 AS statusCode,
        'REJECTED' AS status
    from mao_consignments c
    where c.fulflmnt_type = 'Rejections'
),
consignments AS (
    SELECT con.*, ROW_NUMBER() OVER (PARTITION BY con.org_id, con.ord_id, con.ord_ln_id, con.consignment_id, con.status ORDER BY con.updated_ts DESC) AS consignment_rnk
    FROM consignments_stg con
),
consignments_main AS (SELECT * FROM consignments WHERE consignment_rnk = 1),
obf_order_status_history AS (
SELECT
    con.consignment_id AS consignmentid,
    con.ord_id AS ordernumber,
    CASE UPPER(TRIM(con.dlvry_method_id)) WHEN 'PICKUPATSTORE' THEN 'PICK' WHEN 'SHIPTOADDRESS' THEN 'SHIP' WHEN 'SHIPTOSTORE' THEN 'SHIP' WHEN 'SHIPTORETURNCENTER' THEN 'SHIP' WHEN 'EMAIL' THEN 'ELECTRONIC' WHEN 'STORESALE' THEN 'XSTORE' WHEN 'STORERETURN' THEN 'XSTORE' ELSE UPPER(TRIM(con.dlvry_method_id)) END AS fulfillmenttype,
    con.fulflmnt_id AS fulfillmentordernumber,
    con.fulflmnt_ln_id AS fulfillmentorderlinenumber,
    con.statuscode::VARCHAR AS statuscode,
    con.status::VARCHAR AS status,
    COALESCE(dim_loc.loc_num, con.ship_from_loc_id, con.physical_org_id) AS location,
    CASE WHEN UPPER(loc.loc_type_id) = 'DC' THEN 'WHSE' WHEN UPPER(loc.loc_type_id) = 'SUPPLIER' THEN 'DROPSHIP' WHEN UPPER(loc.loc_type_id) = 'STORE' THEN 'STORE' ELSE UPPER(loc.loc_type_id) END AS locationtype,
    NULL::VARCHAR AS newlocation,
    ABS(COALESCE(con.odrd_qty, 0))::BIGINT AS orderedquantity,
    ABS(COALESCE(con.picked_qty, 0))::BIGINT AS pickedquantity,
    ABS(COALESCE(con.pked_qty, 0))::BIGINT AS packedquantity,
    ABS(COALESCE(con.shipped_qty, 0))::BIGINT AS shippedquantity,
    ABS(COALESCE(con.shipped_qty, 0))::BIGINT AS currentshippedquantity,
    ABS(COALESCE(con.cnlled_qty, 0))::BIGINT AS cancelledquantity,
    cnl_reason.oms_cancel_code::VARCHAR AS cancelreasoncode,
    CASE WHEN con.cnl_reason_id IS NOT NULL THEN con.updated_by END AS cancelledby,
    con.rel_created_ts AS createddatetime,
    con.updated_ts AS updateddatetime,
    con.pkg_id AS containernumber,
    con.tracking_num AS trackingnumber,
    ABS(COALESCE(con.qty, 0))::BIGINT AS quantity,
    con.shpd_dt::TIMESTAMP AS shipdate,
    UPPER(TRIM(con.ship_via_id)) AS carrier,
    CASE WHEN con.is_backorderflg = 1 THEN TRUE WHEN con.is_backorderflg = 0 THEN FALSE END AS backordered,
    CASE WHEN con.channel != 'XSTORE' AND con.org_id IN ('FL-US', 'KFL-US', 'CH-US') THEN CONCAT_WS('-', pm.online_us_sku, pm.legacy_size_desc) WHEN con.channel != 'XSTORE' AND con.org_id IN ('FL-CA', 'CH-CA') THEN CONCAT_WS('-', pm.online_ca_sku, pm.legacy_size_desc) WHEN con.channel = 'XSTORE' THEN pm_div.sku_size_with_check_digit END AS cpid,
    con.odrd_qty::BIGINT AS originalorderquantity,
    con.org_id AS organizationcode,
    ABS(COALESCE(con.unit_price, 0))::VARCHAR AS unitprice,
    con.promised_dlvry_dt::TIMESTAMP AS expecteddeliverydate,
    con.created_ts AS orderdate,
    CASE WHEN con.is_pre_sale = 0 THEN 'false' END AS presell,
    DATE(con.updated_ts) AS load_date,
    'MAO' AS ref_source
FROM consignments_main con
    LEFT JOIN {{ source('dom_gold', 'dim_mao_loc_v') }} loc ON COALESCE(con.ship_from_loc_id, con.physical_org_id) = loc.loc_id
    LEFT JOIN dim_location dim_loc ON LPAD(dim_loc.loc_snum, 5, '0') = LPAD(COALESCE(con.ship_from_loc_id, con.physical_org_id), 5, '0')
    LEFT JOIN product_master pm ON con.is_gift_card != 1 AND TRIM(con.item_id) = TRIM(pm.global_size_id) AND (CASE WHEN con.org_id IN ('FL-CA', 'CH-CA') THEN '98' ELSE '81' END) = pm.banner_id
    LEFT JOIN product_master_div pm_div ON con.is_gift_card != 1 AND TRIM(con.item_id) = TRIM(pm_div.global_size_id) AND con.org_id = pm_div.org_desc
    LEFT JOIN {{ source('dom_gold', 'lkp_cancel_code_reason_v') }} cnl_reason ON
        CASE
            WHEN con.cnl_reason_id IS NOT NULL THEN con.cnl_reason_id::STRING = cnl_reason.cancel_reason_id::STRING
            ELSE con.short_reason_id = TRY_TO_NUMBER(cnl_reason.cancel_reason_id)
        END
)
SELECT
    CAST(consignmentid AS VARCHAR(50)) AS consignmentid,
    CAST(ordernumber AS VARCHAR(50)) AS ordernumber,
    CAST(fulfillmenttype AS VARCHAR(50)) AS fulfillmenttype,
    CAST(fulfillmentordernumber AS VARCHAR(50)) AS fulfillmentordernumber,
    CAST(fulfillmentorderlinenumber AS VARCHAR(50)) AS fulfillmentorderlinenumber,
    CAST(statuscode AS VARCHAR(50)) AS statuscode,
    CAST(status AS VARCHAR(100)) AS status,
    CAST(location AS VARCHAR(100)) AS location,
    CAST(locationtype AS VARCHAR(50)) AS locationtype,
    CAST(newlocation AS VARCHAR(50)) AS newlocation,
    CAST(orderedquantity AS DECIMAL(38,0)) AS orderedquantity,
    CAST(pickedquantity AS DECIMAL(38,0)) AS pickedquantity,
    CAST(packedquantity AS DECIMAL(38,0)) AS packedquantity,
    CAST(shippedquantity AS DECIMAL(38,0)) AS shippedquantity,
    CAST(currentshippedquantity AS DECIMAL(38,0)) AS currentshippedquantity,
    CAST(cancelledquantity AS DECIMAL(38,0)) AS cancelledquantity,
    CAST(cancelreasoncode AS VARCHAR(50)) AS cancelreasoncode,
    CAST(cancelledby AS VARCHAR(100)) AS cancelledby,
    CAST(createddatetime AS TIMESTAMP) AS createddatetime,
    CAST(updateddatetime AS TIMESTAMP) AS updateddatetime,
    CAST(containernumber AS VARCHAR(100)) AS containernumber,
    CAST(trackingnumber AS VARCHAR(100)) AS trackingnumber,
    CAST(quantity AS DECIMAL(38,0)) AS quantity,
    CAST(shipdate AS TIMESTAMP) AS shipdate,
    CAST(carrier AS VARCHAR(100)) AS carrier,
    CAST(backordered AS BOOLEAN) AS backordered,
    CAST(cpid AS VARCHAR(50)) AS cpid,
    CAST(originalorderquantity AS VARCHAR(100)) AS originalorderquantity,
    CAST(organizationcode AS VARCHAR(100)) AS organizationcode,
    CAST(unitprice AS VARCHAR(100)) AS unitprice,
    CAST(expecteddeliverydate AS TIMESTAMP) AS expecteddeliverydate,
    CAST(orderdate AS TIMESTAMP) AS orderdate,
    CAST(presell AS VARCHAR(100)) AS presell,
    CAST(load_date AS DATE) AS load_date,
    CAST(ref_source AS VARCHAR(50)) AS ref_source
FROM obf_order_status_history
