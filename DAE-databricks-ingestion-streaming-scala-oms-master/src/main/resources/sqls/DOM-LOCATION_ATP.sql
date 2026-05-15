CREATE OR REPLACE TEMP VIEW product_master AS
SELECT
  trim(global_size_id) AS global_size_id,
  banner_id,
  sku_number,
  online_us_sku,
  online_us_sku_size,
  online_ca_sku,
  legacy_size_desc
FROM prod.product_npii.product_master
WHERE banner_id IN ('81', '98');
 
-- Step 2: Location
CREATE OR REPLACE TEMP VIEW dim_location_mv AS
select * from
       (select
            lpad(loc_snum, 5, '0') AS loc_snum_padded,loc_num,row_number() over (partition by lpad(loc_snum, 5, 0) order by loc_sk desc,loc_seq_num desc) loc_rnk
        from sf_gold_prod_db.location_gold_prod.dim_location_v where UPPER(banner_geo)='NA')
        where loc_rnk=1
;
 
-- Step 3: Final ATP View
CREATE OR REPLACE TEMP VIEW obf_atp_final AS
SELECT DISTINCT
  cast(loc.loc_num AS string) AS location_id,
  cast(UPPER(dim_mao_loc_v.LOC_TYPE_ID) AS string) AS location_type,
  cast('FL-NA' AS string) AS org_id,
  cast(
    CASE
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'FL-US-SHIPTOHOME' THEN 'FL_US_ECOMM'
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'FL-US-BOPIS' THEN 'FL_US_ECOMM'
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'FL-CA-SHIPTOHOME' THEN 'FL_CA_ECOMM'
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'FL-CA-BOPIS' THEN 'FL_CA_ECOMM'
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'CH-US-SHIPTOHOME' THEN 'CHP_US_ECOMM'
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'CH-US-BOPIS' THEN 'CHP_US_ECOMM'
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'CH-CA-SHIPTOHOME' THEN 'CHP_CA_ECOMM'
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'CH-CA-BOPIS' THEN 'CHP_CA_ECOMM'
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'KFL-US-SHIPTOHOME' THEN 'KIDS_US_ECOMM'
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'KFL-US-BOPIS' THEN 'KIDS_US_ECOMM'
      WHEN UPPER(REPLACE(atp.selling_channel, ' ', '')) = 'KFL-CA-SHIPTOHOME' THEN 'KIDS_CA_ECOMM'
      ELSE atp.selling_channel
    END
  AS string) AS selling_channel,
  cast(CASE WHEN atp.transaction_type = 'SyncDetail' THEN 'CHECKOUT' END AS string) AS transaction_type,
  cast(pm.sku_number AS string) AS gtin,
  cast(null AS string) AS launch_date,
  cast(null AS string) AS launch_date_time,
  cast(
    CASE
      WHEN UPPER(atp.selling_channel) LIKE '%CA%'
        THEN trim(concat_ws('-', pm.online_ca_sku, pm.legacy_size_desc))
      ELSE trim(concat_ws('-', pm.online_us_sku, pm.legacy_size_desc))
    END
  AS string) AS product_id,
  'EACH' AS uom,
  cast(
    CASE
      WHEN atp.selling_channel ILIKE '%BOPIS%' THEN 'PICK'
      WHEN atp.selling_channel ILIKE '%ShipToHome%' THEN 'SHIP'
      ELSE null
    END
  AS string) AS fulfillment_type,
  cast(atp.ATP AS string) AS atp,
  cast(CASE WHEN atp.Status = 'AVAILABLE' THEN 'GREEN' ELSE 'RED' END AS string) AS atp_status,
  cast('0.0' AS string) AS demand,
  cast('0' AS string) AS safety_stock,
  cast('COMMON_POOL' AS string) AS segment,
  cast('0.0' AS string) AS supply,
  cast(null AS string) AS future_qty_by_dates,
  cast(date_format(atp.UPDATED_DATE, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS") AS string) AS update_time,
  cast(atp.load_time_kafka AS timestamp) AS load_time_kafka,
  to_timestamp(now(),'yyyy-MM-dd HH:mm:ss') AS load_time_adls,
  current_date() AS load_date
FROM ${dom_gold_db}.${dom_gold_schema}.fct_mao_inv_location_atp_v atp
LEFT JOIN ${dom_gold_db}.${dom_gold_schema}.dim_mao_loc_v dim_mao_loc_v
  ON atp.LOCATION_ID = dim_mao_loc_v.LOC_ID
LEFT JOIN product_master pm
  ON pm.global_size_id = trim(atp.item_id)
  AND pm.banner_id = CASE
    WHEN UPPER(atp.selling_channel) LIKE '%CA%' THEN '98'
    ELSE '81'
  END
LEFT JOIN dim_location_mv loc
  ON loc.loc_snum_padded = lpad(atp.location_id, 5, '0')
WHERE atp.message_type = 'locationAvailabilityDeltaSync';