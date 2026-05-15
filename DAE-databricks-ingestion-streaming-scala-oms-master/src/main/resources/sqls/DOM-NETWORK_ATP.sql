CREATE OR REPLACE TEMP VIEW network_atp AS
(
WITH product_master_filtered AS (
    SELECT distinct
        pm.online_us_sku,
        pm.online_us_sku_size,
        pm.online_ca_sku,
        pm.online_ca_sku_size,
        pm.sku_number,
        pm.global_size_id,
        pm.banner_id
    FROM prod.product_npii.product_master pm
    WHERE pm.banner_id IN ('81', '98')
)

select
distinct
'FL_NA' as orgId,
CASE
    WHEN product.banner_id = '81' THEN concat_ws('-', product.online_us_sku, product.online_us_sku_size)
    WHEN product.banner_id = '98' THEN concat_ws('-', product.online_ca_sku, product.online_ca_sku_size)
    ELSE NULL
END as productId,
CASE
    WHEN product.banner_id = '81' THEN concat_ws('-', product.online_us_sku, product.online_us_sku_size)
    WHEN product.banner_id = '98' THEN concat_ws('-', product.online_ca_sku, product.online_ca_sku_size)
    ELSE NULL
END as originalProductId,
'EACH' as uom,
product.sku_number as gtin,
cast(CASE WHEN atp>0 THEN TRUE ELSE FALSE end as string) as available,
cast(CASE WHEN store_atp>0 THEN TRUE ELSE FALSE end as string) as availableInStore,
cast(CASE WHEN wh_atp>0 THEN TRUE ELSE FALSE end as string) as availableInWarehouse,
cast (CASE WHEN dropship_atp>0 THEN TRUE ELSE FALSE end as string) as availableInDropShip,
CASE
    -- Foot Locker US
    WHEN UPPER(REPLACE(dom_network_atp_v.selling_channel, ' ', '')) = 'FL-US-SHIPTOHOME' THEN 'FL_US_ECOMM'
    WHEN UPPER(REPLACE(dom_network_atp_v.selling_channel, ' ', '')) = 'FL-US-BOPIS' THEN 'FL_US_RETAIL'

    -- Foot Locker Canada
    WHEN UPPER(REPLACE(dom_network_atp_v.selling_channel, ' ', '')) = 'FL-CA-SHIPTOHOME' THEN 'FL_CA_ECOMM'
    WHEN UPPER(REPLACE(dom_network_atp_v.selling_channel, ' ', '')) = 'FL-CA-BOPIS' THEN 'FL_CA_RETAIL'

    -- Champs US (New: CH -> Old: CHP)
    WHEN UPPER(REPLACE(dom_network_atp_v.selling_channel, ' ', '')) = 'CH-US-SHIPTOHOME' THEN 'CHP_US_ECOMM'
    WHEN UPPER(REPLACE(dom_network_atp_v.selling_channel, ' ', '')) = 'CH-US-BOPIS' THEN 'CHP_US_RETAIL'

    -- Champs Canada (New: CH -> Old: CHP)
    WHEN UPPER(REPLACE(dom_network_atp_v.selling_channel, ' ', '')) = 'CH-CA-SHIPTOHOME' THEN 'CHP_CA_ECOMM'

    -- Kids Foot Locker US (New: KFL -> Old: KIDS)
    WHEN UPPER(REPLACE(dom_network_atp_v.selling_channel, ' ', '')) = 'KFL-US-SHIPTOHOME' THEN 'KIDS_US_ECOMM'
    WHEN UPPER(REPLACE(dom_network_atp_v.selling_channel, ' ', '')) = 'KFL-US-BOPIS' THEN 'KIDS_US_RETAIL'

    -- Kids Foot Locker Canada (New: KFL -> Old: KIDS)
    WHEN UPPER(REPLACE(dom_network_atp_v.selling_channel, ' ', '')) = 'KFL-CA-SHIPTOHOME' THEN 'KIDS_CA_ECOMM'
    ELSE dom_network_atp_v.selling_channel
END AS sellingChannel,
'ATP_PUBLISH' as transactionType,
date_format(DOM_NETWORK_ATP_V.updatedtime, "yyyy-MM-dd'T'HH:mm:ss.SSS") as updateTime,
CAST(NULL as string) preSell,
cast(DOM_NETWORK_ATP_V.future_date as string) as futureDate,
cast(DOM_NETWORK_ATP_V.atp as string) as totalAtp,
cast(DOM_NETWORK_ATP_V.wh_atp as string)as whAtp,
cast(DOM_NETWORK_ATP_V.store_atp as string) as storeAtp,
cast(DOM_NETWORK_ATP_V.dropship_atp as string) as dropshipAtp,
cast(DOM_NETWORK_ATP_V.load_time_kafka as timestamp) as load_time_kafka,
to_timestamp(now(),'yyyy-MM-dd HH:mm:ss') as load_time_adls,
current_date() as load_date,
MESSAGE_TYPE as messageType
from ${dom_gold_db}.${dom_gold_schema}.fct_mao_inv_network_atp_v DOM_NETWORK_ATP_V
left join product_master_filtered product
on trim(product.global_size_id) = trim(DOM_NETWORK_ATP_V.item_id)
AND (CASE 
        WHEN SUBSTRING_INDEX(DOM_NETWORK_ATP_V.SELLING_CHANNEL, '-', 2) IN ('FL-CA', 'CH-CA') THEN '98' 
        ELSE '81' 
     END) = product.banner_id
)
