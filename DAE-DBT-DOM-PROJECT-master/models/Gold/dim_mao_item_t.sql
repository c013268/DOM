{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}
 
{% set target_key_column_list =
    [ "item_pk" ]  %}
   
{% set target_context_typ2_column_list =
["item_id", "short_description", "description", "brand", "color", "web_url", "color_image_uri", "small_image_uri", "dpt_num", "dpt", "str_dpt", "item_cd_dict", "size", "size_category_id", "style", "product_class", "product_sub_class", "item_height", "item_len", "item_vol", "vol_weight", "vol_weight_uom", "item_weight", "item_width", "vendor_style", "season", "season_yr", "ship_ready", "hazmat_cd", "frozen_flg", "hazmat_flg", "parcel_ship_allow_flg", "air_ship_allow_flg", "str_sold_flg", "digital_sold_flg", "str_pick_flg", "ship_flg", "discountable_flg", "exchangable_flg", "price_overridable_flg", "rtn_able_at_dc_flg", "rtn_able_at_str_flg", "taxable_flg", "tax_exemptable_flg", "tax_overrideable_flg", "discontinued_flg", "purchase_gift_flg", "gc_flg", "non_merch_flg", "recalled_flg", "rfid_tag_flg", "scan_only_flg", "champs_canada_digital_eligible_flg", "champs_us_digital_eligible_flg", "fl_canada_digital_eligible_flg", "fl_us_digital_eligible_flg", "kfl_us_digital_eligible_flg", "fl_boss_block_flg", "is_caselot_flg", "is_non_merchandise_flg", "item_max_disc_amt"]  %}
 
{{ config(
    materialized="incremental", 
    unique_key=["hash_sk","hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}"
                    ,"{{ fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_itm_silver','mao_itm_item_v'), this, 'item_pk' ) }}"
                    ,"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ), 'src_load_ts' ) }}"], 
    meta={'strategy': 'merge', 'update_condition': 'active_flg'}
) }}

with 
mao_itm_item as (
    select a.*
    from {{ source('src_itm_silver','mao_itm_item_v')}} a
    {% if is_incremental() %}
        where
            a.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
    {% endif %}
),
item_t  as (
select
a.pk as item_pk,
a.profile_id,
----------- item attr ------------
item_id,
short_description,
description,
brand, 
color, 
web_u_r_l as web_url,
color_image_u_r_i as color_image_uri,
small_image_u_r_i as small_image_uri,
department_number as dpt_num,
department_name as dpt,
store_department as str_dpt,
to_json(object_construct(upper(b.code_type_id)::varchar, b.value)) as item_cd_dict,

----------- product attr ------------
size,
size_category_id,
style,
product_class,
product_sub_class,
height as item_height,
length as item_len,
volume as item_vol,
volumetric_weight as vol_weight,
volumetric_weight_uom_code as vol_weight_uom,
weight as item_weight,
width as item_width,
----------- vendor attributes ------------
ext_vendor_style as vendor_style,
----------- season attr ------------
season,
season_year as season_yr,
----------- inv attr ------------
ship_ready,
----------- handling attr ------------
hazmat_code as hazmat_cd,
----------- handling flags ------------
is_frozen as frozen_flg,
is_hazmat as hazmat_flg,
is_parcel_shipping_allowed as parcel_ship_allow_flg,
is_air_shipping_allowed as air_ship_allow_flg,
----------- sellings flags ------------
sold_in_stores as str_sold_flg,
sold_online as digital_sold_flg,
pick_up_in_store as str_pick_flg,
ship_to_address as ship_flg,
is_discountable as discountable_flg,
is_exchangeable as exchangable_flg,
is_price_overrideable as price_overridable_flg,
is_returnable_at_d_c as rtn_able_at_dc_flg,
is_returnable_at_store as rtn_able_at_str_flg,
is_taxable as taxable_flg,
is_tax_exemptable as tax_exemptable_flg,
is_tax_overrideable as tax_overrideable_flg,
----------- flags ------------
is_discontinued as discontinued_flg,
is_giftwith_purchase as purchase_gift_flg,
is_gift_card as gc_flg,
is_non_merchandise as non_merch_flg,
is_recalled as recalled_flg,
is_rfid_tagged as rfid_tag_flg,
is_scan_only as scan_only_flg,
-- =============================================================================
-- digital eligible
-- =============================================================================
ext_is_digitaleligible_chca            as champs_canada_digital_eligible_flg,
ext_is_digitaleligible_chus            as champs_us_digital_eligible_flg,
ext_is_digitaleligible_flca            as fl_canada_digital_eligible_flg,
ext_is_digitaleligible_flus            as fl_us_digital_eligible_flg,
ext_is_digitaleligible_kflus           as kfl_us_digital_eligible_flg,

-- =============================================================================
-- general / misc
-- =============================================================================
ext_fl_boss_block                       as fl_boss_block_flg,
ext_is_caselot                          as is_caselot_flg,
ext_is_nonmerchandise                   as is_non_merchandise_flg,
----------- selling metrics ------------
max(max_discount_amount) as item_max_disc_amt,
----------- date -----------
a.src_load_ts as src_load_ts,
a.updated_timestamp as updt_ts 
from 
mao_itm_item a 
left join 
{{ source('src_itm_silver','mao_itm_item_code_v')}} b
on a.pk = b.item_pk
left join 
{{ source('src_itm_silver','mao_itm_handling_attributes_v')}} c
on a.pk = c.item_pk
left join 
{{ source('src_itm_silver','mao_itm_selling_attributes_v')}} d
on a.pk = d.item_pk
group by all
),
item_main as (
    select
        src.*
        ,{{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk
        ,{{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
    from
        item_t src
    )
select
    src.*
    ,current_timestamp()::timestamp_ntz as start_ts
    ,null::timestamp as  end_ts
    ,'Y'::varchar(1) as active_flg
    ,'Y'::varchar(1) as reporting_flg
    ,{{ v_batch_id }}::decimal(38,0) as batch_id
    ,current_timestamp()::timestamp_ntz as etl_load_ts
    ,current_timestamp()::timestamp_ntz as etl_updt_ts
from item_main src

