{% set v_batch_id = fl_utils.m_get_batch_id( var('p_atp_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_atp_pipeline_name'), 'dom_silver', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_atp_pipeline_name'), 'dom_silver', this , v_batch_id ) %}

{{ config(
    materialized="incremental",
    table_format='iceberg',
    database=env_var('DBT_SILVER_DATABASE'),
    schema=env_var('DBT_SILVER_SCHEMA'),
    unique_key=["item_id", "updatedtime", "selling_channel", "message_type"],
    merge_exclude_columns=["etl_load_ts", "batch_id"],
    post_hook=["{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_atp_pipeline_name'), 'dom_silver', this , fl_utils.m_get_batch_id( var('p_atp_pipeline_name') ) , 'src_load_ts' ) }}"],
    tags=['atp']
) }}

with network_atp_v_dedup as (
    select
        header_message_type as message_type,
        header_source_environment as environment,
        header_banner_name as banner_name,
        header_source_country as source_country,
        header_source_region as region,
        header_time_zone as time_zone,
        transaction_type,
        trim(item_id) as item_id,
        on_hand_quantity,
        on_hand_status,
        total_quantity as atp,
        quantity_distribution_centers as wh_atp,
        quantity_stores as store_atp,
        quantity_suppliers as dropship_atp,
        status as atp_status,
        future_quantity,
        view_name as selling_channel,
        first_available_future_quantity as first_available_future_qty,
        next_availability_date,
        is_infinite_availability,
        is_kit_item,
        first_available_future_date as future_date,
        header_timestamp::timestamp_ntz as header_timestamp,
        transaction_date_time::timestamp_ntz as updatedtime,
        kafka_timestamp::timestamp_ntz as src_load_ts,
        row_number() over (partition by item_id, updatedtime, selling_channel, message_type order by header_timestamp desc) as rn
    from {{ source('src_atp_bronze', 'MAO_NETWORK_ATP_V') }}
    where lower(header_message_type) = 'networkfullavailabilitysync'
    {% if is_incremental() %}
        and kafka_timestamp > {{ v_inc_load_ts }}
    {% endif %}
)
select
    d.message_type::varchar as message_type,
    d.environment::varchar as environment,
    d.banner_name::varchar as banner_name,
    d.source_country::varchar as source_country,
    d.region::varchar as region,
    d.time_zone::varchar as time_zone,
    d.transaction_type::varchar as transaction_type,
    d.item_id::varchar as item_id,
    nvl(d.on_hand_quantity, 0)::number as on_hand_qty,
    d.on_hand_status::varchar as on_hand_status,
    nvl(d.atp, 0)::number as atp,
    case when nvl(d.atp, 0) = 0 then 0 else nvl(d.wh_atp, 0) end::number as wh_atp,
    case when nvl(d.atp, 0) = 0 then 0 else nvl(d.store_atp, 0) end::number as store_atp,
    case when nvl(d.atp, 0) = 0 then 0 else nvl(d.dropship_atp, 0) end::number as dropship_atp,
    d.atp_status::varchar as atp_status,
    nvl(d.future_quantity, 0)::number as future_quantity,
    d.selling_channel::varchar as selling_channel,
    nvl(d.first_available_future_qty, 0)::number as first_available_future_qty,
    d.next_availability_date::date as next_availability_date,
    d.is_infinite_availability::boolean as is_infinite_availability,
    d.is_kit_item::boolean as is_kit_item,
    d.future_date::date as future_date,
    d.header_timestamp::timestamp_ntz as header_timestamp,
    d.updatedtime::timestamp_ntz as updatedtime,
    d.src_load_ts::timestamp_ntz as src_load_ts,
    {{ v_batch_id }}::number(38, 0) as batch_id,
    current_timestamp()::timestamp_ntz as etl_load_ts,
    current_timestamp()::timestamp_ntz as etl_updt_ts
from network_atp_v_dedup d
where rn = 1
