{% set v_batch_id = fl_utils.m_get_batch_id( var('p_atp_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_atp_pipeline_name'), 'dom_silver', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_atp_pipeline_name'), 'dom_silver', this , v_batch_id ) %}

{{ config(
    materialized="incremental",
    table_format='iceberg',
    database=env_var('DBT_SILVER_DATABASE'),
    schema=env_var('DBT_SILVER_SCHEMA'),
    unique_key=["item_id", "location_id", "updated_date", "message_type", "selling_channel"],
    merge_exclude_columns=["etl_load_ts", "batch_id"],
    post_hook=["{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_atp_pipeline_name'), 'dom_silver', this , fl_utils.m_get_batch_id( var('p_atp_pipeline_name') ) , 'src_load_ts' ) }}"],
    tags=['atp', 'location_delta']
) }}

with loc_atp_delta as (
    select
        location_transaction_date_time::timestamp_ntz as updated_date,
        header_message_type as message_type,
        header_source_region as region,
        header_banner_name as banner_name,
        header_timezone as timezone,
        header_source_country as country,
        payload_transaction_type as transaction_type,
        location_item_id as item_id,
        location_location_id as location_id,
        location_on_hand_quantity as on_hand_qty,
        location_on_hand_status as on_hand_status,
        location_quantity as atp,
        location_status as status,
        location_view_name as selling_channel,
        location_future_quantity as future_qty,
        location_is_infinite_availability as is_infinite_availability,
        location_is_kit_item as is_kit_item,
        location_first_available_future_date as first_available_future_date,
        location_first_available_future_quantity as first_available_future_qty,
        header_timestamp::timestamp_ntz as header_timestamp,
        kafka_timestamp as src_load_ts
    from {{ source('src_atp_bronze', 'MAO_LOCATION_ATP_DELTA_V') }}
    where lower(header_message_type) = 'locationavailabilitydeltasync' and location_location_id is not null
    {% if is_incremental() %}
        and kafka_timestamp::timestamp_ntz > {{ v_inc_load_ts }}
    {% endif %}
),
loc_atp_delta_dedup as (
    select
        *,
        row_number() over (partition by item_id, location_id, updated_date, message_type, selling_channel order by header_timestamp desc) as rn
    from loc_atp_delta
)
select
    d.*,
    {{ v_batch_id }}::number(38, 0) as batch_id,
    current_timestamp()::timestamp_ntz as etl_load_ts,
    current_timestamp()::timestamp_ntz as etl_updt_ts
from loc_atp_delta_dedup d
where d.rn = 1
