{% set v_batch_id = fl_utils.m_get_batch_id(var('p_atp_pipeline_name')) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_atp_pipeline_name'), 'dom_gold', this, v_batch_id) %}

{% set target_key_column_list = [ "updatedtime", "item_id", "message_type", "selling_channel", "atp" ] %}
{% set target_context_typ2_column_list = [ "environment", "region", "time_zone", "transaction_type", "on_hand_qty", "on_hand_status", "wh_atp", "store_atp", "dropship_atp", "atp_status", "future_quantity", "future_date", "load_time_kafka" ] %}

{{ config(
    materialized="incremental",
    unique_key=["hash_sk", "hash_seq_num"],
    merge_exclude_columns=["etl_load_ts"],
    post_hook=[
        "{{ fl_utils.m_upd_typ2_activ_records(this, 'hash_sk', 'hash_seq_num') }}",
        "{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit(var('p_atp_pipeline_name'), 'dom_gold', this, fl_utils.m_get_batch_id(var('p_atp_pipeline_name')), 'src_load_ts') }}"
    ],
    meta={'strategy': "merge", 'update_condition': "active_flg"}
) }}

with
network_atp_delta as (
    select
        item_id,
        selling_channel,
        message_type,
        environment,
        region,
        time_zone,
        transaction_type,
        on_hand_qty,
        on_hand_status,
        atp,
        wh_atp,
        store_atp,
        dropship_atp,
        atp_status,
        future_quantity,
        future_date,
        updatedtime,
        src_load_ts as load_time_kafka,
        src_load_ts
    from {{ source('src_atp_silver', 'mao_network_alerts_v') }}
    {% if is_incremental() %}
        where src_load_ts::timestamp_ntz >= DATEADD(day, -1, CURRENT_DATE())
    {% endif %}
),

network_atp_fullsync as (
    select
        item_id,
        selling_channel,
        message_type,
        environment,
        region,
        time_zone,
        transaction_type,
        on_hand_qty,
        on_hand_status,
        atp,
        wh_atp,
        store_atp,
        dropship_atp,
        atp_status,
        future_quantity,
        future_date,
        updatedtime,
        src_load_ts as load_time_kafka,
        src_load_ts
    from {{ source('src_atp_silver', 'mao_network_atp_v') }}
    {% if is_incremental() %}
        where src_load_ts::timestamp_ntz >= DATEADD(day, -2, CURRENT_DATE())
    {% endif %}
),

network_atp_stg as (
    select * from network_atp_delta
    union all
    select * from network_atp_fullsync
),

network_atp_main as (
    select
        src.*,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
    from
        network_atp_stg as src
)

select
    src.*,
    src.src_load_ts::timestamp_ntz as start_ts,
    null::timestamp_ntz as end_ts,
    'Y'::varchar(1) as active_flg,
    'Y'::varchar(1) as reporting_flg,
    {{ v_batch_id }}::number(38, 0) as batch_id,
    current_timestamp()::timestamp_ntz as etl_load_ts,
    current_timestamp()::timestamp_ntz as etl_updt_ts
from
    network_atp_main as src
