{% set v_batch_id = fl_utils.m_get_batch_id( var('p_atp_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_atp_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_atp_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "updated_date", "item_id", "location_id", "message_type", "selling_channel"]  %}

{% set target_context_typ2_column_list = 
    [ "transaction_type","region","timezone","country","on_hand_qty","on_hand_status","atp","status","future_qty", "load_time_kafka" ]  %}

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records( this ,'hash_sk' ,'hash_seq_num' ) }}",
					"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_atp_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_atp_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge", 'update_condition': "active_flg"}
) }}


with 
loc_atp_delta as (
	select
		item_id,
		location_id,
		selling_channel,
		message_type,
		transaction_type,
		region,
		timezone,
		country,
		on_hand_qty,
		on_hand_status,
		atp,
		status,
		future_qty,
		updated_date,
		src_load_ts as load_time_kafka,
		src_load_ts
	from {{ source('src_atp_silver', 'mao_location_atp_delta_v') }}
	{% if is_incremental() %}
        where src_load_ts::timestamp_ntz >= DATEADD(day, -1, CURRENT_DATE())
    {% endif %}
),
loc_atp_fullsync as (
	select
		item_id,
		location_id,
		selling_channel,
		message_type,
		transaction_type,
		region,
		timezone,
		country,
		on_hand_qty,
		on_hand_status,
		atp,
		status,
		future_qty,
		updated_date,
		src_load_ts as load_time_kafka,
		src_load_ts
	from {{ source('src_atp_silver', 'mao_location_atp_fullsync_v') }}
	{% if is_incremental() %}
        where src_load_ts::timestamp_ntz >= DATEADD(day, -1, CURRENT_DATE())
    {% endif %}
),
loc_atp_stg as (
	select * from loc_atp_delta
	union all
	select * from loc_atp_fullsync
),
loc_atp_main as (
    select
        src.*,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list)}} as hash_seq_num
    from
        loc_atp_stg as src
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
    loc_atp_main as src