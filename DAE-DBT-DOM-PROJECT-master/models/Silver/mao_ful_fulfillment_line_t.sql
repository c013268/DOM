{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}

{{ config(
    materialized="incremental", 
    table_format='iceberg',
    unique_key=["pk", "fulfillment_line_status_id"], 
    merge_exclude_columns=["etl_load_ts"], 
    post_hook=["{{ fl_utils.m_mysql_process_deletions_from_silver_nonframework_using_model_id(  var('p_pipeline_name'), 'dom_silver', source('ord_fulflmnt_bronze', 'ful_fulfillment_line_archive_v'), this, 'src_load_ts', fl_utils.m_get_batch_id( var('p_pipeline_name') ) ) }}",
            "{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge"}
) }}

with fulflmnt_line_stg as (
	select
		src.pk
		,src.fulfillment_pk
		,src.org_id
        ,nvl(fulfillment_line_status_id::number(38,0)::string,'') as fulfillment_line_status_id
		,src.order_line_id
		,src.cancel_reason_id
		,src.pipeline_id
		,src.release_id
		,src.supply_type_id
		,src.allocation_id
		,src.item_id
		,src.fulfillment_line_id
		,src.order_id
		,src.context_id
		,src.release_line_id
		,src.asn_id
		,src.substituted_item_id
		,src.parent_item_id
		,src.is_gift
		,src.is_hazmat
		,src.is_serial_number_required
		,src.is_substitution_allowed
		,src.shipped_qty
		,src.item_unit_price
		,src.item_style
		,src.returned_to_shelf
		,src.item_description
		,src.packed_qty
		,src.cancelled_qty
		,src.received_qty
		,src.picked_qty
		,src.item_size
		,src.sorted_qty
		,src.process
		,src.item_unit_weight
		,src.item_color
		,src.created_by
		,src.item_small_image_u_r_i
		,src.quantity_uom
		,src.weight_u_o_m
		,src.serial_numbers
		,src.item_upc
		,src.updated_by
		,src.version
		,src.line_short_count
		,src.store_department
		,src.ordered_qty
		,src.original_picked_qty
		,src.seq
		,src.fulfillment_substitution_type
		,src.original_item_picked_qty
		,src.allowed_substitution_on_u_i
		,src.original_unit_price
		,src.json_store
		,src.updated_timestamp::timestamp_ntz as updated_timestamp
		,src.created_timestamp::timestamp_ntz as created_timestamp
		,src.purge_date::timestamp_ntz as purge_date
		,src.primary_key__pk
		,src.event_type
		,src.most_significant_position
		,src.least_significant_position
		,src.seen_at::timestamp_ntz as seen_at
		,src.sf_metadata
		,src.src_journal_tbl
		,src.updated_timestamp::timestamp_ntz as src_load_ts
		,row_number() over (
					partition by src.pk, nvl(src.fulfillment_line_status_id::number(38,0)::string,'')
					order by src.seen_at desc, src.least_significant_position desc, src.etl_updt_ts desc
				) as rnk
	from
		{{ source('ord_fulflmnt_bronze', 'ful_fulfillment_line_archive_v') }} src
	where
		lower(src.event_type)!='incrementaldeleterows'
        and src.fulfillment_pk is not null
		{% if is_incremental() %}
			and src.src_load_ts >= {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL_SILVER") | as_text }}'
		{% endif %}
	)
select
	src.*,
    {{ v_batch_id }}::number(38, 0) as batch_id,
    current_timestamp()::timestamp_ntz as etl_load_ts,
    current_timestamp()::timestamp_ntz as etl_updt_ts
from fulflmnt_line_stg src
where src.rnk=1