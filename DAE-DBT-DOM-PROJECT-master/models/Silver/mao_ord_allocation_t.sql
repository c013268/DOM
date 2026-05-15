{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}

{{ config(
    materialized="incremental", 
    table_format='iceberg',
    unique_key=["pk", "status_id","quantity","ship_from_location_id","item_id"], 
    merge_exclude_columns=["etl_load_ts"], 
    post_hook=["{{ fl_utils.m_mysql_process_deletions_from_silver_nonframework_using_model_id(  var('p_pipeline_name'), 'dom_silver', source('ord_mgmt_bronze', 'ord_allocation_archive_v'), this, 'src_load_ts', fl_utils.m_get_batch_id( var('p_pipeline_name') ) ) }}",
            "{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge"}
) }}

with alloc_stg as (
	select
		src.pk
		,src.order_line_pk
		,src.org_id
		,src.substitution_type_id
		,src.allocation_id
		,src.ship_to_location_id
		,src.item_id
		,src.group_id
		,nvl(src.status_id,'') as status_id
		,src.inventory_type_id
		,src.reservation_request_id
		,src.allocation_dependency_id
		,src.inventory_segment_id
		,src.po_id
		,src.po_detail_id
		,src.asn_detail_id
		,src.ship_via_id
		,src.ship_from_location_id
		,src.product_status_id
		,src.context_id
		,src.reservation_request_detail_id
		,src.asn_id
		,src.is_virtual
		,src.quantity
		,src.process
		,src.service_level_code
		,src.batch_number
		,src.created_by
		,src.country_of_origin
		,src.updated_by
		,src.latest_release_date_oh_rdd::timestamp_ntz as latest_release_date_oh_rdd
		,src.allocation_type
		,src.json_store
		,src.location_backlog
		,src.allocated_on::timestamp_ntz as allocated_on
		,src.substitution_ratio
		,src.inventory_attribute5
		,src.uom
		,src.inventory_attribute1
		,src.inventory_attribute2
		,src.inventory_attribute3
		,src.inventory_attribute4
		,src.version
		,src.carrier_code
		,src.seq
		,src.updated_timestamp::timestamp_ntz as updated_timestamp
		,src.latest_ship_date::timestamp_ntz as latest_ship_date
		,src.predicted_delivery_date::timestamp_ntz as predicted_delivery_date
		,src.heuristic_ship_date::timestamp_ntz as heuristic_ship_date
		,src.purge_date::timestamp_ntz as purge_date
		,src.earliest_ship_date::timestamp_ntz as earliest_ship_date
		,src.release_date::timestamp_ntz as release_date
		,src.heuristic_delivery_date::timestamp_ntz as heuristic_delivery_date
		,src.committed_delivery_date::timestamp_ntz as committed_delivery_date
		,src.latest_release_date::timestamp_ntz as latest_release_date
		,src.earliest_delivery_date::timestamp_ntz as earliest_delivery_date
		,src.created_timestamp::timestamp_ntz as created_timestamp
		,src.predicted_ship_date::timestamp_ntz as predicted_ship_date
		,src.committed_ship_date::timestamp_ntz as committed_ship_date
		,src.primary_key__pk
		,src.event_type
		,src.most_significant_position
		,src.least_significant_position
		,src.seen_at
		,src.sf_metadata
		,src.src_journal_tbl
		,src.updated_timestamp::timestamp_ntz as src_load_ts
		,row_number() over (
					partition by src.pk, nvl(src.status_id,'')
					order by src.seen_at desc, src.least_significant_position desc, src.etl_updt_ts desc
				) as rnk
	from
		{{ source('ord_mgmt_bronze', 'ord_allocation_archive_v') }} src
	where
		lower(src.event_type)!='incrementaldeleterows'
		{% if is_incremental() %}
			and src.src_load_ts >= {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL_SILVER") | as_text }}'
		{% endif %}
	)
select
	src.*,
    {{ v_batch_id }}::number(38, 0) as batch_id,
    current_timestamp()::timestamp_ntz as etl_load_ts,
    current_timestamp()::timestamp_ntz as etl_updt_ts
from alloc_stg src
where src.rnk=1