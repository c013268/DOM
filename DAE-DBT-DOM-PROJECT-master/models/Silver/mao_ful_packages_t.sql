{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}

{{ config(
    materialized="incremental", 
    table_format='iceberg',
    unique_key=["pk", "package_status"], 
    merge_exclude_columns=["etl_load_ts"], 
    post_hook=["{{ fl_utils.m_mysql_process_deletions_from_silver_nonframework_using_model_id(  var('p_pipeline_name'), 'dom_silver', source('ord_fulflmnt_bronze', 'ful_packages_archive_v'), this, 'src_load_ts', fl_utils.m_get_batch_id( var('p_pipeline_name') ) ) }}",
            "{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge"}
) }}

with pkg_stg as (
	select
		src.pk
		,src.org_id
		,src.task_group_id
		,src.ship_to_location_id
		,src.package_type_id
		,src.task_id
		,src.shipment_id
		,src.fulfillment_id
		,src.ship_via_id
		,src.ship_from_location_id
		,src.current_location_id
		,src.order_id
		,src.context_id
		,src.pickup_fulfillment_id
		,src.package_id
		,src.cubed_package_id
		,src.is_rejected_package
		,src.return_tracking_number
		,src.late_shipment
		,src.gross_weight_qty
		,src.delivery_type
		,src.receipt_type
		,src.packed_date_time::timestamp_ntz as packed_date_time
		,src.process
		,src.service_level_code
		,src.type
		,src.received_date_time::timestamp_ntz as received_date_time
		,nvl(src.package_status,'') as package_status
		,src.created_by
		,src.ship_to_address_email
		,src.ship_to_address_phone
		,src.updated_by
		,src.tracking_number
		,src.previous_receipt_type
		,src.ship_to_address_postalcode
		,src.contains_hazmat
		,src.packed_by
		,src.ship_to_address_country
		,src.gross_volume_uom
		,src.delivery_status
		,src.ship_to_address_lastname
		,src.shipped_date_time::timestamp_ntz as shipped_date_time
		,src.gross_weight_uom
		,src.ship_to_address_city
		,src.ship_to_address_address1
		,src.ship_to_address_county
		,src.ship_to_address_address3
		,src.ship_to_address_address2
		,src.version
		,src.carrier_code
		,src.carrier_barcode
		,src.ship_to_address_firstname
		,src.shipment_type
		,src.gross_volume_qty
		,src.ship_to_address_state
		,src.seq
		,src.json_store
		,src.ext_test_attr
		,src.ext_packageerror
		,src.updated_timestamp::timestamp_ntz as updated_timestamp
		,src.estimated_delivery_date::timestamp_ntz as estimated_delivery_date
		,src.purge_date::timestamp_ntz as purge_date
		,src.created_timestamp::timestamp_ntz as created_timestamp
		,src.event_type
		,src.most_significant_position
		,src.least_significant_position
		,src.seen_at
		,src.sf_metadata
		,src.src_journal_tbl
		,src.updated_timestamp::timestamp_ntz as src_load_ts,
		row_number() over (
					partition by src.pk, nvl(src.package_status,'')
					order by src.seen_at desc, src.least_significant_position desc, src.etl_updt_ts desc
				) as rnk
	from
		{{ source('ord_fulflmnt_bronze', 'ful_packages_archive_v') }} src
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
from pkg_stg src
where src.rnk=1
