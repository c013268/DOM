{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}

{{ config(
    materialized="incremental", 
    table_format='iceberg',
    unique_key=["pk","context_id"], 
    merge_exclude_columns=["etl_load_ts"], 
    post_hook=["{{ fl_utils.m_mysql_process_deletions_from_silver_nonframework_using_model_id(  var('p_pipeline_name'), 'dom_silver', source('ord_mgmt_bronze', 'ord_cancel_line_charge_detail_archive_v'), this, 'src_load_ts', fl_utils.m_get_batch_id( var('p_pipeline_name') ) ) }}",
            "{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge"}
) }}

with cancel_line_charge_detail_stg as (
	select 
		src.pk
		,src.order_line_pk
		,src.org_id
		,src.reason_id
		,src.parent_charge_detail_id
		,src.charge_reference_detail_id
		,src.related_order_line_id
		,src.charge_type_id
		,src.header_charge_detail_id
		,src.fulfillment_group_id
		,src.charge_detail_id
		,src.context_id
		,src.related_charge_detail_id
		,src.charge_reference_id
		,src.is_overridden
		,src.is_copied
		,src.is_post_return
		,src.is_line_discount
		,src.is_informational
		,src.is_prorated_at_same_level
		,src.is_tax_included
		,src.is_copied_header_charge
		,src.is_return_charge
		,src.charge_sequence
		,src.requested_amount
		,src.discount_on
		,src.comments
		,src.charge_display_name
		,src.process
		,src.original_charge_amount
		,src.charge_total
		,src.unit_charge
		,src.created_by
		,src.charge_sub_type
		,src.related_charge_type
		,src.charge_percent
		,src.updated_by
		,src.version
		,src.tax_code
		,src.seq
		,src.json_store
		,src.updated_timestamp::timestamp_ntz as updated_timestamp
		,src.created_timestamp::timestamp_ntz as created_timestamp
		,src.purge_date::timestamp_ntz as purge_date
		,src.primary_key__pk
		,src.event_type
		,src.most_significant_position
		,src.least_significant_position
		,src.seen_at
		,src.sf_metadata
		,src.src_journal_tbl
		,src.updated_timestamp::timestamp_ntz as src_load_ts
		,row_number() over (
					partition by src.pk,src.context_id
					order by src.seen_at desc, src.least_significant_position desc, src.etl_updt_ts desc
				) as rnk
	from
		{{ source('ord_mgmt_bronze', 'ord_cancel_line_charge_detail_archive_v') }} src
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
from cancel_line_charge_detail_stg src
where src.rnk=1
