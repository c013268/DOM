{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}

{{ config(
    materialized="incremental", 
    table_format='iceberg',
    unique_key=["pk","context_id"], 
    merge_exclude_columns=["etl_load_ts"], 
    post_hook=["{{ fl_utils.m_mysql_process_deletions_from_silver_nonframework_using_model_id(  var('p_pipeline_name'), 'dom_silver', source('ord_mgmt_bronze', 'ord_order_tax_detail_archive_v'), this, 'src_load_ts', fl_utils.m_get_batch_id( var('p_pipeline_name') ) ) }}",
            "{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge"}
) }}

with order_tax_detail_stg as (
	select 
		src.pk
		,src.order_pk
		,src.org_id
		,src.tax_engine_id
		,src.tax_detail_id
		,src.fulfillment_group_id
		,src.jurisdiction_type_id
		,src.tax_type_id
		,src.context_id
		,src.is_prorated
		,src.is_invoice_tax
		,src.is_informational
		,src.json_store
		,src.vat_tax_code
		,src.tax_identifier3
		,src.tax_amount
		,src.tax_identifier4
		,src.temp_timestamp1::timestamp_ntz as temp_timestamp1
		,src.tax_identifier1
		,src.tax_identifier2
		,src.process
		,src.tax_identifier5
		,src.temp_money_amount1
		,src.created_by
		,src.temp_id1
		,src.jurisdiction
		,src.temp_long_id1
		,src.taxable_amount
		,src.temp_date1
		,src.tax_rate_percent
		,src.updated_by
		,src.temp_decimal1
		,src.tax_rate
		,src.version
		,src.temp_boolean_false1
		,src.temp_integer1
		,src.tax_code
		,src.seq
		,src.updated_timestamp::timestamp_ntz as updated_timestamp
		,src.created_timestamp::timestamp_ntz as created_timestamp
		,src.purge_date::timestamp_ntz as purge_date
		,src.tax_date::timestamp_ntz as tax_date
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
		{{ source('ord_mgmt_bronze', 'ord_order_tax_detail_archive_v') }} src
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
from order_tax_detail_stg src
where src.rnk=1
