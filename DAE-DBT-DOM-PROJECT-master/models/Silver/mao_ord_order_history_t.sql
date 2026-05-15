{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}

{{ config(
    materialized="incremental",
    table_format='iceberg',
    unique_key=["pk", "max_fulfillment_status_id"], 
    merge_exclude_columns=["etl_load_ts"], 
    post_hook=["{{ fl_utils.m_mysql_process_deletions_from_silver_nonframework_using_model_id(  var('p_pipeline_name'), 'dom_silver', source('ord_mgmt_bronze', 'ord_order_archive_v'), this, 'src_load_ts', fl_utils.m_get_batch_id( var('p_pipeline_name') ) ) }}",
            "{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'format': "iceberg", 'strategy': "merge"}
) }}

with order_line_stg as (
	select 
		src.pk
		,src.org_id
		,src.publish_status_id
		,src.tax_exempt_id
		,src.max_return_status_id
		,src.customer_type_id
		,nvl(src.max_fulfillment_status_id,'') as max_fulfillment_status_id
		,src.min_fulfillment_status_id
		,src.order_type_id
		,src.tax_override_reason_id
		,src.cancel_reason_id
		,src.selling_location_id
		,src.customer_id
		,src.run_id
		,src.tax_exempt_reason_id
		,src.min_return_status_id
		,src.doc_type_id
		,src.selling_channel_id
		,src.parent_reservation_request_id
		,src.post_void_reason_id
		,src.payment_status_id
		,src.suspended_order_id
		,src.alternate_order_id
		,src.order_id
		,src.context_id
		,src.is_on_hold
		,src.is_ready_for_tender
		,src.is_confirmed
		,src.is_un_archive_in_progress
		,src.is_captured_offline
		,src.is_order_countable
		,src.is_tax_exempt
		,src.is_post_voided
		,src.is_cancelled
		,src.is_tax_overridden
		,src.is_archive_in_progress
		,src.refund_payment_method
		,src.liability_amount
		,src.customer_address_address1
		,src.customer_address_address3
		,src.customer_address_address2
		,src.order_token
		,src.temp_timestamp1::timestamp_ntz as temp_timestamp1
		,src.next_event_time::timestamp_ntz as next_event_time
		,src.return_line_count
		,src.process
		,src.total_taxes
		,src.do_not_release_before::timestamp_ntz as do_not_release_before
		,src.customer_address_city
		,src.customer_address_phone
		,src.customer_phone
		,src.tax_override_type
		,src.temp_long_id1
		,src.temp_long_id2
		,src.temp_date1::timestamp_ntz as temp_date1
		,src.process_return_comments
		,src.merch_return_line_count
		,src.updated_by
		,src.customer_email
		,src.total_charges
		,src.process_return_reason
		,src.temp_integer3
		,src.customer_signature
		,src.json_store
		,src.customer_address_country
		,src.order_total
		,src.order_locale
		,src.store_sale_count
		,src.cancelled_order_total
		,src.tax_override_value
		,src.customer_first_name
		,src.order_sub_total
		,src.total_discounts
		,src.temp_id1
		,src.temp_id3
		,src.temp_id2
		,src.temp_text4
		,src.temp_text5
		,src.salted_order_token
		,src.temp_text2
		,src.temp_text3
		,src.customer_address_state
		,src.temp_decimal2
		,src.merch_sale_line_count
		,src.temp_decimal1
		,src.version
		,src.temp_boolean_false2
		,src.temp_boolean_false1
		,src.temp_boolean_false4
		,src.temp_boolean_false3
		,src.temp_integer1
		,src.temp_boolean_false5
		,src.temp_integer2
		,src.loyalty_number
		,src.customer_last_name
		,src.priority
		,src.store_return_count
		,src.created_by
		,src.order_line_count
		,src.customer_address_postalcode
		,src.cancel_comments
		,src.return_label_email
		,src.package_count
		,src.customer_address_firstname
		,src.customer_address_lastname
		,src.customer_address_email
		,src.temp_money_amount2
		,src.temp_money_amount3
		,src.currency_code
		,src.cancelled_order_sub_total
		,src.confirmed_order_total
		,src.tax_ovrd_perc_value
		,src.customer_address_county
		,src.temp_money_amount1
		,src.cancelled_total_discounts
		,src.tax_exempt_comments
		,src.refund_recipient
		,src.cancel_line_count
		,src.event_submit_time::timestamp_ntz as event_submit_time
		,src.pay_by_link_status
		,src.ext_return_auth_no
		,src.ext_totalchargeback
		,src.ext_totalchargebackconsumed
		,src.ext_terminalid
		,src.customer_category
		,src.ext_revert_to_customer_refund
		,src.ext_is_fraudcheck_eligible
		,src.ext_is_fraudservice_failed
		,src.ext_fraud_retry_count
		,src.ext_storemerchantid
		,src.ext_storeorderrequestid
		,src.ext_storeinvoiceid
		,src.purge_date::timestamp_ntz as purge_date
		,src.captured_date::timestamp_ntz as captured_date
		,src.version_timestamp
		,src.archive_date::timestamp_ntz as archive_date
		,src.confirmed_date::timestamp_ntz as confirmed_date
		,src.updated_timestamp::timestamp_ntz as updated_timestamp
		,src.business_date::timestamp_ntz as business_date
		,src.created_timestamp::timestamp_ntz as created_timestamp
		,src.counted_date::timestamp_ntz as counted_date
		,src.ext_not_after_date::timestamp_ntz as ext_not_after_date
		,src.primary_key__pk
		,src.event_type
		,src.most_significant_position
		,src.least_significant_position
		,src.seen_at
		,src.sf_metadata
		,src.src_journal_tbl
		,src.updated_timestamp::timestamp_ntz as src_load_ts
		,row_number() over (
					partition by src.pk, nvl(src.max_fulfillment_status_id,'')
					order by src.seen_at desc, src.least_significant_position desc, src.etl_updt_ts desc
				) as rnk
	from
		{{ source('ord_mgmt_bronze', 'ord_order_archive_v') }} src
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
from order_line_stg src
where src.rnk=1