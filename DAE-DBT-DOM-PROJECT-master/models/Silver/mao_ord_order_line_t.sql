{% set v_batch_id = fl_utils.m_get_batch_id(var("p_pipeline_name")) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , v_batch_id ) %}

{{ config(
    materialized="incremental", 
    table_format='iceberg',
    unique_key=["pk", "max_fulfillment_status_id"], 
    merge_exclude_columns=["etl_load_ts"], 
    post_hook=["{{ fl_utils.m_mysql_process_deletions_from_silver_nonframework_using_model_id(  var('p_pipeline_name'), 'dom_silver', source('ord_mgmt_bronze', 'ord_order_line_archive_v'), this, 'src_load_ts', fl_utils.m_get_batch_id( var('p_pipeline_name') ) ) }}",
            "{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_silver', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge"}
) }}

with order_line_stg as (
	select 
		src.pk
		,src.order_pk
		,src.org_id
		,src.pipeline_id
		,src.release_group_id
		,nvl(src.max_fulfillment_status_id,'') as max_fulfillment_status_id
		,src.fulfillment_group_id
		,src.item_id
		,src.shipping_method_id
		,src.min_fulfillment_status_id
		,src.alternate_order_line_id
		,src.physical_origin_id
		,src.split_from_line_id
		,src.parent_order_line_type_id
		,src.order_line_id
		,src.payment_group_id
		,src.cancel_reason_id
		,src.ship_to_location_id
		,src.delivery_method_id
		,src.selling_location_id
		,src.parent_order_line_id
		,src.store_sale_entry_method_id
		,src.line_type_id
		,src.ship_from_address_id
		,src.allocation_config_id
		,src.transaction_reference_id
		,src.parent_order_id
		,src.order_id
		,src.ship_to_address_id
		,src.context_id
		,src.original_item_id
		,src.is_gift_card
		,src.is_return
		,src.is_on_hold
		,src.is_hazmat
		,src.is_pre_sale
		,src.is_returnable_at_store
		,src.is_price_overrideable
		,src.is_receipt_expected
		,src.is_gift
		,src.is_non_merchandise
		,src.is_exchangeable
		,src.is_pre_order
		,src.is_item_tax_overridable
		,src.is_price_overridden
		,src.is_returnable
		,src.is_refund_gift_card
		,src.is_pack_and_hold
		,src.is_item_tax_exemptable
		,src.is_cancelled
		,src.is_tax_overridden
		,src.is_activation_required
		,src.is_even_exchange
		,src.is_item_not_on_file
		,src.is_perishable
		,src.is_discountable
		,src.is_tax_included
		,src.is_recommended_item
		,src.item_brand
		,src.original_unit_price
		,src.unit_price
		,src.volumetric_weight_uom_code
		,src.next_event_time::timestamp_ntz as next_event_time
		,src.process
		,src.total_taxes
		,src.service_level_code
		,src.item_department_name
		,src.do_not_release_before::timestamp_ntz as do_not_release_before
		,src.tax_override_type
		,src.temp_long_id1
		,src.temp_long_id2
		,src.temp_date1
		,src.small_image_u_r_i
		,src.updated_by
		,src.gift_card_value
		,src.total_charges
		,src.item_department_number
		,src.cancelled_order_line_sub_total
		,src.temp_integer3
		,src.json_store
		,src.item_style
		,src.tax_override_value
		,src.item_max_discount_percentage
		,src.total_discounts
		,src.temp_id1
		,src.refund_price
		,src.temp_id3
		,src.temp_id2
		,src.temp_text4
		,src.item_tax_code
		,src.return_type
		,src.temp_text5
		,src.temp_text2
		,src.cancellation_request_status
		,src.temp_text3
		,src.temp_decimal2
		,src.order_line_sub_total
		,src.temp_decimal1
		,src.version
		,src.carrier_code
		,src.item_color_description
		,src.temp_boolean_false4
		,src.temp_boolean_false3
		,src.item_max_discount_amount
		,src.temp_boolean_false5
		,src.temp_integer2
		,src.quantity
		,src.item_dept_number
		,src.priority
		,src.item_description
		,src.item_size
		,src.can_ship_to_address
		,src.created_by
		,src.has_components
		,src.cancel_comments
		,src.delivery_method_sub_type
		,src.item_barcode
		,src.line_short_count
		,src.temp_money_amount2
		,src.temp_money_amount3
		,src.total_discount_on_item
		,src.effective_rank
		,src.item_web_u_r_l
		,src.volumetric_weight
		,src.tax_ovrd_perc_value
		,src.temp_money_amount1
		,src.hazmat_code
		,src.cancelled_total_discounts
		,src.item_short_description
		,src.order_line_total
		,src.value_entry_required
		,src.uom
		,src.item_season
		,src.product_class
		,src.seq
		,src.fulfillment_sub_type
		,src.ext_ispresell
		,src.return_eligibility_days
		,src.promised_ship_date::timestamp_ntz as promised_ship_date
		,src.purge_date::timestamp_ntz as purge_date
		,src.parent_line_created_timestamp::timestamp_ntz as parent_line_created_timestamp
		,src.updated_timestamp::timestamp_ntz as updated_timestamp
		,src.promised_delivery_date::timestamp_ntz as promised_delivery_date
		,src.do_not_ship_before_date::timestamp_ntz as do_not_ship_before_date
		,src.business_date::timestamp_ntz as business_date
		,src.latest_delivery_date::timestamp_ntz as latest_delivery_date
		,src.created_timestamp::timestamp_ntz as created_timestamp
		,src.street_date::timestamp_ntz as street_date
        ,src.requested_delivery_date::timestamp_ntz as requested_delivery_date
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
		{{ source('ord_mgmt_bronze', 'ord_order_line_archive_v') }} src
	where
		lower(src.event_type)!='incrementaldeleterows'
		and src.order_pk is not null
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