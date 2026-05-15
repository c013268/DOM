{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "org_id", "fulflmnt_id" ]  %}

{% set target_context_typ2_column_list = 
    [ "pickup_fulflmnt_id", "ord_org_id", "ord_type_id", "fulflmnt_type_id", "fulflmnt_reason_id", "fulflmnt_reason_type_id", "ship_from_loc_id", 
	"ship_to_loc_id", "crnt_loc_id", "ship_via_id", "cnl_reason_id", "cust_id", "dlvry_method_id", "pkg_id", "pkg_type_id", "dest_action_id", 
	"doc_type_id", "highest_priority_rule_id", "max_status_id", "min_status_id", "transfer_type_id", "is_verified", "is_v_a_s_reqd", "is_same_day_dlvry", 
	"is_r_o_p_i_s", "is_pickup_convert_to_shipment", "is_pick_to_slot", "is_non_parcel", "is_gift", "is_cust_on_the_way", "is_closed", "is_active_pick_task", "is_rejected_pkg", 
	"fulflmnt_reason_name", "fulflmnt_reason_desc", "fulflmnt_reason_type_sdesc", "fulflmnt_reason_type_name", "dlvry_type", "shpmnt_type", "shpmnt_method", 
	"pkg_status", "pkg_status_desc", "tracking_num", "dlvry_method_sub_type", "dlvry_status", "carrier_cd", "carrier_barcd", "ccy_code", 
	"service_level_cd", "ord_priority", "fulflmnt_source", "packed_by", "picked_by", "ord_total", "picked_up_value", "total_shorts_qty", "gross_wt_qty", "gross_wt_uom", 
	"gross_vol_qty", "gross_vol_uom", "ord_capture_dt", "estimated_dlvry_dt", "pickup_arrival_dt_time", "pickup_e_t_a_dt_time", "pickup_expiry_dt", "packed_dt_time", 
	"expected_dlvry_dt", "shipped_dt_time", "received_dt_time", "created_by", "created_ts", "updated_by", "updated_ts" ]  %}

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
					"{{  fl_utils.m_apply_typ2_deletion_on_tgt_model( source('src_ord_fulflmnt_silver','mao_ful_fulfillment_v'), this, 'fulflmnt_pk' ) }}",
					"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge", 'update_condition': "active_flg"}
) }}

with 
    mao_ful_fulfillment as (
        select fu.*    
        from {{ source('src_ord_fulflmnt_silver','mao_ful_fulfillment_v') }} fu
        {% if is_incremental() %}
            WHERE
                fu.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
    ),
    ful_status_stg as (
	    select * 
        from {{ source('src_ord_fulflmnt_silver','mao_ful_fw_status_definition_v') }} 
        where lower(profile_id)='fl-us' and lower(process_type_id) = 'fulfillment_execution'
	),
	reason_code as
	(
	select re.profile_id
		,re.fulfillment_reason_id
		,re.fulfillment_type_id
		,re.fulfillment_reason_type_id
		,re.fulfillment_reason_name
		,re.fulfillment_reason_desc
		,rt.fulfillment_reason_type_desc
		,rt.fulfillment_reason_type_name
	from {{ source('src_ord_fulflmnt_silver','mao_ful_fulfillment_reason_v') }} re
		left join {{ source('src_ord_fulflmnt_silver','mao_ful_fulfillment_reason_type_v') }} rt on re.fulfillment_reason_type_id = rt.fulfillment_reason_type_id
	where lower(re.profile_id)='fl-us'
	),
	package_details as
	(
	select ph.org_id,ph.fulfillment_id,ph.pickup_fulfillment_id,ph.current_location_id,ph.package_id,ph.package_type_id,
        ph.is_rejected_package,ph.delivery_type,ph.shipment_type,ph.package_status,ph.tracking_number,ph.delivery_status,
        ph.carrier_barcode,ph.service_level_code,ph.gross_weight_qty,ph.gross_weight_uom,ph.gross_volume_qty,ph.gross_volume_uom,
        ph.estimated_delivery_date,ph.packed_date_time,ph.shipped_date_time,ph.received_date_time,ph.src_load_ts, 
        ps.package_status_name, ps.package_status_desc
	from 
		(select *, 
		    row_number() over (
					partition by org_id,fulfillment_id
					order by src_load_ts desc
				) as rnk
            from {{ ref('mao_ful_packages_t') }} 
            {% if is_incremental() %}
                WHERE (org_id, fulfillment_id) IN (SELECT org_id, fulfillment_id FROM mao_ful_fulfillment)
            {% endif %}
        ) ph
		left join {{ source('src_ord_fulflmnt_silver','mao_ful_package_status_v') }} ps on ph.package_status = ps.package_status_id
    where ph.rnk=1
	),
	fulfillment_hdr as 
	(
	select 
		fu.pk as fulflmnt_pk	
		,fu.org_id as org_id
		,fu.fulfillment_id as fulflmnt_id
		,ph.pickup_fulfillment_id as pickup_fulflmnt_id
		,fu.order_org_id as ord_org_id
		,fu.order_type_id as ord_type_id
		,fu.delivery_method_id as fulflmnt_type_id
		,replace(re.fulfillment_reason_id,'.000','') as fulflmnt_reason_id
		,re.fulfillment_reason_name as fulflmnt_reason_name
		,re.fulfillment_reason_desc as fulflmnt_reason_desc
		,replace(re.fulfillment_reason_type_id,'.000','') as fulflmnt_reason_type_id
		,re.fulfillment_reason_type_desc as fulflmnt_reason_type_sdesc
		,re.fulfillment_reason_type_name as fulflmnt_reason_type_name	
		,fu.ship_from_location_id as ship_from_loc_id
		,fu.ship_to_location_id as ship_to_loc_id
		,ph.current_location_id as crnt_loc_id
		,fu.ship_via_id	as ship_via_id
		,fu.cancel_reason_id as cnl_reason_id
		,fu.customer_id as cust_id
		,fu.delivery_method_id as dlvry_method_id
		,ph.package_id as pkg_id
		,ph.package_type_id	 as pkg_type_id
		,fu.destination_action_id as dest_action_id
		,fu.doc_type_id as doc_type_id
		,fu.highest_priority_rule_id as highest_priority_rule_id
		,fu.min_status_id::decimal(38, 0) as min_status_id
		,st1.description as min_status_desc
		,fu.max_status_id::decimal(38, 0) as max_status_id
		,st2.description as max_status_desc
		,fu.transfer_type_id as transfer_type_id		
		,fu.is_verified as is_verified
		,fu.is_v_a_s_required as is_v_a_s_reqd
		,fu.is_same_day_delivery as is_same_day_dlvry
		,fu.is_r_o_p_i_s as  is_r_o_p_i_s
		,fu.is_pickup_convert_to_shipment as is_pickup_convert_to_shipment
		,fu.is_pick_to_slot as is_pick_to_slot
		,fu.is_non_parcel as is_non_parcel
		,fu.is_gift as is_gift
		,fu.is_customer_on_the_way as is_cust_on_the_way
		,fu.is_closed as is_closed
		,fu.is_active_pick_task as is_active_pick_task
		,ph.is_rejected_package as is_rejected_pkg	
		,ph.delivery_type as dlvry_type	
		,ph.shipment_type as shpmnt_type
		,fu.ext_flshipmethod as shpmnt_method
		,NULLIF(ph.package_status, '')::decimal(38, 0) as pkg_status
		,ph.package_status_name as pkg_status_desc
		,ph.tracking_number as tracking_num 
		,fu.delivery_method_sub_type as dlvry_method_sub_type
		,ph.delivery_status as dlvry_status
		,fu.carrier_code as carrier_cd
		,ph.carrier_barcode as carrier_barcd
		,fu.currency_code as ccy_code	
		,ph.service_level_code as service_level_cd
		,fu.order_priority as ord_priority
		,fu.source as fulflmnt_source
		,fu.packed_by as packed_by
		,fu.picked_by as picked_by
		,fu.order_total as ord_total
		,fu.pickedup_value as picked_up_value
		,fu.total_shorts_quantity as total_shorts_qty
		,ph.gross_weight_qty as gross_wt_qty
		,ph.gross_weight_uom as gross_wt_uom
		,ph.gross_volume_qty as gross_vol_qty
		,ph.gross_volume_uom as gross_vol_uom	
		,fu.order_capture_date::timestamp_ntz as ord_capture_dt
		,ph.estimated_delivery_date::timestamp_ntz as estimated_dlvry_dt
		,fu.pickup_arrival_date_time::timestamp_ntz as pickup_arrival_dt_time
		,fu.pickup_e_t_a_date_time::timestamp_ntz as pickup_e_t_a_dt_time
		,fu.pickup_expiry_date::timestamp_ntz as pickup_expiry_dt
		,ph.packed_date_time::timestamp_ntz as packed_dt_time
		,fu.expected_delivery_date::timestamp_ntz as expected_dlvry_dt
		,ph.shipped_date_time::timestamp_ntz as shipped_dt_time
		,ph.received_date_time::timestamp_ntz as received_dt_time
		,fu.created_by as created_by
		,fu.created_timestamp::timestamp_ntz as created_ts
		,fu.updated_by as updated_by
		,fu.updated_timestamp::timestamp_ntz as updated_ts
		,fu.src_load_ts as src_load_ts
	from mao_ful_fulfillment fu
	    left join package_details ph on fu.org_id=ph.org_id and fu.fulfillment_id = ph.fulfillment_id 
		left join reason_code re on fu.cancel_reason_id = re.fulfillment_reason_id
		left join ful_status_stg st1 on (fu.min_status_id=st1.status)
		left join ful_status_stg st2 on (fu.max_status_id=st2.status)
	),
	fulfillment_hdr_hash as (
    select
        src.*,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list)}} as hash_seq_num
    from
        fulfillment_hdr as src
	)
select
    src.*,
    current_timestamp()::timestamp_ntz as start_ts,
    null::timestamp_ntz as end_ts,
    'Y'::varchar(1) as active_flg,
    'Y'::varchar(1) as reporting_flg,
    {{ v_batch_id }}::number(38, 0) as batch_id,
    current_timestamp()::timestamp_ntz as etl_load_ts,
    current_timestamp()::timestamp_ntz as etl_updt_ts
from
    fulfillment_hdr_hash as src