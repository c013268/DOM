{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "org_id", "ord_id", "ord_ln_id"]  %}
	
{% set target_context_typ2_column_list = 
    [ "prnt_ord_id", "prnt_ord_ln_id", "item_id", "min_fulflmnt_status_id", "max_fulflmnt_status_id", "cnl_reason_id", "cnl_reason_desc","physical_org_id", "pipeline_id", 
	"dlvry_method_id", "fulflmnt_group_id", "orig_item_id", "prnt_ord_ln_type_id", "pymt_grp_id", "selling_loc_id", "alloc_id", "resrv_req_id", "ship_via_id", "ship_from_loc_id", 
	"ship_from_loc_type", "ship_to_loc_id", "ship_to_loc_type", "rtn_loc_id", "ship_method_id", "split_from_ln_id", "tax_cd", "str_sale_entry_method_id", "txn_ref_id", "loyalty_reward_id", 
	"ship_to_addr_id","ship_to_addr_postal_cd","ship_to_addr_last_name","ship_to_addr_first_name","ship_to_addr_name","ship_to_addr_email","ship_to_addr_addr1","ship_to_addr_addr2",
	"ship_to_addr_addr3","ship_to_addr_phone","ship_to_addr_country","ship_to_addr_state","ship_to_addr_county","ship_to_addr_city","is_ship_to_addr_verified",	"is_activation_reqd", 
	"is_cnlled", "is_discable", "is_even_exchg", "is_exchgable", "is_gift", "is_gift_card", "is_item_not_on_file", "is_item_tax_exempt", "is_item_tax_overridable", "is_non_merchandise", 
	"is_on_hold", "is_pk_and_hold", "is_pre_ord", "is_pre_sale", "is_price_overridden", "is_price_overrideable", "is_recommended_item", "is_refund_gift_card", "is_rtn", "is_rtnable", 
	"is_rtnable_at_d_c", "is_rtnable_at_store", "is_tax_included", "is_free_shipping", "is_appeasement", "is_loyalty_disc", "is_backorderFlg", "is_launch_sku_flg", "is_base_shipping_charged", 
	"is_restockable", "rush_flg", "ord_coupons", "rtn_fee_info", "is_tax_overridden", "can_ship_to_addr", "cnl_comments", "cnl_request_status", "carrier_cd", "dlvry_method_sub_type", 
	"eff_rank", "fulflmnt_sub_type", "item_bar_cd", "rtn_type", "service_level_cd", "tax_override_type", "uom", "tracking_num", "pkg_status", "itm_desc", "itm_short_desc", "itm_size", 
	"itm_color_desc", "itm_style", "itm_tax_cd", "itm_brand", "small_image_u_r_i", "itm_web_u_r_l", "itm_dept_name", "color_image_uri", "cart_shpmnt_method", "product_id", "product_type", 
	"shpmnt_method", "price_Override_Reason", "rtn_reason", "appeasment_reason_cd", "relate_ord_num_csa", "ord_note", "value_entry_reqd", "volumetric_weight", "pkg_cnt", "orig_unit_price", "unit_price", 
	"cnlled_ord_disc_amt","cnlled_ord_promo_amt","cnlled_ord_coupon_amt","cnlled_ord_shipping_amt","cnlled_ord_shipping_tax_amt","cnlled_ord_sales_tax_amt","cnlled_orig_ord_shipping_amt",
	"cnlled_orig_ord_shipping_tax_amt","cnlled_orig_ord_sales_tax_amt","cnlled_ord_ln_sub_total","cnlled_ord_ln_total","cnlled_total_charges","cnlled_total_disc","cnlled_total_taxes",
	"gift_card_value","item_max_discount_amt","ord_ln_sub_total","ord_ln_total","total_disc_on_item","total_charges","total_disc","total_taxes","qty","refund_price","ord_appeasement_amt",
	"ord_rtn_fee_amt","ord_disc_amt","ord_promo_amt","ord_coupon_amt","ord_shipping_amt","ord_shipping_tax_amt","ord_sales_tax_amt","orig_ord_shipping_amt","orig_ord_shipping_tax_amt",
	"orig_ord_sales_tax_amt","hdr_loyalty_disc_amt", "orig_line_sub_total", "orig_ord_qty", "orig_total_tax", "orig_disc_amt","pro_rated_disc_total","business_dt", "do_not_ship_before_dt", 
	"latest_dlvry_dt", "promised_dlvry_dt", "promised_ship_dt", "requested_dlvry_dt", "original_est_dlvry_ts", "estimated_dlvry_ts", "scheduled_ts", "shipped_ts", "delivered_ts", "created_by", 
	"created_ts", "updated_by", "updated_ts" ]  %}	
	

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
					"{{ fl_utils.m_apply_typ2_deletion_on_tgt_model( ref('mao_ord_order_line_t'), this, 'ord_ln_pk' ) }}",
					"{{ fl_utils.m_upd_typ2_status_hist_flg_records( this ,'hash_sk' ,'hash_seq_num', 'max_fulflmnt_status_id') }}",
					"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'format': "iceberg", 'strategy': "merge", 'update_condition': "active_flg"}
) }}

with
    mao_ord_order_line as (
        select
            ol.pk,
            ol.order_pk,
            ol.org_id,
            ol.order_id,
            ol.order_line_id,
            ol.parent_order_id,
            ol.parent_order_line_id,
            ol.item_id,
            ol.min_fulfillment_status_id::decimal(38, 0) as min_fulfillment_status_id,
            case when ol.max_fulfillment_status_id != '' then ol.max_fulfillment_status_id::decimal(38, 0) end as max_fulfillment_status_id,
            ol.physical_origin_id,
            ol.pipeline_id,
            ol.delivery_method_id,
            ol.fulfillment_group_id,
            ol.original_item_id,
            ol.parent_order_line_type_id,
            ol.payment_group_id,
            ol.selling_location_id,
            ol.ship_to_location_id,
            ol.ship_to_address_id,
            ol.shipping_method_id,
            ol.split_from_line_id,
            ol.store_sale_entry_method_id,
            ol.transaction_reference_id,
            ol.is_activation_required,
            ol.is_cancelled,
            ol.is_discountable,
            ol.is_even_exchange,
            ol.is_exchangeable,
            ol.is_gift,
            ol.is_gift_card,
            ol.is_item_not_on_file,
            ol.is_item_tax_exemptable,
            ol.is_item_tax_overridable,
            ol.is_non_merchandise,
            ol.is_on_hold,
            ol.is_pack_and_hold,
            ol.is_pre_order,
            ol.is_pre_sale,
            ol.is_price_overridden,
            ol.is_price_overrideable,
            ol.is_recommended_item,
            ol.is_refund_gift_card,
            ol.is_return,
            ol.is_returnable,
            ol.is_tax_included,
            ol.is_tax_overridden,
            ol.can_ship_to_address,
            ol.cancel_comments,
            ol.cancellation_request_status,
            ol.carrier_code,
            ol.delivery_method_sub_type,
            ol.effective_rank,
            ol.fulfillment_sub_type,
            ol.item_barcode,
            ol.return_type,
            ol.service_level_code,
            ol.tax_override_type,
            ol.uom,
            ol.item_description,
            ol.item_short_description,
            ol.item_size,
            ol.item_color_description,
            ol.item_style,
            ol.item_tax_code,
            ol.item_brand,
            ol.small_image_u_r_i,
            ol.item_web_u_r_l,
            ol.item_department_name,
            ol.value_entry_required,
            ol.volumetric_weight,
            ol.unit_price,
            ol.cancelled_order_line_sub_total,
            ol.cancelled_total_discounts,
            ol.gift_card_value,
            ol.item_max_discount_amount,
            ol.order_line_sub_total,
            ol.order_line_total,
            ol.total_discount_on_item,
            ol.total_charges,
            ol.total_discounts,
            ol.total_taxes,
            ol.quantity,
            ol.refund_price,
            ol.business_date,
            ol.do_not_ship_before_date,
            ol.latest_delivery_date,
            ol.promised_delivery_date,
            ol.promised_ship_date,
            ol.requested_delivery_date,
            ol.created_by,
            ol.created_timestamp,
            ol.updated_by,
            ol.updated_timestamp,
            ol.src_load_ts,
            parse_json(ol.json_store):"Fields" as json_fields,
            row_number() over (partition by ol.org_id, ol.order_id, ol.order_line_id order by ol.updated_timestamp desc) as ord_ln_pk
        from {{ ref('mao_ord_order_line_t') }} ol
        where ol.order_id is not null
        {% if is_incremental() %}
            and ol.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
    ),
    line_charge_detail as (
        select org_id, order_line_pk, charge_type_id, charge_total, requested_amount,
            original_charge_amount, charge_reference_detail_id, charge_reference_id,
            charge_display_name, charge_detail_id, reason_id, tax_code, discount_on,
            parent_charge_detail_id, process, updated_timestamp
        from {{ ref('mao_ord_order_line_charge_detail_hist_t') }}
        where lower(charge_type_id) in ('shipping', 'appeasement', 'promotion', 'coupon', 'return fee','discount')
    ),
    line_charge_agg as (
        select
            org_id,
            order_line_pk,
            sum(case when lower(charge_type_id) = 'shipping' and process not ilike 'woolin02riskifiedintegration%' then charge_total end) as ship_charge_total,
            sum(case when lower(charge_type_id) = 'shipping' and process not ilike 'woolin02riskifiedintegration%' then original_charge_amount end) as ship_original_charge_amount,
            sum(case when lower(charge_type_id) = 'shipping' and process ilike 'woolin02riskifiedintegration%' then charge_total end) as orig_ship_charge_total,
            sum(case when lower(charge_type_id) = 'shipping' and process ilike 'woolin02riskifiedintegration%' then original_charge_amount end) as orig_ship_original_charge_amount,
            sum(case when lower(charge_type_id) = 'coupon' then coalesce(charge_total, requested_amount) end) as coupon_charge_total,
            sum(case when lower(charge_type_id) = 'promotion' then coalesce(charge_total, requested_amount) end) as promo_charge_total,
            sum(case when lower(charge_type_id) = 'return fee' then charge_total end) as return_fee_charge_total,
            sum(case when lower(charge_type_id) = 'appeasement' and parent_charge_detail_id is null then charge_total end) as appeasement_charge_total,
            sum(case when lower(charge_type_id) = 'discount' then charge_total end) as discount_charge_total,
            array_compact(array_agg(
                case when ((lower(charge_type_id) in ('coupon', 'promotion', 'discount')) or (lower(charge_type_id) ='appeasement' and parent_charge_detail_id is null)) then
                    object_construct(
                        'couponCode', case 
                                        when lower(charge_type_id) in ('promotion', 'discount') then charge_detail_id  
                                        when lower(charge_type_id) ='appeasement' and parent_charge_detail_id is null then charge_detail_id 
                                        when lower(charge_type_id) ='coupon' then coalesce(charge_reference_detail_id, regexp_substr(charge_detail_id, '_(.+)_', 1, 1, 'e')) 
                                    end,
                        'promoGroup', upper(charge_display_name),
                        'promoCode', charge_reference_id,
                        'amount', abs(coalesce(charge_total, requested_amount))
                    )
                end
            ) within group (order by abs(charge_total) desc, charge_reference_detail_id))::string as ord_coupons,
            array_compact(array_agg(
                case when lower(charge_type_id) = 'return fee' then
                    object_construct(
                        'adjustmentType', 2,
                        'adjustmentTypeDesc', 'RETURN LABEL FEE',
                        'adjustmentTypeId', 'NA',
                        'amount', abs(charge_total),
                        'override', 'NA'
                    )
                end
            ) within group (order by abs(charge_total) desc))::string as return_fee_info,
			max_by(case when lower(charge_type_id) = 'coupon' then charge_detail_id end,
                   case when lower(charge_type_id) = 'coupon' then updated_timestamp end) as loyalty_reward_id,
            max_by(case when lower(charge_type_id) = 'appeasement' and parent_charge_detail_id is null then tax_code end,
                   case when lower(charge_type_id) = 'appeasement' and parent_charge_detail_id is null then updated_timestamp end) as appeasement_tax_code,
            max_by(case when lower(charge_type_id) = 'appeasement' and parent_charge_detail_id is null then reason_id end,
                   case when lower(charge_type_id) = 'appeasement' and parent_charge_detail_id is null then updated_timestamp end) as appeasement_reason_id,
            max_by(case when lower(charge_type_id) = 'promotion' then charge_display_name end,
                   case when lower(charge_type_id) = 'promotion' then updated_timestamp end) as promo_charge_display_name
        from line_charge_detail
        where coalesce(charge_total, requested_amount,0)  !=0
        group by all
    ),
    line_tax_detail as (
        select
            org_id,
            order_line_pk,
            sum(case when process not ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') = 'Shipping_1' then tax_amount end) as shipping_tax_amount,
            sum(case when process not ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') != 'Shipping_1' then tax_amount end) as sales_tax_amount,
            max(case when process not ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') = 'Shipping_1' then tax_code end) as shipping_tax_code,
            sum(case when process ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') = 'Shipping_1' then tax_amount end) as orig_shipping_tax_amount,
            sum(case when process ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') != 'Shipping_1' then tax_amount end) as orig_sales_tax_amount,
            max(case when process ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') = 'Shipping_1' then tax_code end) as orig_shipping_tax_code
        from {{ ref('mao_ord_order_line_tax_detail_hist_t') }}
        where lower(tax_type_id) = 'sales'
            and coalesce(tax_amount,0)<>0
        group by all
    ),
    cancelled_line_charge_detail as (
        select
            org_id,
            order_line_pk,
            sum(case when lower(charge_type_id) = 'discount' then coalesce(charge_total, requested_amount) end) as discount_charge_total,
            sum(case when lower(charge_type_id) = 'coupon' then coalesce(charge_total, requested_amount) end) as coupon_charge_total,
			sum(case when lower(charge_type_id) = 'promotion' then coalesce(charge_total, requested_amount) end) as promo_charge_total,
            sum(case when lower(charge_type_id) = 'shipping' and process not ilike 'woolin02riskifiedintegration%' then charge_total end) as shipping_charge_total,
			sum(case when lower(charge_type_id) = 'shipping' and process ilike 'woolin02riskifiedintegration%' then charge_total end) as orig_shipping_charge_total,
        from {{ ref('mao_ord_cancel_line_charge_detail_hist_t') }}
        where lower(charge_type_id) in ('coupon', 'promotion', 'shipping', 'discount')
            and coalesce(charge_total, requested_amount,0)  !=0
        group by all
    ),
    cancelled_line_tax_detail as (
        select
            org_id,
            order_line_pk,
            sum(case when process not ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') = 'Shipping_1' then tax_amount end) as shipping_tax_amount,
            sum(case when process not ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') != 'Shipping_1' then tax_amount end) as sales_tax_amount,
            max(case when process not ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') = 'Shipping_1' then tax_code end) as shipping_tax_code,
            sum(case when process ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') = 'Shipping_1' then tax_amount end) as orig_shipping_tax_amount,
            sum(case when process ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') != 'Shipping_1' then tax_amount end) as orig_sales_tax_amount,
            max(case when process ilike 'woolin02riskifiedintegration%' and coalesce(header_tax_detail_id,'') = 'Shipping_1' then tax_code end) as orig_shipping_tax_code
        from {{ ref('mao_ord_cancel_line_tax_detail_hist_t') }}
        where lower(tax_type_id) = 'sales'
            and coalesce(tax_amount,0)<>0
        group by all
    ),
    mao_fw_status_definition_v as (
        select status, description
        from {{ source('src_ord_mgmt_silver','mao_fw_status_definition_v') }}
        where lower(profile_id) = 'fl-us'
    ),
    dim_mao_loc_t as (
        select loc_id, loc_type_id
        from {{ ref('dim_mao_loc_t') }}
        where reporting_flg = 'Y'
    ),
    mao_ord_return_detail_v as (
        select org_id, order_line_pk, return_order_id, return_order_line_id, item_id, location_id
        from {{ source('src_ord_mgmt_silver','mao_ord_return_detail_v') }}
        qualify row_number() over (partition by org_id, order_line_pk order by updated_timestamp desc) = 1
    ),
    mao_ful_packages as (
        select org_id, package_id, ship_from_location_id, ship_via_id
        from {{ ref('mao_ful_packages_t') }}
        qualify row_number() over (partition by org_id, package_id order by updated_timestamp desc) = 1
    ),
    mao_ord_allocation_v as (
        select
            org_id, 
            order_line_pk, 
            allocation_id, 
            ship_to_location_id, 
            reservation_request_id,
            ship_via_id, 
            ship_from_location_id, 
            uom, 
            carrier_code
        from {{ source('src_ord_mgmt_silver','mao_ord_allocation_v') }}
        qualify row_number() over (partition by org_id, order_line_pk, allocation_id order by updated_timestamp desc) = 1
    ),
    mao_ord_release as (
        select
            reh.org_id,
            reh.release_id as rel_id,
            rel.release_line_id as rel_ln_id,
            reh.order_id as ord_id,
            rel.order_line_id as ord_ln_id,
            coalesce(reh.ship_from_location_id, al.ship_from_location_id) as ship_from_loc_id,
            rel.allocation_id,
            rel.quantity,
            rel.cancelled_quantity,
            rel.fulfilled_quantity,
            rel.cancelled_date,
            al.reservation_request_id,
            coalesce(reh.ship_via_id,al.ship_via_id) as ship_via_id,
            al.uom, 
            al.carrier_code
        from {{ source('src_ord_mgmt_silver','mao_ord_release_v') }} reh
        join {{ source('src_ord_mgmt_silver','mao_ord_release_line_v') }} rel
            on reh.org_id = rel.org_id and reh.pk = rel.release_pk
        join mao_ord_order_line ol
            on rel.org_id = ol.org_id and reh.order_id = ol.order_id and rel.order_line_id = ol.order_line_id and ol.ord_ln_pk=1
        left join mao_ord_allocation_v al
            on al.org_id = ol.org_id and al.order_line_pk = ol.pk and al.allocation_id = rel.allocation_id
        qualify row_number() over (partition by reh.org_id, reh.release_id, rel.release_line_id order by rel.updated_timestamp desc, reh.updated_timestamp desc) = 1
    ),
    mao_ord_fulfillment_detail as (
        select
            fd.org_id, ot.order_pk, fd.order_line_pk, fd.package_id, fd.item_id, fd.status_id,
            fd.tracking_number, ot.package_status, ot.original_est_delivery_date, ot.scheduled_date,
            ot.shipped_date, ot.delivered_date, ot.estimated_delivery_date,
            rel.allocation_id,
            rel.ship_from_loc_id as ship_from_loc_id, 
            rel.ship_via_id as ship_via_id,
            rel.reservation_request_id,
            rel.uom, 
            rel.carrier_code
        from {{ source('src_ord_mgmt_silver','mao_ord_fulfillment_detail_v') }} fd
            left join {{ source('src_ord_mgmt_silver','mao_ord_order_tracking_info_v') }} ot on fd.org_id = ot.org_id and fd.tracking_number = ot.tracking_number
            left join mao_ful_packages pkg on fd.org_id = pkg.org_id and fd.package_id = pkg.package_id
            left join mao_ord_release rel on fd.org_id = rel.org_id and fd.release_id = rel.rel_id and fd.release_line_id = rel.rel_ln_id
        qualify row_number() over (partition by fd.org_id, fd.order_line_pk order by fd.updated_timestamp desc, fd.status_id desc) = 1
    ),
    mao_ord_order_line_promising_info as (
        select org_id, order_line_pk, ship_from_location_id
        from {{ source('src_ord_mgmt_silver','mao_ord_order_line_promising_info_v') }}
        qualify row_number() over (partition by org_id, order_line_pk order by updated_timestamp desc) = 1
    ),
    order_note_json as (
        select
            org_id,
            order_line_pk,
            object_construct(replace(lower(note_type_id), ' ', ''), note_text) as note_obj
        from {{ source('src_ord_mgmt_silver','mao_ord_order_line_note_v') }}
    ),
    order_note_main as (
        select
            org_id,
            order_line_pk,
            array_construct(object_agg(key, value))::string as gift_array
        from (
            select
                nj.org_id,
                nj.order_line_pk,
                f.key,
                max(f.value) as value
            from order_note_json nj,
                lateral flatten(input => note_obj) f
            group by all
        ) t
        group by all
    ),
    ord_line_is_backorder as (
        select org_id, order_pk as ord_pk, pk as ord_ln_pk
        from {{ ref('mao_ord_order_line_t') }}
        where max_fulfillment_status_id = '1500'
        group by all
    ),
    itm_item_attrib as (
        select itm.item_id, isa.is_returnable_at_store, isa.is_returnable_at_d_c
        from {{ source('src_itm_silver','mao_itm_item_v') }} itm
            join {{ source('src_itm_silver','mao_itm_selling_attributes_v') }} isa on itm.pk = isa.item_pk and itm.profile_id = isa.profile_id
        where lower(itm.profile_id) = 'fl-inc-na'
    ),
    cnl_reason as (
        select
            ch.org_id, ch.order_line_pk, ch.cancel_reason as cancel_reason_id, re.description as cancel_reason_desc
        from {{ source('src_ord_mgmt_silver','mao_ord_order_line_cancel_history_v') }} ch
            left join (
                select reason_id, description
                from {{ source('src_ord_mgmt_silver','mao_ord_reason_v') }}
                where lower(profile_id) = 'fl-us'
            ) re on ch.cancel_reason = re.reason_id
        qualify row_number() over (partition by ch.org_id, ch.order_line_pk order by ch.updated_timestamp desc) = 1
    ),
    ord_line as (
        select
            ol.order_pk as ord_pk
            ,ol.pk as ord_ln_pk
            ,ol.org_id as org_id
            ,ol.order_id as ord_id
            ,ol.order_line_id as ord_ln_id
            ,ol.parent_order_id as prnt_ord_id
            ,ol.parent_order_line_id as prnt_ord_ln_id
            ,ol.item_id as item_id
            ,ol.min_fulfillment_status_id as min_fulflmnt_status_id
            ,st2.description as min_fulflmnt_status_desc
            ,ol.max_fulfillment_status_id as max_fulflmnt_status_id
            ,st3.description as max_fulflmnt_status_desc
            ,cr.cancel_reason_id as cnl_reason_id
            ,cr.cancel_reason_desc as cnl_reason_desc
            ,ol.physical_origin_id as physical_org_id
            ,ol.pipeline_id as pipeline_id
            ,ol.delivery_method_id as dlvry_method_id
            ,ol.fulfillment_group_id as fulflmnt_group_id
            ,ol.original_item_id as orig_item_id
            ,ol.parent_order_line_type_id as prnt_ord_ln_type_id
            ,ol.payment_group_id as pymt_grp_id
            ,ol.selling_location_id as selling_loc_id
            ,fd.allocation_id as alloc_id
            ,fd.reservation_request_id as resrv_req_id
            ,fd.ship_via_id as ship_via_id
            ,coalesce(fd.ship_from_loc_id, po.ship_from_location_id, ol.physical_origin_id) as ship_from_loc_id
            ,ltf.loc_type_id as ship_from_loc_type
            ,ol.ship_to_location_id as ship_to_loc_id
            ,ltt.loc_type_id as ship_to_loc_type
            ,rt.location_id as rtn_loc_id
            ,ol.shipping_method_id as ship_method_id
            ,ol.split_from_line_id as split_from_ln_id
            ,ltx.shipping_tax_code as tax_cd
            ,ol.store_sale_entry_method_id as str_sale_entry_method_id
            ,ol.transaction_reference_id as txn_ref_id
            ,lca.loyalty_reward_id
            ,ol.ship_to_address_id as ship_to_addr_id
            ,sa.address_postalcode as ship_to_addr_postal_cd
            ,sa.address_lastname as ship_to_addr_last_name
            ,sa.address_firstname as ship_to_addr_first_name
            ,sa.address_name as ship_to_addr_name
            ,sa.address_email as ship_to_addr_email
            ,sa.address_address1 as ship_to_addr_addr1
            ,sa.address_address2 as ship_to_addr_addr2
            ,sa.address_address3 as ship_to_addr_addr3
            ,sa.address_phone as ship_to_addr_phone
            ,sa.address_country as ship_to_addr_country
            ,sa.address_state as ship_to_addr_state
            ,sa.address_county as ship_to_addr_county
            ,sa.address_city as ship_to_addr_city
            ,sa.is_address_verified as is_ship_to_addr_verified
            ,ol.is_activation_required as is_activation_reqd
            ,ol.is_cancelled as is_cnlled
            ,ol.is_discountable as is_discable
            ,ol.is_even_exchange as is_even_exchg
            ,ol.is_exchangeable as is_exchgable
            ,ol.is_gift as is_gift
            ,ol.is_gift_card as is_gift_card
            ,ol.is_item_not_on_file as is_item_not_on_file
            ,ol.is_item_tax_exemptable as is_item_tax_exempt
            ,ol.is_item_tax_overridable as is_item_tax_overridable
            ,ol.is_non_merchandise as is_non_merchandise
            ,ol.is_on_hold as is_on_hold
            ,ol.is_pack_and_hold as is_pk_and_hold
            ,ol.is_pre_order as is_pre_ord
            ,ol.is_pre_sale as is_pre_sale
            ,ol.is_price_overridden as is_price_overridden
            ,ol.is_price_overrideable as is_price_overrideable
            ,ol.is_recommended_item as is_recommended_item
            ,ol.is_refund_gift_card as is_refund_gift_card
            ,ol.is_return as is_rtn
            ,ol.is_returnable as is_rtnable
            ,it.is_returnable_at_d_c as is_rtnable_at_d_c
            ,it.is_returnable_at_store as is_rtnable_at_store
            ,ol.is_tax_included as is_tax_included
            ,ol.is_tax_overridden as is_tax_overridden
            ,case when round(nvl(lca.ship_charge_total, 0)) = 0 then 1 else 0 end as is_free_shipping
            ,case when coalesce(oh.order_total, 0) = 0 and lower(oh.order_type_id) = 'callcenter' and ol.item_id ilike '%card%' then 1 else 0 end is_appeasement
            ,case when lca.ord_coupons is not null then 1 else 0 end as is_loyalty_disc
            ,case when olbk.ord_ln_pk is not null then 1 else 0 end as is_backorderFlg
            ,case when ol.json_fields:"extend::LaunchSKUFlag"::string != 'false' then 1 else 0 end as is_launch_sku_flg
            ,case when round(nvl(lca.ship_original_charge_amount, 0)) <> 0 then 1 else 0 end as is_base_shipping_charged
            ,case when lower(oh.order_type_id) = 'savethesale' and trim(lower(olrt.return_reason)) = 'lost package' then 0 else 1 end as is_restockable
            ,case when lower(ol.shipping_method_id) in ('express', 'overnight') then 1 else 0 end as rush_flg
            ,lca.ord_coupons as ord_coupons
            ,lca.return_fee_info as rtn_fee_info
            ,ol.can_ship_to_address as can_ship_to_addr
            ,ol.cancel_comments as cnl_comments
            ,ol.cancellation_request_status as cnl_request_status
            ,ol.carrier_code as carrier_cd
            ,ol.delivery_method_sub_type as dlvry_method_sub_type
            ,ol.effective_rank as eff_rank
            ,ol.fulfillment_sub_type as fulflmnt_sub_type
            ,ol.item_barcode as item_bar_cd
            ,ol.return_type as rtn_type
            ,ol.service_level_code as service_level_cd
            ,ol.tax_override_type as tax_override_type
            ,ol.uom as uom
            ,fd.tracking_number as tracking_num
            ,fd.package_status as pkg_status
            ,ol.item_description as itm_desc
            ,ol.item_short_description as itm_short_desc
            ,ol.item_size as itm_size
            ,ol.item_color_description as itm_color_desc
            ,ol.item_style as itm_style
            ,ol.item_tax_code as itm_tax_cd
            ,ol.item_brand as itm_brand
            ,ol.small_image_u_r_i
            ,ol.item_web_u_r_l as itm_web_u_r_l
            ,ol.item_department_name as itm_dept_name
            ,ol.json_fields:"ColorImageURI"::string as color_image_uri
            ,ol.json_fields:"extend::CartShipMethod"::string as cart_shpmnt_method
            ,ol.json_fields:"extend::ProductId"::string as product_id
            ,case when ol.item_id ilike '%card%' then 'E_GIFT_CARD' else 'REGULAR' end as product_type
            ,ol.json_fields:"extend::ShippingMethod"::string as shpmnt_method
            ,lca.promo_charge_display_name as price_Override_Reason
            ,olrt.return_reason as rtn_reason
            ,lca.appeasement_reason_id as appeasment_reason_cd
            ,case
                when lower(lca.appeasement_reason_id) in ('csarefund', 'customer service credit', 'customerservice', 'lost/damaged package credit', 'wrong item', 'missing items credit') then coalesce(ol.parent_order_id, ol.order_id)
            end as relate_ord_num_csa
            ,no.gift_array as ord_note
            ,ol.value_entry_required as value_entry_reqd
            ,ol.volumetric_weight as volumetric_weight
            ,oh.package_count as pkg_cnt
            ,ol.json_fields:"extend::FLOriginalUnitPrice"::decimal(16, 2) as orig_unit_price
            ,ol.unit_price as unit_price
            ,ccl.discount_charge_total as cnlled_ord_disc_amt
            ,ccl.promo_charge_total as cnlled_ord_promo_amt
            ,ccl.coupon_charge_total as cnlled_ord_coupon_amt
            ,coalesce(ccl.orig_shipping_charge_total,ccl.shipping_charge_total) as cnlled_ord_shipping_amt
            ,coalesce(cst.orig_shipping_tax_amount,cst.shipping_tax_amount) as cnlled_ord_shipping_tax_amt
            ,coalesce(cst.orig_sales_tax_amount,cst.sales_tax_amount) as cnlled_ord_sales_tax_amt
            ,ccl.orig_shipping_charge_total as cnlled_orig_ord_shipping_amt
            ,cst.orig_shipping_tax_amount as cnlled_orig_ord_shipping_tax_amt
            ,cst.orig_sales_tax_amount as cnlled_orig_ord_sales_tax_amt
            ,ol.cancelled_order_line_sub_total as cnlled_ord_ln_sub_total
            ,ol.json_fields:"CancelledOrderLineTotal"::decimal(16, 2) as cnlled_ord_ln_total
            ,ol.json_fields:"CancelledTotalCharges"::decimal(16, 2) as cnlled_total_charges
            ,ol.cancelled_total_discounts as cnlled_total_disc
            ,ol.json_fields:"CancelledTotalTaxes"::decimal(16, 2) as cnlled_total_taxes
            ,ol.gift_card_value as gift_card_value
            ,ol.item_max_discount_amount as item_max_discount_amt
            ,case
                when coalesce(ol.order_line_sub_total, 0) = 0 and ol.item_id like '%ECARD%' then coalesce(ol.gift_card_value, 0)
                else coalesce(ol.order_line_sub_total, 0)
            end as ord_ln_sub_total
            ,case
                when coalesce(ol.order_line_total, 0) = 0 and ol.item_id like '%ECARD%' then coalesce(ol.gift_card_value, 0)
                else coalesce(ol.order_line_total, 0)
            end as ord_ln_total
            ,ol.total_discount_on_item as total_disc_on_item
            ,ol.total_charges as total_charges
            ,ol.total_discounts as total_disc
            ,ol.total_taxes as total_taxes
            ,ol.quantity as qty
            ,ol.refund_price as refund_price
			,lca.appeasement_charge_total as ord_appeasement_amt
			,lca.return_fee_charge_total as ord_rtn_fee_amt
			,lca.discount_charge_total as ord_disc_amt
            ,lca.promo_charge_total as ord_promo_amt
            ,lca.coupon_charge_total as ord_coupon_amt
            ,coalesce(lca.orig_ship_charge_total,lca.ship_charge_total) as ord_shipping_amt
            ,coalesce(ltx.orig_shipping_tax_amount,ltx.shipping_tax_amount) as ord_shipping_tax_amt
            ,coalesce(ltx.orig_sales_tax_amount,ltx.sales_tax_amount) as ord_sales_tax_amt
            ,lca.orig_ship_charge_total as orig_ord_shipping_amt
            ,ltx.orig_shipping_tax_amount as orig_ord_shipping_tax_amt
            ,ltx.orig_sales_tax_amount as orig_ord_sales_tax_amt
            ,parse_json(oh.json_store):"Fields":"extend::HeaderLoyaltyDiscountAmount"::decimal(16, 2) as hdr_loyalty_disc_amt
            ,ol.json_fields:"extend::OrigLineSubtotal"::decimal(16, 2) as orig_line_sub_total
            ,ol.json_fields:"extend::OrigOrderedQty"::decimal(16, 0) as orig_ord_qty
            ,ol.json_fields:"extend::OrigTotalTaxes"::decimal(16, 2) as orig_total_tax
            ,ol.json_fields:"extend::OriginalDiscountAmount"::decimal(16, 2) as orig_disc_amt
            ,ol.json_fields:"extend::ProratedDiscountTotal"::decimal(16, 2) as pro_rated_disc_total
            ,ol.business_date::timestamp_ntz as business_dt
            ,ol.do_not_ship_before_date::timestamp_ntz as do_not_ship_before_dt
            ,ol.latest_delivery_date::timestamp_ntz as latest_dlvry_dt
            ,ol.promised_delivery_date::timestamp_ntz as promised_dlvry_dt
            ,ol.promised_ship_date::timestamp_ntz as promised_ship_dt
            ,ol.requested_delivery_date::timestamp_ntz as requested_dlvry_dt
            ,fd.original_est_delivery_date::timestamp_ntz as original_est_dlvry_ts
            ,fd.estimated_delivery_date::timestamp_ntz as estimated_dlvry_ts
            ,fd.scheduled_date::timestamp_ntz as scheduled_ts
            ,fd.shipped_date::timestamp_ntz as shipped_ts
            ,fd.delivered_date::timestamp_ntz as delivered_ts
            ,ol.created_by as created_by
            ,ol.created_timestamp::timestamp_ntz as created_ts
            ,ol.updated_by as updated_by
            ,ol.updated_timestamp::timestamp_ntz as updated_ts
            ,ol.src_load_ts as src_load_ts
        from mao_ord_order_line ol
            join {{ source('src_ord_mgmt_silver','mao_ord_order_v') }} oh on ol.org_id = oh.org_id and ol.order_pk = oh.pk
            left join {{ source('src_ord_mgmt_silver','mao_ord_ship_to_address_v') }} sa on ol.org_id = sa.org_id and ol.order_id = sa.order_id and ol.ship_to_address_id = sa.address_id
            left join mao_ord_fulfillment_detail fd on ol.org_id = fd.org_id and ol.pk = fd.order_line_pk
            left join mao_ord_order_line_promising_info po on ol.org_id = po.org_id and ol.pk = po.order_line_pk
            left join mao_fw_status_definition_v st2 on ol.min_fulfillment_status_id = st2.status
            left join mao_fw_status_definition_v st3 on ol.max_fulfillment_status_id = st3.status
            left join line_charge_agg lca on ol.org_id = lca.org_id and ol.pk = lca.order_line_pk
            left join line_tax_detail ltx on ol.org_id = ltx.org_id and ol.pk = ltx.order_line_pk
            left join cancelled_line_charge_detail ccl on ol.org_id = ccl.org_id and ol.pk = ccl.order_line_pk
            left join cancelled_line_tax_detail cst on ol.org_id = cst.org_id and ol.pk = cst.order_line_pk
            left join mao_ord_return_detail_v rt on ol.org_id = rt.org_id and ol.pk = rt.order_line_pk
            left join dim_mao_loc_t ltf on coalesce(fd.ship_from_loc_id, po.ship_from_location_id, ol.physical_origin_id) = ltf.loc_id
            left join dim_mao_loc_t ltt on ol.ship_to_location_id = ltt.loc_id
            left join cnl_reason cr on ol.org_id = cr.org_id and ol.pk = cr.order_line_pk
            left join ord_line_is_backorder olbk on ol.org_id = olbk.org_id and ol.order_pk = olbk.ord_pk and ol.pk = olbk.ord_ln_pk
            left join {{ source('src_ord_mgmt_silver','mao_ord_order_line_additional_v') }} olrt on ol.org_id = olrt.org_id and ol.pk = olrt.order_line_pk
            left join order_note_main no on ol.org_id = no.org_id and ol.pk = no.order_line_pk
            left join itm_item_attrib it on ol.item_id = it.item_id
    ),
    ord_line_hash as (
        select
            src.*,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
        from
            ord_line as src
    )
select
    src.* exclude(src_load_ts),
    current_timestamp()::timestamp_ntz as start_ts,
    null::timestamp_ntz as end_ts,
    'Y'::varchar(1) as active_flg,
    'Y'::varchar(1) as reporting_flg,
    'N'::varchar(1) as status_hist_flg,
    src.src_load_ts::timestamp_ntz as src_load_ts,
    {{ v_batch_id }}::number(38, 0) as batch_id,
    current_timestamp()::timestamp_ntz as etl_load_ts,
    current_timestamp()::timestamp_ntz as etl_updt_ts
from
    ord_line_hash as src
