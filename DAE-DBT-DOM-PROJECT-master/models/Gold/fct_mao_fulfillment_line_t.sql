{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "org_id", "fulflmnt_id", "fulflmnt_ln_id"]  %}
	
{% set target_context_typ2_column_list = 
    [ "rel_id","ord_org_id","fulflmnt_ln_status_id","fulflmnt_ln_status_desc","ord_id","ord_ln_id","rel_ln_id",
	"cnl_reason_id","cnl_reason_desc","alloc_id","supply_type_id","asn_id","item_id","min_status_id","min_status_desc","max_status_id","max_status_desc",
	"substituted_item_id","pymt_method_id","substitution_reason_id","ship_via_id","shpmt_id","tracking_num","task_id","task_type_id","task_type_name",
	"task_status_id","task_status_name","is_gift","short_reason_id", "short_reason_desc", "transaction_type", "rejected_flg","dest_action","service_level_cd",
	"carrier_cd","gift_card_num","serial_num","sgtin","sold_online","sold_in_store","priority","fulflmnt_substitution_type","associate_num","str_dept",
	"rtned_to_shelf","task_assigned_to","task_updtd_by","task_picked_qty","task_qty","item_unit_price","odrd_qty","picked_qty","pked_qty","shipped_qty",
	"sorted_qty","rcvd_qty","cnlled_qty","qty_uom","orig_picked_qty","ln_short_count","eta", "task_status_ts", "created_by", "created_ts", "updated_by", "updated_ts" ]  %}

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
					"{{ fl_utils.m_apply_typ1_deletion_on_tgt_model( source('src_ord_fulflmnt_silver','mao_ful_fulfillment_line_v'), this, 'fulflmnt_ln_pk')   }}",
					"{{ fl_utils.m_upd_typ2_status_hist_flg_records( this ,'hash_sk' ,'hash_seq_num', 'fulflmnt_ln_status_id') }}",
					"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge", 'update_condition': "active_flg"}
) }}

with 
    mao_ful_fulfillment as (
        select
            fl.pk,
            fl.fulfillment_pk,
            fl.org_id,
			case when fl.fulfillment_line_status_id != '' then fl.fulfillment_line_status_id end::decimal(38, 0) as fulfillment_line_status_id,
            fl.fulfillment_line_id,
            fl.order_id,
            fl.order_line_id,
            fl.release_id,
            fl.release_line_id,
            fl.cancel_reason_id,
            fl.allocation_id,
            fl.supply_type_id,
            fl.asn_id,
            fl.item_id,
            fl.substituted_item_id,
            fl.is_gift,
            fl.fulfillment_substitution_type,
            fl.store_department,
            fl.returned_to_shelf,
            fl.item_unit_price,
            fl.ordered_qty,
            fl.picked_qty,
            fl.packed_qty,
            fl.shipped_qty,
            fl.sorted_qty,
            fl.received_qty,
            fl.cancelled_qty,
            fl.quantity_uom,
            fl.original_picked_qty,
            fl.line_short_count,
            fl.created_by,
            fl.created_timestamp,
            fl.updated_by,
            fl.updated_timestamp,
            fl.src_load_ts
        from {{ source('src_ord_fulflmnt_silver','mao_ful_fulfillment_line_v') }} fl
        {% if is_incremental() %}
            where
                fl.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
        {% endif %}
    ),
    ful_status_stg as (
        select status, description
        from {{ source('src_ord_fulflmnt_silver','mao_ful_fw_status_definition_v') }} 
        where profile_id = 'FL-US' and process_type_id = 'FULFILLMENT_EXECUTION'
    ),
    ful_task_detail as (
        select
            ts.task_id,
            ts.task_type_id,
            ts.task_status_id,
            ts.assigned_to,
            ts.updated_by,
            ts.task_status_timestamp,
            tt.task_type_name,
            tst.task_status_name,
            pt.org_id,
            pt.fulfillment_id,
            pt.fulfillment_line_id,
            pt.picked_qty,
            pt.quantity
        from {{ source('src_ord_fulflmnt_silver','mao_ful_task_v') }} ts
        join {{ source('src_ord_fulflmnt_silver','mao_ful_pick_task_detail_v') }} pt
            on pt.org_id = ts.org_id and pt.task_id = ts.task_id
        left join {{ source('src_ord_fulflmnt_silver','mao_ful_task_type_v') }} tt
            on ts.task_type_id = tt.task_type_id
        left join {{ source('src_ord_fulflmnt_silver','mao_ful_task_status_v') }} tst
            on ts.task_status_id = tst.task_status_id
    ),
    fulfilmnt_details as (
        select
            ol.org_id,
            ol.ord_id as order_id,
            ol.ord_ln_id as order_line_id,
            fd.eta,
            fd.gift_card_number,
            fd.sgtin,
            fd.shipment_id,
            fd.serial_number,
            fd.tracking_number
        from {{ source('src_ord_mgmt_silver','mao_ord_fulfillment_detail_v') }} fd
        join {{ ref('fct_mao_ord_line_t') }} ol
            on ol.org_id = fd.org_id and ol.ord_ln_pk = fd.order_line_pk and ol.reporting_flg = 'Y'
        qualify row_number() over (
            partition by fd.org_id, fd.order_line_pk
            order by fd.src_load_ts desc
        ) = 1
    ),
    itm_item_attrib as (
        select itm.item_id, isa.sold_in_stores, isa.sold_online
        from {{ source('src_itm_silver','mao_itm_item_v') }} itm 
        join {{ source('src_itm_silver','mao_itm_selling_attributes_v') }} isa
            on itm.pk = isa.item_pk and itm.profile_id = isa.profile_id
        where lower(itm.profile_id) = 'fl-inc-na'
    ),
    payment_method_info as (
        select
            ph.org_id,
            ph.order_id,
            pm.payment_method_id
        from {{ source('src_payment_silver','mao_pay_payment_header_v') }} ph
        join {{ source('src_payment_silver','mao_pay_payment_method_v') }} pm
            on pm.org_id = ph.org_id and pm.payment_header_pk = ph.pk
        qualify row_number() over (
            partition by ph.org_id, ph.order_id
            order by ph.updated_timestamp desc
        ) = 1
    ),
    mao_ful_fulfillment_line_shorts as (
        select
            s.fulfillment_line_pk,
            s.org_id,
            s.short_reason_id,
            r.fulfillment_reason_name as short_reason_desc,
            upper(s.transaction_type) as transaction_type, 
            case upper(s.transaction_type) when 'PICK' then '3000.000' when 'PACK' then '4000.000' end as fulfillment_line_status_id
        from {{ source('src_ord_fulflmnt_silver','mao_ful_fulfillment_line_shorts_v') }} s 
        left join (
            select fulfillment_reason_id, fulfillment_reason_name
            from {{ source('src_ord_fulflmnt_silver','mao_ful_fulfillment_reason_v') }}
            where lower(profile_id) = 'fl-us' and fulfillment_reason_type_id = 2000
        ) r on r.fulfillment_reason_id = s.short_reason_id
        qualify row_number() over (
            partition by s.fulfillment_line_pk, s.org_id, 
                case upper(s.transaction_type) when 'PICK' then '3000.000' when 'PACK' then '4000.000' end
            order by s.updated_timestamp desc
        ) = 1
    ),
    fulfillment_line_stg as (
        select
            fl.fulfillment_pk as fulflmnt_pk
            ,fl.pk as fulflmnt_ln_pk
            ,fl.org_id as org_id
            ,fh.fulfillment_id as fulflmnt_id
            ,fl.fulfillment_line_id as fulflmnt_ln_id
            ,fl.org_id as ord_org_id
            ,fl.fulfillment_line_status_id as fulflmnt_ln_status_id
            ,st1.description as fulflmnt_ln_status_desc
            ,fl.order_id as ord_id
            ,fl.order_line_id as ord_ln_id
            ,fl.release_id as rel_id
            ,fl.release_line_id as rel_ln_id
            ,fl.cancel_reason_id as cnl_reason_id
            ,rs.fulfillment_reason_name as cnl_reason_desc
            ,fl.allocation_id as alloc_id
            ,fl.supply_type_id as supply_type_id
            ,fl.asn_id as asn_id
            ,fl.item_id as item_id
            ,fh.min_status_id::decimal(38, 0) as min_status_id
            ,st2.description as min_status_desc
            ,fh.max_status_id::decimal(38, 0) as max_status_id
            ,st3.description as max_status_desc
            ,fl.substituted_item_id as substituted_item_id
            ,pm.payment_method_id as pymt_method_id
            ,null as substitution_reason_id
            ,fh.ship_via_id as ship_via_id
            ,fd.shipment_id as shpmt_id	
            ,fd.tracking_number as tracking_num
            ,ft.task_id as task_id
            ,ft.task_type_id as task_type_id	
            ,ft.task_type_name as task_type_name
            ,ft.task_status_id as task_status_id
            ,ft.task_status_name as task_status_name
            ,fl.is_gift as is_gift
            ,sh.short_reason_id
            ,sh.short_reason_desc
            ,sh.transaction_type
            ,case when sh.short_reason_id is not null then 1 else 0 end as rejected_flg
            ,re.destination_action as dest_action
            ,fh.service_level_code as service_level_cd
            ,fh.carrier_code as carrier_cd
            ,fd.gift_card_number as gift_card_num
            ,fd.serial_number as serial_num
            ,fd.sgtin as sgtin
            ,it.sold_online as sold_online
            ,it.sold_in_stores as sold_in_store
            ,fh.priority as priority	
            ,fl.fulfillment_substitution_type as fulflmnt_substitution_type
            ,parse_json(oh.json_store):"Fields":"extend::AssociateNumber"::string as associate_num		
            ,fl.store_department as str_dept
            ,fl.returned_to_shelf as rtned_to_shelf		
            ,ft.assigned_to as task_assigned_to
            ,ft.updated_by as task_updtd_by
            ,ft.picked_qty as task_picked_qty
            ,ft.quantity as task_qty
            ,fl.item_unit_price as item_unit_price
            ,fl.ordered_qty as odrd_qty
            ,fl.picked_qty as picked_qty
            ,fl.packed_qty as pked_qty
            ,fl.shipped_qty as shipped_qty
            ,fl.sorted_qty as sorted_qty
            ,fl.received_qty as rcvd_qty
            ,fl.cancelled_qty as cnlled_qty
            ,fl.quantity_uom as qty_uom
            ,fl.original_picked_qty as orig_picked_qty
            ,fl.line_short_count as ln_short_count
            ,fd.eta as eta
            ,ft.task_status_timestamp::timestamp_ntz as task_status_ts
            ,fl.created_by as created_by
            ,fl.created_timestamp::timestamp_ntz as created_ts
            ,fl.updated_by as updated_by
            ,fl.updated_timestamp::timestamp_ntz as updated_ts
            ,fl.src_load_ts
        from mao_ful_fulfillment fl
            join (
                select pk, org_id, fulfillment_id, min_status_id, max_status_id, ship_via_id, service_level_code, carrier_code, priority
                from {{ source('src_ord_fulflmnt_silver','mao_ful_fulfillment_v') }}
                {% if is_incremental() %}
                    where src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
                {% endif %}
            ) fh on fl.org_id = fh.org_id and fl.fulfillment_pk = fh.pk
            left join (
                select org_id, order_id, json_store
                from {{ source('src_ord_mgmt_silver','mao_ord_order_v') }}
            ) oh on fl.org_id = oh.org_id and fl.order_id = oh.order_id
            left join (
                select org_id, release_id, destination_action
                from {{ source('src_ord_fulflmnt_silver','mao_ord_release_v') }}
            ) re on fl.org_id = re.org_id and fl.release_id = re.release_id
            left join (
                select fulfillment_reason_id, fulfillment_reason_name
                from {{ source('src_ord_fulflmnt_silver','mao_ful_fulfillment_reason_v') }}
                where lower(profile_id) = 'fl-us'
            ) rs on fl.cancel_reason_id = rs.fulfillment_reason_id
            left join ful_task_detail ft
                on fl.org_id = ft.org_id and fh.fulfillment_id = ft.fulfillment_id and fl.fulfillment_line_id = ft.fulfillment_line_id
            left join ful_status_stg st1
                on fl.fulfillment_line_status_id = st1.status
            left join ful_status_stg st2
                on fh.min_status_id = st2.status
            left join ful_status_stg st3
                on fh.max_status_id = st3.status
            left join fulfilmnt_details fd
                on fl.org_id = fd.org_id and fl.order_id = fd.order_id and fl.order_line_id = fd.order_line_id
            left join itm_item_attrib it
                on fl.item_id = it.item_id
            left join payment_method_info pm
                on fl.org_id = pm.org_id and fl.order_id = pm.order_id
            left join mao_ful_fulfillment_line_shorts sh
                on sh.org_id = fl.org_id and sh.fulfillment_line_pk = fl.pk 
                and sh.fulfillment_line_status_id = fl.fulfillment_line_status_id
    ),
    fulfillment_line_main as (
        select
            src.*,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
            {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list) }} as hash_seq_num
        from
            fulfillment_line_stg as src
    )
select
    src.*,
    current_timestamp()::timestamp_ntz as start_ts,
    null::timestamp_ntz as end_ts,
    'Y'::varchar(1) as active_flg,
    'Y'::varchar(1) as reporting_flg,
    'N'::varchar(1) as status_hist_flg,
    {{ v_batch_id }}::number(38, 0) as batch_id,
    current_timestamp()::timestamp_ntz as etl_load_ts,
    current_timestamp()::timestamp_ntz as etl_updt_ts
from
    fulfillment_line_main as src