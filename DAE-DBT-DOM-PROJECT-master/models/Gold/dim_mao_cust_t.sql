{% set v_batch_id = fl_utils.m_get_batch_id( var('p_pipeline_name') ) %}
{% set v_pre_hook = fl_utils.m_init_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %} 
{% set v_inc_load_ts = fl_utils.m_get_inc_load_ts_for_model_record_from_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , v_batch_id ) %}

{% set target_key_column_list = 
    [ "org_id", "cust_id" ]  %}

{% set target_context_typ2_column_list = 
    [ "first_name", "last_name", "order_phoneNumber", "billing_first_name", "billing_last_name", "billing_address_line1", "billing_address_line2", "billing_city", "billing_postal_code", 
	"billing_postal_state", "billing_country", "billing_email", "billing_phoneNumber", "shipping_addressline1", "shipping_addressline2", "shipping_city", "shipping_country", 
	"shipping_state", "shipping_postal_code", "shipping_email", "shipping_first_name", "shipping_last_name", "shipping_phoneNumber", "f_l_x_id", "company_num" ]  %}

{{ config(
    materialized="incremental", 
    unique_key=["hash_sk", "hash_seq_num"], 
    merge_exclude_columns=["hash_sk", "hash_seq_num","start_ts","end_ts","active_flg","reporting_flg","etl_load_ts"], 
    post_hook=["{{ fl_utils.m_upd_typ2_activ_records_dom( this ,'hash_sk' ,'hash_seq_num' ) }}",
					"{{ fl_utils.m_upd_post_load_attrib_vals_for_model_record_in_dbt_model_audit( var('p_pipeline_name'), 'dom_gold', this , fl_utils.m_get_batch_id( var('p_pipeline_name') ) , 'src_load_ts' ) }}"], 
    meta={'strategy': "merge", 'update_condition': "active_flg"}
) }}

with 
    mao_ord_order as (
        select oh.*
        from {{ source('src_ord_mgmt_silver','mao_ord_order_v') }} oh 
        {% if is_incremental() %}
            WHERE
                oh.src_load_ts > {{ v_inc_load_ts }}::timestamp - interval '{{ env_var("DBT_T_MINUS_INTERVAL") | as_text }}'
			    and oh.customer_id is not null
        {% endif %}
    ),
    payment_billing_address as (
	select 
        ph.org_id,
		ph.order_id ,
		ph.customer_id, 
		pb.address_firstname,
		pb.address_lastname,
		pb.address_address1,
		pb.address_address2,
		pb.address_city,
		pb.address_postalcode,
		pb.address_state,
		pb.address_country,
		pb.address_email,
		pb.address_phone
	from 
	    {{ source('src_payment_silver','mao_pay_payment_header_v') }} ph
		join {{ source('src_payment_silver','mao_pay_payment_method_v') }} pm on (pm.org_id=ph.org_id and pm.payment_header_pk=ph.pk)
		left join {{ source('src_payment_silver','mao_pay_billing_address_v') }} pb on (pm.org_id=pb.org_id and pm.pk = pb.payment_method_pk)
	),
	cust_stg as (
	select 
		oh.org_id
		,oh.customer_id as cust_id
		,upper(oh.customer_first_name) as first_name
		,upper(oh.customer_last_name) as last_name
		,oh.customer_phone as order_phoneNumber
		,upper(ba.address_firstname) as billing_first_name
		,upper(ba.address_lastname) as billing_last_name
		,upper(ba.address_address1) as billing_address_line1
		,upper(ba.address_address2) as billing_address_line2
		,upper(ba.address_city) as billing_city
		,upper(ba.address_postalcode) as billing_postal_code
		,upper(ba.address_state) as billing_postal_state
		,upper(ba.address_country) as billing_country
		,upper(ba.address_email) as billing_email
		,ba.address_phone as billing_phoneNumber
		,upper(sa.address_address1) as shipping_addressline1
		,upper(sa.address_address2) as shipping_addressline2
		,upper(sa.address_city) as shipping_city
		,upper(sa.address_country) as shipping_country
		,upper(sa.address_state) as shipping_state
		,upper(sa.address_postalcode) as shipping_postal_code
		,upper(sa.address_email) as shipping_email
		,upper(sa.address_firstname) as shipping_first_name
		,upper(sa.address_firstname) as shipping_last_name
		,sa.address_phone as shipping_phoneNumber
		,parse_json(oh.json_store):"Fields":"extend::FLX-ID"::string as f_l_x_id
		,parse_json(oh.json_store):"Fields":"extend::CartCompanyNumber"::string as company_num
		,oh.src_load_ts as src_load_ts
		,row_number() over (partition by oh.org_id,oh.customer_id order by oh.src_load_ts desc) as rnk
	from mao_ord_order oh 
		left join payment_billing_address ba on (oh.org_id = ba.org_id and oh.order_id = ba.order_id)
		left join {{ source('src_ord_mgmt_silver','mao_ord_ship_to_address_v') }} sa on (oh.org_id = sa.org_id and oh.order_id = sa.order_id)
	),
	cust_stg_hash as (
    select
        src.*,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_key_column_list) }} as hash_sk,
        {{ fl_utils.m_prep_hash_key_from_column_list(target_context_typ2_column_list)}} as hash_seq_num
    from
        cust_stg as src
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
    cust_stg_hash as src
where rnk=1