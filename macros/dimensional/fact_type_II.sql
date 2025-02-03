{% macro fact_type2(
    unique_key,
    source,
    business_key,
    as_at_date,
    record_action,
    payload,
	dimensions,
	role_playing_dimensions
    ) -%}
with
	bridge as (
		select 
			*,
            lead({{as_at_date}}) 
                over (
                partition by {{business_key}}  
                order by {{as_at_date}}
            ) - INTERVAL 1 second               as next_date,
            cast('9999-12-31 23:59:59' as timestamp
            )                                   as late_date                                      
		from
			{{ ref(source)}}
	)
select 
	bridge.{{unique_key}},
    -- dimensions
        {% for dim in dimensions %}
        {%- set hkey = dim|replace('dim_','hk_') %}
        bridge.{{hkey}},
        {%- endfor %}
    -- role playing dimensions
        {% for dim in role_playing_dimensions%}
            {% for key, value in dim.items() %}
                bridge.{{value['hk']}},
                
            {%- endfor %}
        {%- endfor %}
	{{ mac_payload_cols(payload)}}
	
    bridge.{{business_key}}         as {{business_key}},
    bridge.{{as_at_date}}           as effective_from_datetime,
    COALESCE(next_date, late_date ) as effective_to_datetime,
    iff(
        isnull(next_date ),
        true,
        false
    )                               as is_current,
    {% if record_action|length > 0 %}
    iff(
        {{ record_action }} = 'DELETE', 
        true, 
        false
        )                           as is_deleted,
    {% else %}
        false                       as is_deleted,
    {% endif %} 
    current_timestamp()             as load_datetime,
    '{{ source }}'                  as record_source
from
    bridge
{%- endmacro %}