{% macro fact(
    hash_key,
    source,
    as_at_date,
    payload,
	type1_dimensions,
	type2_dimensions,
	role_playing_dimensions
    ) -%}
{%- set bkey = hash_key|replace('hk_','bk_') -%}
{%- set dim_key = hash_key|replace('hk_','dim_') ~ '_sid' -%}
with
	bridge as (
		select
			*
		from
			{{ ref(source)}}
	)
select 
	bridge.{{hash_key}},
    dim_date.dim_date_sid,
    -- type 1 dimensions
        {% for dim in type1_dimensions %}
        {{dim~'_sid'}},
        {%- set hkey = dim|replace('dim_','hk_') %}
        bridge.{{hkey}},
        {%- endfor %}
    -- type 2 dimensions
        {% for dim in type2_dimensions %}
        {{dim}}.{{dim~'_sid'}},
        {%- set hkey = dim|replace('dim_','hk_') %}
        bridge.{{hkey}},
        {%- endfor %}
    -- role playing dimensions
        {% for dim in role_playing_dimensions%}
            {% for key, value in dim.items() %}
                {{key}}.{{value['dim']~'_sid'}} as {{key~'_sid'}},
                bridge.{{value['hk']}},
                
            {%- endfor %}
        {%- endfor %}
	{{ mac_payload_cols(payload)}}
	
    current_timestamp() as load_datetime,
    '{{ source }}'  as record_source
from
    bridge
	left join
		{{ ref('dim_date')}}
		on
			bridge.hk_date = dim_date.hk_date
-- type 1s
	{%- for dim in type1_dimensions %}
    {% set dim_lower = dim|lower%}
	left join
		{{ ref(dim_lower)}}
		on
			{%- set hkey = dim|replace('dim_','hk_') %}
			bridge.{{hkey}} = {{dim}}.{{hkey}} 
	{%- endfor %}            
-- type 2s
	{%- for dim in type2_dimensions %}
    {% set dim_lower = dim|lower%}
	left join
		{{ ref(dim_lower)}}
		on
			{%- set hkey = dim|replace('dim_','hk_') %}
			bridge.{{hkey}} = {{dim}}.{{hkey}} and
            {{ date_seed(as_at_date)}}
			-- dim_date.date {{as_at_date}}
				between 
					{{dim}}.effective_from_datetime and
					{{dim}}.effective_to_datetime
    
	{%- endfor %}
-- role playing dimensions
    {% for dim in role_playing_dimensions %}
        {% for key, value in dim.items() %}
    left join
        {{ ref(value['dim']|lower)}} as {{key}}
        on
            {%- set hkey = value['dim']|replace('dim_','hk_') %}
            bridge.{{value['hk']}} = {{key}}.{{hkey}}
            {% if value['type'] == 2 %}
            and {{ date_seed(as_at_date)}}
                between
                    {{key}}.effective_from_datetime and
					{{key}}.effective_to_datetime
            {% else %}
            {% endif %}
        {%- endfor %}
	{%- endfor %}
{%- endmacro %}

{% macro date_seed(as_at_date) %}
    date_trunc(
        'SECOND',
        {% if as_at_date %}
            {{as_at_date}}
        {% else %}
            dim_date.date
        {% endif %}
    )
{%- endmacro %}