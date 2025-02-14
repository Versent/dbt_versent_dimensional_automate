{% macro fact(
    source,
    name,
    primary_key,
	type1_dims,
    type2_dims,
	payload
    ) -%}

with
	bridge as (
		select
			*
		from
			{{ ref(source) }}
	)
    select
        bridge.{{primary_key}},
        -- Add Type 1 dimension columns (if type 1 dims exist)
        {{ dimension_columns(type1_dims) }}
        -- Add Type 2 dimension columns (if type 2 dims exist)
        {{ dimension_columns(type2_dims) }}
        {{ payload_columns(payload)}},
        current_timestamp() as load_datetime,
        '{{ source }}' as record_source
    from
        bridge
        -- join Type 1 Dimensions (if they exist)
        {{join_dimensions(type1_dims, 'type1')}}                
        -- join Type 2 Dimensions (if they exist)
        {{join_dimensions(type2_dims, 'type2')}}          
    
{%- endmacro %}

{% macro join_dimensions(dimensions, type) -%}
    {%- if type == 'type2' -%}
        {%- set dim_as_of_date = dimensions.get('dimension_as_of_date') -%}
        {%- set effective_from = var('effective_from_name', 'effective_from') -%}
        {%- set effective_to = var('effective_to_name', 'effective_to') -%}       
    {%- endif -%}

    {%- if dimensions.get('dimensions') -%}
        {%- for dim in dimensions['dimensions'] %}
            {% for dim_name, properties in dim.items() %}
                {%- set dim_table_name = dimension_table_name(dim_name) -%}
                {%- if properties.get('role_playing') -%}
                    {% for alias, _ in properties['role_playing'].items() %}
                    left join {{ dim_table_name }} AS {{ alias }}
                        on bridge.{{ business_key_name(alias) }} = {{ alias }}.{{ properties['join_key'] }}
                        {% if type == 'type2' and dim_as_of_date -%}
                        and bridge.{{ dim_as_of_date }} between {{ alias }}.{{ effective_from }} and {{ alias }}.{{ effective_to }} 
                        {%- endif -%}                        
                    {%- endfor -%}
                {%- else -%}
                    left join {{ dim_table_name }}
                        on bridge.{{ properties['join_key'] }} = {{ dim_table_name }}.{{ properties['join_key'] }}
                        {% if type == 'type2' and dim_as_of_date -%}
                        and bridge.{{ dim_as_of_date }} between {{ dim_table_name }}.{{ effective_from }} and {{ dim_table_name }}.{{ effective_to }} 
                        {%- endif -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {% endif %}    
{%- endmacro %}

{% macro dimension_columns(dimensions) -%}
        {%- if dimensions.get('dimensions') %}
            {%- for dim in dimensions['dimensions'] %}
                {% for dim_name, properties in dim.items() %}
                    {%- set dim_table_name = dimension_table_name(dim_name) -%}                
                    {%- if properties.get('role_playing') -%}
                        {% for alias, _ in properties['role_playing'].items() %}
                            {{ alias }}.{{ sid_name(dim_name) }} as {{ sid_name(alias) }},
                            {% if var("include_business_keys") == 'y' -%}
                            {{ alias }}.{{ business_key_name(dim_name) }} as {{ business_key_name(alias) }},
                            {%- endif -%}
                        {%- endfor -%}
                    {%- else -%}
                        {{ dim_table_name }}.{{ sid_name(dim_name) }} as {{ sid_name(dim_name) }},
                        {% if var("include_business_keys") == 'y' -%}
                        {{ dim_table_name }}.{{ business_key_name(dim_name) }} as {{ business_key_name(dim_name) }},
                        {%- endif -%}                        
                    {%- endif -%}
                {%- endfor -%}
            {%- endfor -%}
        {%- endif %}
{%- endmacro %}