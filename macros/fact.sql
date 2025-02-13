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
        -- Loop through Type 1 dimensions (if they exist)
        {%- if type1_dims.get('dimensions') %}
            {%- for dim in type1_dims['dimensions'] %}
                {%- for dim_name, properties in dim.items() %}
                    {%- if properties.get('role_playing') -%}
                        {% for alias, _ in properties['role_playing'].items() %}
                            {{ alias }}.{{ sid_name(dim_name) }} as {{ sid_name(alias) }},
                            {% if var("include_business_keys") == 'y' -%}
                            {{ alias }}.{{ business_key_name(dim_name) }} as {{ business_key_name(alias) }},
                            {%- endif -%}
                        {%- endfor -%}
                    {%- else -%}
                        {{ dimension_table_name(dim_name) }}.{{ sid_name(dim_name) }} as {{ sid_name(dim_name) }},
                        {% if var("include_business_keys") == 'y' -%}
                        {{ dimension_table_name(dim_name) }}.{{ business_key_name(dim_name) }} as {{ business_key_name(dim_name) }},
                        {%- endif -%}                        
                    {%- endif -%}
                {%- endfor -%}
            {%- endfor -%}
        {%- endif %}
        -- Loop through Type 2 dimensions (if they exist)
        {% if type2_dims.get('dimensions') %}
            {%- for dim in type2_dims['dimensions'] %}
                {%- for dim_name, properties in dim.items() %}
                    {%- if properties.get('role_playing') -%}
                        {% for alias, _ in properties['role_playing'].items() %}
                            {{ alias }}.{{ sid_name(dim_name) }} as {{ sid_name(alias) }},
                            {% if var("include_business_keys") == 'y' -%}
                            {{ alias }}.{{ business_key_name(dim_name) }} as {{ business_key_name(alias) }},
                            {%- endif -%}
                        {%- endfor -%}
                    {%- else -%}
                        {{ dimension_table_name(dim_name) }}.{{ sid_name(dim_name) }} as {{ sid_name(dim_name) }},
                        {% if var("include_business_keys") == 'y' -%}
                        {{ dimension_table_name(dim_name) }}.{{ business_key_name(dim_name) }} as {{ business_key_name(dim_name) }},
                        {%- endif -%}                        
                    {%- endif -%}
                {%- endfor -%}
            {%- endfor -%}
        {%- endif %}
        {{ payload_columns(payload)}}
        'record_source' as record_source
    from
        bridge
    -- join Type 1 Dimensions (if they exist)
    {%- if type1_dims.get('dimensions') -%}
        {%- for dim in type1_dims['dimensions'] %}
            {% for dim_name, properties in dim.items() %}
                {%- if properties.get('role_playing') -%}
                    {% for alias, _ in properties['role_playing'].items() %}
                    left join {{ dimension_table_name(dim_name) }} AS {{ alias }}
                        on bridge.{{ properties['join_key'] }} = {{ alias }}.{{ properties['join_key'] }} 
                    {%- endfor -%}
                {%- else -%}
                    left join {{ dimension_table_name(dim_name) }}
                        on bridge.{{ properties['join_key'] }} = {{ dim_name }}.{{ properties['join_key'] }}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {% endif %}               
    -- join Type 2 Dimensions (if they exist)
    {%- if type2_dims.get('dimensions') -%}
        {%- for dim in type2_dims['dimensions'] %}
            {% for dim_name, properties in dim.items() %}
                {%- if properties.get('role_playing') -%}
                    {% for alias, _ in properties['role_playing'].items() %}
                    left join {{ dimension_table_name(dim_name) }} AS {{ alias }}
                        on bridge.{{ properties['join_key'] }} = {{ alias }}.{{ properties['join_key'] }} 
                    {%- endfor -%}
                {%- else -%}
                    left join {{ dimension_table_name(dim_name) }}
                        on bridge.{{ properties['join_key'] }} = {{ dim_name }}.{{ properties['join_key'] }}
                {%- endif -%}
            {%- endfor -%}
        {%- endfor -%}
    {% endif %}                    
    
{%- endmacro %}
