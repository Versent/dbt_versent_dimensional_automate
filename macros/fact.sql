{% macro fact(
    source,
    name,
    primary_key,
	dimensions,
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
        {%- for dim_type in dimensions -%}
            {% if dim_type.get('type1') %}
            -- Processing type1 dimensions
                {% for dim in dim_type['type1'] %}
                    {%- for key, value in dim.items() -%}
                    {{ key }} as key,
                    {{ value }} as value,
                    {{ value['join_key'] }}
                    {%- endfor -%}
                {%- endfor -%}
            {%- endif -%} 
            {% if dim_type.get('type2') %}
            -- Processing type2 dimensions
                {% for dim in dim_type['type2'] %}
                    {%-for key, value in dim.items() -%}
                        {{ key }} as key,
                        {{ value }} as value,
                    {%- endfor -%}
                {%- endfor -%}
            {%- endif -%}                      
        {%- endfor -%}       
    from
        bridge

    {%- for dim_type in dimensions -%}
        {% if dim_type.get('type1') %}
            -- join type1 dimensions
            {%- for dims in dim_type['type1'] -%}
                {%- for dim, properties in dims.items() -%}
                    {%- if properties.get('role_playing') -%}
                        {% for alias, _ in properties['role_playing'].items() %}
                        left join {{ dim }} as {{ alias }}
                            on bridge.{{ properties['join_key'] }} = {{ alias }}.{{ properties['join_key'] }}                        
                        {%- endfor -%}
                    {% else %}
                    left join {{ dim }}
                        on bridge.{{ properties['join_key'] }} = {{ dim }}.{{ properties['join_key'] }}
                    {% endif %}
                {%- endfor -%}
            {%- endfor -%}
        {%- endif -%}

        {% if dim_type.get('type2') %}
            -- join type2 dimensions
            {%- for dims in dim_type['type2'] -%}
                {%- for dim, properties in dims.items() -%}
                    {%- if properties.get('role_playing') -%}
                        {% for alias, _ in properties['role_playing'].items() %}
                        left join {{ dim }} as {{ alias }}
                            on bridge.{{ properties['join_key'] }} = {{ alias }}.{{ properties['join_key'] }}                        
                        {%- endfor -%}
                    {% else %}
                    left join {{ dim }}
                        on bridge.{{ properties['join_key'] }} = {{ dim }}.{{ properties['join_key'] }}
                    {% endif %}
                {%- endfor -%}
            {%- endfor -%}
        {%- endif -%} 

    {%- endfor -%}            
    
{%- endmacro %}
