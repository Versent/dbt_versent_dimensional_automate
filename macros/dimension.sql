{% macro dim(
    name,
    source,
    payload,
    type2
    ) -%}
{%- set business_key = business_key_name(name) -%}
{%- set effective_from = var('effective_from_name', 'effective_from') -%}
{%- set effective_to = var('effective_to_name', 'effective_to') -%}       

{%- set sid = sid_name(name) -%}
with 
    source as (
        select
            {{ business_key}},
            {{ payload_columns(payload)}},
            -- hashdiff
                CAST(UPPER(md5(CONCAT(
                {%- for col in payload %}
                IFNULL(NULLIF(TRIM(CAST({{col}} AS VARCHAR(16))), ''), '^^'){% if not loop.last %}, '||',{% endif %}
                {%- endfor %}
                {% if record_action|length > 0 %}
                , '||',{{ record_action }}
                {% endif %}            
                ))) AS binary) as hashdiff,
            {% if record_action|length > 0 %}
            {{ record_action }},
            {% endif %}
            {% if type2 %}
            {{ type2_columns(business_key, type2) }}
            {% endif %}
            {{ audit_columns() }}
        from 
        {{ ref(source) }}
        where
          -- remove the ghost as_of_date from records other than the ghost
            (TO_DATE(as_of_date) = '1900-01-01' and {{business_key}} = '00000000000000000000000000000000') or
            TO_DATE(as_of_date) <> '1900-01-01'        
        
    ),
    final as (
        select
            md5(concat(
                {{ business_key }} 
                {%- if type2 -%}, '|', {{ effective_from }} {%- endif%}
                 )) as {{sid}},
            {{ business_key }},
            {%- for col in payload %}
            {{col}},
            {%- endfor %}
            {%- if type2 -%}
            -- cdc columns
                {{ effective_from }},
                {{ effective_to }},
                is_current,
            {%- endif -%}
            -- audit columns
                '{{source}}'                        as record_source,
                current_timestamp()                 as load_datetime
        from
            source
        where
            -- row_num = 1 and 
            1=1
    )
select 
    *
from final
{%- endmacro %}


