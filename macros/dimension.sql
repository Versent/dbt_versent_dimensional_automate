{% macro dim(
    name,
    source,
    payload,
    type2
    ) -%}
{%- set business_key = business_key_name(name) -%}
with 
    source as (
        select
            {{ sid_name(name)}},
            {{ business_key}},
            {{ payload_columns(payload)}}
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
            (TO_DATE(as_of_date) = '1900-01-01' and {{bkey}} = '00000000000000000000000000000000') or
            TO_DATE(as_of_date) <> '1900-01-01'        
        )
    ),
    {%- if type2 -%}
    cdc as (
        select
            *,
            iff(previous_date is null, early_date, as_of_date) as effective_from_datetime,
            
            coalesce(
                next_date, 
                late_date
                )                               as effective_to_datetime,
            iff(
                next_date is null,
                true,
                false
            )                                   as is_current,
            {% if record_action|length > 0 %}
            iff(
                {{ record_action }} = 'DELETE', 
                true, 
                false
                )                               as is_deleted
            {% else %}
                false                           as is_deleted
            {% endif %} 
        from
            source
        ),  
    {%- endif -%}
    final as (
        select
            md5(concat({{ business_key }} , '|', effective_from_datetime)) as {{dim_key}},
            {{ business_key }},
            {%- for col in payload %}
            {{col}},
            {%- endfor %}
            -- cdc columns
                effective_from_datetime,
                effective_to_datetime,
                is_current,
                is_deleted,
            --{% if record_action|length > 0 %}
            --    record_action,
            --{% endif %}
            -- audit columns
                '{{source}}'                        as record_source,
                current_timestamp()                 as load_datetime
        from
            {%- if type2 -%}
            cdc
            {%- else -%}
            source
            {%- endif%}
        where
            -- row_num = 1 and 
            1=1
    )
select 
    *
from final
{%- endmacro %}


