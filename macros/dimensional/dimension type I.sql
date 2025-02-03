{% macro dim_type1(
    hash_key,
    source,
    payload
    ) -%}
{%- set bkey = hash_key|replace('hk_','bk_') -%}
{%- set dim_key = hash_key|replace('hk_','dim_') ~ '_sid' -%}
with 
    source as (
        select
            {{hash_key}} as {{ dim_key}}, 
            {{hash_key}}, 
            {{bkey}},            
            {%- for col in payload %}
            {{col}},
            {%- endfor %}
            record_source,
            load_datetime
        from {{ ref(source) }}
        ),


    final as (
        select
            cast({{ dim_key}} as string), 
            {{ hash_key }}, 
            {{ bkey }},
            {%- for col in payload %}
            {{col}},
            {%- endfor %}

            -- audit columns
                '{{source}}'                        as record_source,
                current_timestamp()                 as load_datetime
        from
            source
    )
select 
    *
from final
{%- endmacro %}