{% macro dim(
    dimension_name,
    business_key,
    source,
    record_action,
    payload
    ) -%}
{%- set bkey = business_key|replace('hk_','bk_') -%}
{%- set dim_key = 'dim_' ~ dimension_name ~ '_sid' -%}
with 
    source as (
        select
            {{bkey}},            
            {%- for col in payload %}
            {{col}},
            {%- endfor %}

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
            as_of_date,
            record_source,
            load_datetime
        from {{ ref(source) }}
        where
          -- remove the ghost as_of_date from records other than the ghost
            (TO_DATE(as_of_date) = '1900-01-01' and {{bkey}} = '00000000000000000000000000000000') or
            TO_DATE(as_of_date) <> '1900-01-01'        
        ),
    window_functions as (
        select 
            *,
            as_of_date,
            lag(hashdiff) over (
                partition by {{hash_key}} 
                order by as_of_date
            )   as previous_hashdiff,
            cast(
                '1900-01-01 00:00:01' 
                as timestamp
            )                                   as early_date,
             cast(
                '2999-01-01 00:00:01' 
                as timestamp
                )                               as late_date                                      
            
        from 
            source
    ),
    deltas as (
        select *,
            lag(as_of_date) 
                over (
                partition by {{hash_key}}  
                order by as_of_date
            )               as previous_date,
            lead(as_of_date) 
                over (
                partition by {{hash_key}}  
                order by as_of_date
            ) 
            -- {{ target.type }}
                {% if target.type == "databricks" %}
                - INTERVAL 1 microsecond     
                {% elif target.type == "snowflake" %}          
                - INTERVAL '1 MICROSECONDS'
                {% endif %}
                as next_date
             from window_functions where
            -- coalesce null previous_hashdiff with empty binary as 
            -- comparison with null will always be false 
                hashdiff <> coalesce(previous_hashdiff,cast('' as binary))
    ),
    cdc as (
        select
            *,
            if(previous_date is null, early_date, as_of_date) as effective_from_datetime,
            
            coalesce(
                next_date, 
                late_date
                )                               as effective_to_datetime,
            iff(
                isnull(next_date ),
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
            deltas
    ),  
    final as (
        select
            md5(concat({{ business_key }} , "|", effective_from_datetime)) as {{dim_key}},
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
            cdc
        where
            -- row_num = 1 and 
            1=1
    )
select 
    *
from final
{%- endmacro %}

