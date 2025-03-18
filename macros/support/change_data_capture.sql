{%- macro type2_columns(
    business_key,
    type2
    ) -%}
    -- type2 columns
            {%- set as_of_date_source = type2.as_of_date %}
            {%- set effective_from = var('effective_from_name', 'effective_from') -%}
            {%- set effective_to = var('effective_to_name', 'effective_to') %}                  
            {{ versent_automate_dbt_dimensional.effective_from_datetime(business_key, as_of_date_source)}}  as {{ effective_from }},
            {{ versent_automate_dbt_dimensional.effective_to_datetime(business_key, as_of_date_source)}}  as {{ effective_to }},
            {{ versent_automate_dbt_dimensional.is_current(business_key, as_of_date_source)}}  as is_current,

{%- endmacro %}
{%- macro early_date() -%}
    cast('1900-01-01 00:00:01'   as timestamp)
{%- endmacro %}
{%- macro late_date() -%}
    cast('2999-01-01 00:00:01'   as timestamp)
{%- endmacro %}  
{%- macro previous_date( 
    business_key,
    as_of_date_source
    ) -%}
    lag({{ as_of_date_source }} ) 
            over (
            partition by {{business_key}}   
            order by {{ as_of_date_source }} 
            )
    {%- endmacro -%}
{%- macro next_date( 
    business_key,
    as_of_date_source
    )  
    -%}
    lead({{ as_of_date_source }} ) 
    over (
        partition by {{ business_key }}  
        order by {{ as_of_date_source }} 
    ) 
    -- {{ target.type }}
        {%- if target.type == "databricks" -%}
        - INTERVAL 1 microsecond     
        {%- elif target.type == "snowflake" -%}          
        - INTERVAL '1 MICROSECONDS'
        {% endif %}
    {%- endmacro %}  
{%- macro effective_from_datetime(
    business_key,
    as_of_date_source
    ) -%}
    iff(
    -- previous_date
        {{ versent_automate_dbt_dimensional.previous_date(business_key, as_of_date_source)}}
        is null, 
        {{ versent_automate_dbt_dimensional.early_date() }}, 
        as_of_date)


    {%- endmacro %}  
{%- macro effective_to_datetime(
    business_key,
    as_of_date_source
    ) -%}
    coalesce(
        -- next_date - 1 very small bit
            {{ versent_automate_dbt_dimensional.next_date(business_key, as_of_date_source)}} 
            -- {{ target.type }}
            {% if target.type == "databricks" %}
            - INTERVAL 1 microsecond     
            {% elif target.type == "snowflake" %}          
            - INTERVAL '1 MICROSECONDS'
            {% endif %}, 
        {{ versent_automate_dbt_dimensional.late_date() }}
        )

    {%- endmacro%}
{%- macro is_current(
    business_key,
    as_of_date_source
    ) -%}
    iff(
        -- next_date
        {{ versent_automate_dbt_dimensional.next_date(business_key, as_of_date_source)}} is null,
        true,
        false
    )    
    {%- endmacro%}                               