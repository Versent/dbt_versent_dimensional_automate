{%- macro type2_columns(
    type2
    ) -%}
        -- type2 columns
                {%- set as_of_date_source = type2.as_of_date %}
                {{ as_of_date_source }}     as as_of_date,
                {{ early_date() }}          as early_date,
                {{ late_date() }}           as late_date,
                lead({{ as_of_date_source }} ) 
                    over (
                        partition by {{business_key}}  
                        order by {{ as_of_date_source }} 
                    ) 
                    -- {{ target.type }}
                        {%- if target.type == "databricks" -%}
                        - INTERVAL 1 microsecond     
                        {%- elif target.type == "snowflake" -%}          
                        - INTERVAL '1 MICROSECONDS'
                        {% endif %}
                                            as next_date,

{%- endmacro %}
{%- macro early_date() -%}
    cast('1900-01-01 00:00:01'   as timestamp)
{%- endmacro %}
{%- macro late_date() -%}
    cast('2999-01-01 00:00:01'   as timestamp) as late_date,  
 {%- endmacro %}  