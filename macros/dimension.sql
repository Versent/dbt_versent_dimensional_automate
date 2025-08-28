{% macro dim(
    name,
    source,
    payload,
    type2
    ) -%}
{%- set business_key = dbt_versent_dimensional_automate.business_key_name(name) -%}
{%- set sid = dbt_versent_dimensional_automate.sid_name(name) -%}
{%- set sid_type = var('sid_type', 'STRING') %}
{%- set hash_algorithm = var('hash_algorithm', 'MD5(<column>)') -%}

{%- set sid_column_expression = "CONCAT(" ~ business_key %}
{%- if type2 %}
    {%- set sid_column_expression = sid_column_expression ~ ", '|', " ~ dbt_versent_dimensional_automate.effective_date_column('from') %}
{%- endif %}
{%- set sid_column_expression = sid_column_expression ~ ")" %}

{%- if hash_algorithm == "automate_dv.hash" %}
    {%- set hash_expression = automate_dv.hash(sid_column_expression,sid) %}
{%- else %}
    {%- set hash_expression = hash_algorithm.replace('<column>', sid_column_expression) %}
{%- endif %}

with 
    source as (
        select
            {{ business_key}},
            {{ dbt_versent_dimensional_automate.payload_columns(payload)}},
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
            {{ dbt_versent_dimensional_automate.audit_columns() }}
        from 
        {{ ref(source) }}
        where
          -- remove the ghost as_of_date from records other than the ghost
            (TO_DATE(as_of_date) = '1900-01-01' and {{business_key}} = '00000000000000000000000000000000') or
            TO_DATE(as_of_date) <> '1900-01-01'        
        
    ),
    final as (
        select
        {%- if hash_algorithm == "automate_dv.hash" %}
            {{ hash_expression }},
        {%- else %}
            cast({{ hash_expression }} as {{ sid_type }}) as {{ sid }},
        {%- endif %}
            {{ business_key }},
            {%- for col in payload %}
            {{col}},
            {%- endfor %}
            {%- if type2 -%}
            -- cdc columns
                {{ dbt_versent_dimensional_automate.effective_date_column('from') }},
                {{ dbt_versent_dimensional_automate.effective_date_column('to') }},
                is_current,
            {%- endif -%}
            -- audit columns
            {{ dbt_versent_dimensional_automate.audit_columns() }}
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


