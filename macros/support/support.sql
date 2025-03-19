{% macro sid_name(
    name
    ) -%}
    {%- set sid_name = var("sid_name")|replace('<name>',name) -%}

    {{ sid_name }}

{%- endmacro %}

{% macro dimension_table_name(
    name
    ) -%}
    {%- set dimension_name = var("dimension_table_name")|replace('<name>',name) -%}

    {{ dimension_name }}

{%- endmacro %}

{% macro business_key_name(
    name
    ) -%}
    {%- set business_key_name = var("business_key_name")|replace('<name>',name) -%}

    {{ business_key_name }}

{%- endmacro %}
{% macro payload_columns(
    payload
    ) -%}
            -- payload
    {%- for col in payload %}
            {{col}}{%- if not loop.last %}, {% endif %}
     {%- endfor %}
{%- endmacro %}
{% macro audit_columns(
    source=None
    ) -%}

        -- audit_columns
        {%- if source %}
                {% set qualifier = source ~ '.' %}
        {%- endif %}
            {{ qualifier }}{{ var('record_source') }},
            {{ qualifier }}{{ var('load_datetime') }}
{%- endmacro %}
{% macro effective_date_column(
    type
    ) -%}
        {%- if type == 'from' -%}
            {{ var('effective_from_name', 'effective_from') }}
        {%- elif type == 'to' -%}
            {{ var('effective_to_name', 'effective_to') }}
        {%- else -%}
            {{ exceptions.raise_compiler_error("Invalid type passed to effective_date_column. Expecting 'from' or 'to'.") }}
        {%- endif -%}
{%- endmacro %}
