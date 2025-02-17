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
                {{ source }}.record_source,
                {{ source }}.load_datetime
        {%- else %}
                record_source,
                load_datetime
        {%- endif -%}
{%- endmacro %}
