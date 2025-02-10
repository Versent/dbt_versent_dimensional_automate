{% macro sid_name(
    name
    ) -%}
    {%- set sid_name = var("sid_name")|replace('<name>',name) -%}

    {{ sid_name }}

{%- endmacro %}

{% macro business_key_name(
    name
    ) -%}
    {%- set business_key_name = var("business_key_name")|replace('<name>',name) -%}

    {{ business_key_name }}

{%- endmacro %}