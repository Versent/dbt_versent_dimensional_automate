{% macro sid_name(
    name
    ) -%}
    {%- set sid_name = var("dim_name")|replace('<name>',name) -%}

    {{ sid_name }}

{%- endmacro %}