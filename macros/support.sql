{% macro sid_name(
    name
    ) -%}
    {%- set sid_name = var("sid_name")|replace('<name>',name) -%}

    {{ sid_name }}

{%- endmacro %}