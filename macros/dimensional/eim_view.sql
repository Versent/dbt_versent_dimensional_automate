{% macro eim_view(
    source,
    hks_to_string
    ) -%}

select
    {%- for col in hks_to_string %}
    cast({{col}} as string) as {{col}},
    {%- endfor %}
    * except (
        {%- for col in hks_to_string %}
        {{col}}{% if not loop.last %},{% endif %}
        {%- endfor %}
        )
from
    {{ ref(source)}}
{%- endmacro %}

